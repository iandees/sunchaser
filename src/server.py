"""Overpass-compatible HTTP API over the lake.

  GET/POST /api/interpreter   data=<Overpass QL> (form, query string, or raw POST body)
  GET      /api/status        plain-text status, like Overpass
  GET      /api/timestamp     timestamp of the newest applied OSM data

Configuration (environment):
  SUNCHASER_CATALOG    DuckLake catalog: a .ducklake file, or s3://bucket/path/catalog.ducklake (the updater's
                       upload, followed by ETag; default data/lake/osm.ducklake)
  SUNCHASER_DATA_PATH  override the catalog's data path (e.g. s3://bucket/lake/ or https://.../data/)
  SUNCHASER_S3_ENDPOINT, SUNCHASER_S3_KEY_ID, SUNCHASER_S3_SECRET, SUNCHASER_S3_REGION,
  SUNCHASER_S3_URL_STYLE (path|vhost), SUNCHASER_S3_USE_SSL (true|false)
                       S3 / R2 credentials for DuckDB's httpfs
  SUNCHASER_MAX_TIMEOUT  cap for [timeout:N] in seconds (default 180)
  SUNCHASER_MAX_OUTPUT   cap for a response in bytes (default 2 GiB); [maxsize:N] can lower it
  SUNCHASER_MAX_READ_MB  refuse a query before it runs when the catalog's file statistics say its scans would read
                         more than this many MB from storage (default 0: off); see src/readbudget.py
  SUNCHASER_MAX_QUERIES  queries executed concurrently (default 16); more wait in line
  SUNCHASER_LANCE        "off" to ignore a lake's Lance id store (default: use it when the lake has one)
  SUNCHASER_LOG_QUERIES  1: one line per query (time, bytes, outcome, query text); 2: also each SQL statement
                         the query ran, with its time (default 0: only uvicorn's access log)

Each request runs on its own DuckDB cursor (temp tables are per cursor) in a worker thread and
streams its output as it is produced, so large results never sit in memory; a slow client pauses
the query. [timeout:N] interrupts the query, and the response carries Overpass's runtime-error
remark; a client that disconnects cancels its query.
"""
import asyncio
import concurrent.futures
import datetime as dt
import html
import pathlib
import sys
import threading
import time
import traceback

import duckdb
from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import PlainTextResponse, Response, StreamingResponse
from starlette.routing import Route

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import lakeconn  # noqa: E402
import ql  # noqa: E402
import readbudget  # noqa: E402
import lancestore  # noqa: E402
from engine import SQL_MACROS, Engine  # noqa: E402

COPYRIGHT = "The data included in this document is from www.openstreetmap.org. The data is made available under ODbL."


env = lakeconn.env
MAX_OUTPUT = int(env("MAX_OUTPUT", str(2 << 30)))       # bytes; [maxsize:N] can only lower it
LOG_QUERIES = int(env("LOG_QUERIES", "0"))
STREAM_AHEAD = 16                                          # unsent 64 KB chunks before the query waits
WORKERS = concurrent.futures.ThreadPoolExecutor(int(env("MAX_QUERIES", "16")))  # concurrent queries


def open_lake():
    for attempt in range(5):
        con = duckdb.connect()
        con.sql("SET parquet_metadata_cache = true")
        try:
            lakeconn.attach(con, read_only=True, override_data_path=bool(env("DATA_PATH")))
        except duckdb.HTTPException as e:
            # the updater replaced the catalog while we were reading it (its ETag changed): read it again
            if "ETag" not in str(e) or attempt == 4:
                raise
            time.sleep(1)
            continue
        con.sql(SQL_MACROS)
        return con


def remote_catalog():
    cat = env("CATALOG", "")
    return cat if cat.startswith(("s3://", "http://", "https://")) else None


def catalog_version(url):
    """ETag (or Last-Modified) of the object-storage catalog the lake is attached through."""
    if url.startswith("s3://"):
        import boto3
        bucket, _, key = url[5:].partition("/")
        endpoint = env("S3_ENDPOINT")
        if endpoint and not endpoint.startswith("http"):
            endpoint = ("https://" if env("S3_USE_SSL", "true") == "true" else "http://") + endpoint
        s3 = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=env("S3_KEY_ID"),
                          aws_secret_access_key=env("S3_SECRET"), region_name=env("S3_REGION", "auto"))
        h = s3.head_object(Bucket=bucket, Key=key)
        return h.get("ETag") or str(h.get("LastModified"))
    import urllib.request
    with urllib.request.urlopen(urllib.request.Request(url, method="HEAD"), timeout=30) as r:
        return r.headers.get("ETag") or r.headers.get("Last-Modified")


def refresher():
    """The catalog object is replaced in place by the updater; reopen the lake when its ETag changes.
    Requests in flight keep their cursor on the previous connection."""
    global DB
    url, every = remote_catalog(), float(env("CATALOG_REFRESH", "30"))
    seen = None
    while True:
        try:
            v = catalog_version(url)
            if seen is not None and v != seen:
                fresh = open_lake()
                with DB_LOCK:
                    DB = fresh
                print(f"reloaded catalog {url} ({v})", flush=True)
            seen = v
        except Exception as e:  # keep serving the last good catalog
            print(f"catalog refresh failed: {e}", flush=True)
        time.sleep(every)


DB = None
DB_LOCK = threading.Lock()


STORES = {}  # (lance base, pinned versions) -> lancestore.Store


def lance_store(cur):
    """The Lance id store at the versions this request's catalog snapshot records (None: no Lance).
    Read through the request's own cursor, so it always matches the DuckLake data it sees."""
    if env("LANCE", "on") == "off":
        return None
    try:
        base, versions = lancestore.state_versions(cur)
    except duckdb.Error:
        return None
    if not base:
        return None
    key = (base, tuple(sorted(versions.items())))
    with DB_LOCK:
        store = STORES.get(key)
        if store is None:
            if len(STORES) > 8:
                STORES.clear()
            store = STORES[key] = lancestore.Store(base, versions)
    return store


def db():
    global DB
    with DB_LOCK:
        if DB is None:
            DB = open_lake()
        return DB


def osm_base(cur):
    try:
        rows = cur.sql("SELECT value FROM lake.state WHERE key = 'osm_base'").fetchall()
        return rows[0][0] if rows else ""
    except duckdb.Error:
        return ""


def error_page(msg):
    body = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
<head>
  <meta http-equiv="content-type" content="text/html; charset=utf-8" lang="en"/>
  <title>OSM3S Response</title>
</head>
<body>

<p>{COPYRIGHT}</p>
<p><strong style="color:#FF0000">Error</strong>: {html.escape(msg)} </p>

</body>
</html>
"""
    return Response(body, status_code=400, media_type="text/html; charset=utf-8")


def reopen():
    """Replace the shared connection (e.g. the catalog was rebuilt or replaced under us)."""
    global DB
    fresh = open_lake()
    with DB_LOCK:
        DB = fresh


class ClientGone(Exception):
    pass


class Chunker:
    """Document sink: coalesces output into ~64 KB chunks and hands them to the response stream,
    blocking (backpressure) while STREAM_AHEAD chunks are unsent."""

    SIZE = 1 << 16

    def __init__(self, loop, queue, cancelled):
        self.loop, self.queue, self.cancelled = loop, queue, cancelled
        self.slots = threading.Semaphore(STREAM_AHEAD)
        self.parts, self.n, self.sent_any = [], 0, False
        self.query, self.total = "", 0  # for SUNCHASER_LOG_QUERIES

    def __call__(self, text):
        self.parts.append(text)
        self.n += len(text)
        if self.n >= self.SIZE:
            self.flush()

    def flush(self):
        if self.parts:
            self.put("data", "".join(self.parts))
            self.total += self.n
            self.parts, self.n, self.sent_any = [], 0, True

    def put(self, kind, value):
        while not self.slots.acquire(timeout=1):
            if self.cancelled.is_set():
                raise ClientGone()
        if self.cancelled.is_set():
            raise ClientGone()
        self.loop.call_soon_threadsafe(self.queue.put_nowait, (kind, value))


def _log_query(sink, started, ran, outcome, steps):
    """SUNCHASER_LOG_QUERIES: where a query's time went (statement times exclude lazily fetched results)."""
    q = " ".join(getattr(sink, "query", "").split())
    setup = f"setup {ran - started:5.2f}s" if ran else "setup    n/a"
    print(f"query {time.time() - started:6.2f}s ({setup}) {getattr(sink, 'total', 0) >> 10:7} KiB {outcome:<9} {q[:300]}",
          flush=True)
    if LOG_QUERIES > 1:
        for secs, sql in steps:
            print(f"    {secs:6.2f}s {' '.join(sql.split())[:160]}", flush=True)
        # result SELECTs are lazy: their work happens while the output is written, not in the statement
        print(f"    {time.time() - (ran or started) - sum(t for t, _ in steps):6.2f}s fetching results / writing output",
              flush=True)


def _execute(script, callback, sink):
    """Run a parsed script, streaming into `sink` (a Chunker). Errors before anything was sent
    propagate (error page / catalog retry); later ones become Overpass runtime-error remarks."""
    started, ran, steps, outcome = time.time(), None, [], "ok"
    cur = db().cursor()
    try:
        st = script.settings
        timeout = min(int(st.get("timeout", 180)), int(env("MAX_TIMEOUT", "180")))
        maxsize = min(int(st.get("maxsize", MAX_OUTPUT)), MAX_OUTPUT)
        max_read = int(float(env("MAX_READ_MB", "0")) * (1 << 20))
        doc = ql.Document(fmt=st.get("out", "xml"), csv=st.get("csv"), timestamp=osm_base(cur),
                          json_callback=callback, sink=sink, maxsize=maxsize)
        timer = threading.Timer(timeout, cur.interrupt)
        timer.start()
        try:
            store = lance_store(cur)
            engine = Engine(cur, "lake", init_macros=False, lance=store)
            if LOG_QUERIES > 1:
                def timed(sql, fn):
                    t = time.time()
                    rel = fn()
                    steps.append((time.time() - t, sql))
                    return rel
                engine.trace = timed
            if max_read:
                engine.budget = readbudget.ReadBudget(cur, max_read)
            ran = time.time()
            ql.Executor(engine, doc.timestamp).run(script, doc=doc)
        except duckdb.InterruptException:
            if sink.cancelled.is_set():
                outcome = "cancelled"
                raise ClientGone()
            outcome = "timeout"
            doc.remarks.append(f"runtime error: Query timed out after {int(time.time() - started)} seconds.")
        except ql.OutputTooLarge:
            outcome = "too-large"
            doc.remarks.append(f"runtime error: Query run out of memory using about {-(-maxsize >> 20)} MB of RAM.")
        except readbudget.ReadBudgetExceeded as e:
            outcome = "too-big"
            doc.remarks.append(f"runtime error: Query would read about {-(-e.estimated >> 20)} MB of data, over the "
                               f"limit of {e.limit / (1 << 20):g} MB. Narrow the bounding box or add more filters.")
        except (ClientGone, ql.QLError):
            outcome = "error"
            raise
        except Exception as e:
            outcome = "error"
            if not sink.sent_any:
                raise
            traceback.print_exc()
            doc.remarks.append(f"runtime error: {type(e).__name__}: {e}")
        finally:
            timer.cancel()
        doc.finish()
        sink.flush()
    finally:
        cur.close()
        if LOG_QUERIES:
            _log_query(sink, started, ran, outcome, steps)


def run_stream(script, callback, sink):
    """Worker-thread body: execute (retrying once if the catalog was replaced) and signal the end."""
    try:
        try:
            _execute(script, callback, sink)
        except (duckdb.CatalogException, duckdb.InvalidInputException) as e:
            if sink.sent_any or "ducklake" not in str(e).lower():
                raise
            print(f"catalog changed under us, reopening: {str(e).splitlines()[0][:120]}", flush=True)
            reopen()
            sink.parts, sink.n = [], 0
            _execute(script, callback, sink)
        sink.put("done", None)
    except ClientGone:
        pass
    except Exception as e:
        if not isinstance(e, ql.QLError):
            traceback.print_exc()
        try:
            sink.put("error", e)
        except ClientGone:
            pass


def run_query(text, callback=None):
    """Buffered execution (used by tests/test_readbudget.py): (content_type, body)."""
    script = ql.parse(text)
    out = []

    class Collect:
        cancelled = threading.Event()
        sent_any = False

        def __call__(self, t):
            out.append(t)

        def flush(self):
            pass

    doc_type = ql.Document(fmt=script.settings.get("out", "xml"), json_callback=callback).content_type
    _execute(script, callback, Collect())
    return doc_type, "".join(out)


async def interpreter(request):
    text = request.query_params.get("data")
    if request.method == "POST":
        form_ct = request.headers.get("content-type", "")
        if "application/x-www-form-urlencoded" in form_ct or "multipart/form-data" in form_ct:
            form = await request.form()
            text = form.get("data", text)
        else:
            raw = (await request.body()).decode("utf-8", "replace")
            text = raw[5:] if raw.startswith("data=") else raw
    if not text:
        return error_page("line 1: parse error: empty query (expected a data= parameter)")
    try:
        script = ql.parse(text)
    except ql.QLError as e:
        return error_page(str(e))
    callback = request.query_params.get("jsonp")
    content_type = ql.Document(fmt=script.settings.get("out", "xml"), json_callback=callback).content_type
    queue, cancelled = asyncio.Queue(), threading.Event()
    sink = Chunker(asyncio.get_running_loop(), queue, cancelled)
    sink.query = text
    WORKERS.submit(run_stream, script, callback, sink)
    kind, value = await queue.get()
    sink.slots.release()
    if kind == "error":
        cancelled.set()
        if isinstance(value, ql.QLError):
            return error_page(str(value))
        return error_page(f"runtime error: {type(value).__name__}: {value}")

    async def body():
        nonlocal kind, value
        try:
            while kind == "data":
                yield value
                kind, value = await queue.get()
                sink.slots.release()
            if kind == "error":  # failed after output began: Overpass would end with a remark
                yield f"\n<!-- runtime error: {type(value).__name__}: {value} -->\n"
        finally:
            if kind not in ("done", "error"):  # client went away mid-stream
                cancelled.set()

    return StreamingResponse(body(), media_type=f"{content_type}; charset=utf-8")


async def status(request):
    cur = db().cursor()
    try:
        ts = await run_in_threadpool(osm_base, cur)
    finally:
        cur.close()
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return PlainTextResponse(
        f"Connected as: {request.client.host if request.client else 'unknown'}\n"
        f"Current time: {now}\nData timestamp: {ts}\nRate limit: 0\nSlots available now: unlimited.\n"
        "Currently running queries (pid, space limit, time limit, start time):\n")


async def timestamp(request):
    cur = db().cursor()
    try:
        return PlainTextResponse(await run_in_threadpool(osm_base, cur))
    finally:
        cur.close()


class PrivateNetworkAccess:
    """Chrome's Private Network Access: https pages (e.g. overpass-turbo.eu) may call a server on a
    private address only if its preflight answers Access-Control-Allow-Private-Network."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                message.setdefault("headers", []).append((b"access-control-allow-private-network", b"true"))
            await send(message)
        await self.app(scope, receive, send_wrapper)


app = Starlette(
    routes=[Route("/api/interpreter", interpreter, methods=["GET", "POST"]),
            Route("/api/status", status), Route("/api/timestamp", timestamp)],
    middleware=[Middleware(PrivateNetworkAccess),
                Middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET", "POST", "OPTIONS"],
                           allow_headers=["*"])],
)

if remote_catalog():
    threading.Thread(target=refresher, daemon=True).start()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=env("HOST", "127.0.0.1"), port=int(env("PORT", "8080")))
