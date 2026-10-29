"""Overpass-compatible HTTP API over the lake.

  GET/POST /api/interpreter   data=<Overpass QL> (form, query string, or raw POST body)
  GET      /api/status        plain-text status, like Overpass
  GET      /api/timestamp     timestamp of the newest applied OSM data

Configuration (environment):
  SUNCHASER_CATALOG    DuckLake catalog, e.g. "sqlite:/srv/lake/catalog.sqlite",
                       "postgres:dbname=lake host=...", or a .ducklake file (default data/lake/osm.ducklake)
  SUNCHASER_DATA_PATH  override the catalog's data path (e.g. s3://bucket/lake/ or https://.../data/)
  SUNCHASER_S3_ENDPOINT, SUNCHASER_S3_KEY_ID, SUNCHASER_S3_SECRET, SUNCHASER_S3_REGION,
  SUNCHASER_S3_URL_STYLE (path|vhost), SUNCHASER_S3_USE_SSL (true|false)
                       S3 / R2 credentials for DuckDB's httpfs
  SUNCHASER_MAX_TIMEOUT  cap for [timeout:N] in seconds (default 180)

Each request runs on its own DuckDB cursor (temp tables are per cursor) in a worker thread;
[timeout:N] interrupts the query, and the response carries Overpass's runtime-error remark.
"""
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
from starlette.responses import PlainTextResponse, Response
from starlette.routing import Route

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import lakeconn  # noqa: E402
import ql  # noqa: E402
from engine import SQL_MACROS, Engine  # noqa: E402

COPYRIGHT = "The data included in this document is from www.openstreetmap.org. The data is made available under ODbL."


env = lakeconn.env


def open_lake():
    con = duckdb.connect()
    con.sql("SET parquet_metadata_cache = true")
    lakeconn.attach(con, read_only=True, override_data_path=bool(env("DATA_PATH")))
    con.sql(SQL_MACROS)
    return con


def remote_catalog():
    cat = env("CATALOG", "")
    return cat if cat.startswith(("s3://", "http://", "https://")) else None


def catalog_version(url):
    """ETag (or Last-Modified) of a published frozen catalog."""
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
    """Frozen catalogs are replaced by the publisher; reopen the lake when a new one appears.
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
                print(f"reloaded frozen catalog {url} ({v})", flush=True)
            seen = v
        except Exception as e:  # keep serving the last good catalog
            print(f"catalog refresh failed: {e}", flush=True)
        time.sleep(every)


DB = None
DB_LOCK = threading.Lock()


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


def run_query(text, callback=None):
    cur = db().cursor()
    try:
        script = ql.parse(text)
        timeout = min(int(script.settings.get("timeout", 180)), int(env("MAX_TIMEOUT", "180")))
        engine = Engine(cur, "lake", init_macros=False)
        timer = threading.Timer(timeout, cur.interrupt)
        timer.start()
        started = time.time()
        try:
            doc = ql.Executor(engine, osm_base(cur)).run(script, json_callback=callback)
        except duckdb.InterruptException:
            doc = ql.Document(fmt=script.settings.get("out", "xml"), timestamp=osm_base(cur))
            doc.remarks.append(f"runtime error: Query timed out after {int(time.time() - started)} seconds.")
        finally:
            timer.cancel()
        return doc.render()
    finally:
        cur.close()


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
        content_type, body = await run_in_threadpool(run_query, text, request.query_params.get("jsonp"))
    except ql.QLError as e:
        return error_page(str(e))
    except Exception as e:  # report like Overpass runtime errors instead of a bare 500
        traceback.print_exc()
        return error_page(f"runtime error: {type(e).__name__}: {e}")
    return Response(body, media_type=f"{content_type}; charset=utf-8")


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
