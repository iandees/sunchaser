"""Run the corpus: brute-force reference vs. lake plans, element-level comparison + I/O accounting.

Scenarios for the lake (served by tools/range_server.py to mimic object storage):
  cold   - new DuckDB connection per query (catalog + footers + data fetched)
  worker - long-lived connection: catalog and Parquet footers cached (primed with count(*) on every
           table), data blocks never touched before this query
  hot    - long-lived connection with DuckDB's external file cache on (repeat query)
"""
import argparse
import json
import pathlib
import sys
import time
import urllib.request

import duckdb

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "queries")]
from corpus import CORPUS  # noqa: E402
from engine import Engine, Plan  # noqa: E402


def http(path, base):
    return json.loads(urllib.request.urlopen(f"{base.rsplit('/', 1)[0]}/{path}").read())


DUCKDB_SETTINGS = []
LANCE = {"on": False}
S3 = {"aws_endpoint": "http://127.0.0.1:8765", "allow_http": "true", "aws_access_key_id": "x",
      "aws_secret_access_key": "x", "aws_region": "us-east-1", "virtual_hosted_style_request": "false"}


def lance_opener():
    """Per-engine dataset cache: a fresh opener is a cold Lance process, a reused one a warm worker."""
    if not LANCE["on"]:
        return None
    import lance
    cache = {}

    def get(name):
        if name not in cache:
            cache[name] = lance.dataset(f"s3://lance/{name}.lance", storage_options=S3)
        return cache[name]
    return get


def connect_lake(base, file_cache):
    con = duckdb.connect()
    con.sql("LOAD ducklake; LOAD httpfs; SET parquet_metadata_cache = true")
    for setting in DUCKDB_SETTINGS:
        con.sql(f"SET {setting}")
    con.sql(f"SET enable_external_file_cache = {str(file_cache).lower()}")
    con.sql(f"ATTACH 'ducklake:{base}/osm.ducklake' AS lake "
            f"(READ_ONLY, DATA_PATH '{base}/data/', OVERRIDE_DATA_PATH true)")
    return con


def timed(fn, base=None):
    if base:
        http("__reset", base)
    t = time.perf_counter()
    res = fn()
    dt = time.perf_counter() - t
    st = http("__stats", base) if base else {"requests": 0, "bytes": 0}
    return res, dt, st["requests"], st["bytes"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:8765/lake")
    ap.add_argument("--latency-ms", type=float, default=30)
    ap.add_argument("--only", nargs="*")
    ap.add_argument("--no-qt-ranges", action="store_true")
    ap.add_argument("--no-postings", action="store_true")
    ap.add_argument("--no-locator", action="store_true")
    ap.add_argument("--out", default="results/corpus.json")
    ap.add_argument("--lance", action="store_true", help="route node/way scans to the Lance element store")
    ap.add_argument("--lance-scope", default="all", choices=["all", "ids"])
    ap.add_argument("--set", action="append", default=[], help="extra DuckDB setting, e.g. disable_parquet_prefetching=true")
    args = ap.parse_args()
    DUCKDB_SETTINGS.extend(args.set)
    LANCE["on"] = args.lance
    plan_kw = dict(qt_ranges=not args.no_qt_ranges, use_postings=not args.no_postings, locator=not args.no_locator,
                   lance_scope=args.lance_scope)

    ref_con = duckdb.connect()
    ref = Engine(ref_con, mode="raw", raw_dir=str(ROOT / "data/raw"))

    http(f"__latency?ms={args.latency_ms}", args.base)
    hot = connect_lake(args.base, file_cache=True)
    hot_e = Engine(hot, "lake", plan=Plan(**plan_kw), lance=lance_opener())

    def fresh_worker():
        con = connect_lake(args.base, file_cache=True)
        for (tbl,) in con.sql("SELECT table_name FROM ducklake_table_info('lake')").fetchall():
            con.sql(f"SELECT count(*) FROM lake.{tbl}").fetchall()  # footers + catalog only
        opener = lance_opener()
        if opener:
            for name in ("nodes_tagged", "nodes_untagged", "ways"):
                opener(name).to_table(columns=["id"], filter="id = -1")  # manifest + index roots only
        return con, Engine(con, "lake", plan=Plan(**plan_kw), lance=opener)

    rows = []
    for name, ql, prog in CORPUS:
        if args.only and name not in args.only:
            continue
        t = time.perf_counter()
        expected = ref.ids(prog(ref))
        t_ref = time.perf_counter() - t

        def cold_run():
            c = connect_lake(args.base, file_cache=True)
            e = Engine(c, "lake", plan=Plan(**plan_kw), lance=lance_opener())
            out = e.ids(prog(e))
            c.close()
            return out

        got_cold, t_cold, req_cold, b_cold = timed(cold_run, args.base)
        worker, worker_e = fresh_worker()
        got_w, t_w, req_w, b_w = timed(lambda: worker_e.ids(prog(worker_e)), args.base)
        worker.close()
        prog(hot_e)
        got_h, t_h, req_h, b_h = timed(lambda: hot_e.ids(prog(hot_e)), args.base)

        ok = expected == got_cold == got_w == got_h
        row = dict(name=name, ql=ql, n=len(expected), match=ok,
                   missing=len(expected - got_cold), extra=len(got_cold - expected),
                   ref_s=round(t_ref, 3),
                   cold_s=round(t_cold, 3), cold_req=req_cold, cold_kib=b_cold // 1024,
                   worker_s=round(t_w, 3), worker_req=req_w, worker_kib=b_w // 1024,
                   hot_s=round(t_h, 3), hot_req=req_h)
        rows.append(row)
        print(f"{name:<28} n={row['n']:<7} {'OK ' if ok else 'BAD'} "
              f"ref={t_ref:6.2f}s | cold {t_cold:6.2f}s {req_cold:4}req {b_cold / 2**20:7.1f}MiB | "
              f"worker {t_w:6.2f}s {req_w:4}req {b_w / 2**20:7.1f}MiB | hot {t_h:6.3f}s", flush=True)
        if not ok:
            print("    missing:", sorted(expected - got_cold)[:5], "extra:", sorted(got_cold - expected)[:5])

    out = ROOT / args.out
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(dict(latency_ms=args.latency_ms, plan=plan_kw, settings=args.set, lance=args.lance,
                                   rows=rows), indent=1))
    print(f"{sum(r['match'] for r in rows)}/{len(rows)} element-level matches -> {out}")


if __name__ == "__main__":
    main()
