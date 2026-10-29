"""Id-lookup benchmark: fetch (id, lat, lon) for a list of node ids through the simulated object store.

  parquet/locator : DuckLake elem_locator -> qt ranges -> nodes (the engine's current path)
  lance/<table>   : Lance BTree scalar index on id (filter "id IN (...)"), via the S3 API
Each variant runs 'cold' (fresh handle) and 'warm' (same handle again: index pages / footers cached).
"""
import json
import pathlib
import sys
import time
import urllib.request

import duckdb
import lance

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from engine import Engine  # noqa: E402

SRV = "http://127.0.0.1:8765"
S3 = {"aws_endpoint": SRV, "allow_http": "true", "aws_access_key_id": "x", "aws_secret_access_key": "x",
      "aws_region": "us-east-1", "virtual_hosted_style_request": "false"}


def st(p):
    return json.loads(urllib.request.urlopen(f"{SRV}/{p}").read())


def measure(fn):
    st("__reset")
    t = time.perf_counter()
    n = fn()
    s = st("__stats")
    return n, time.perf_counter() - t, s["requests"], s["bytes"]


def id_sets():
    c = duckdb.connect()
    c.sql("LOAD ducklake")
    c.sql("ATTACH 'ducklake:data/lake/osm.ducklake' AS lake (READ_ONLY)")
    ids = lambda q: sorted({r[0] for r in c.sql(q).fetchall()})
    return {
        "5 ids": [30353086, 253846340, 322796388, 2812237303, 13254184468],
        "street (208)": ids("SELECT unnest(nds) FROM lake.ways WHERE tags['name'] = 'Unter den Linden'"),
        "subway (8957)": ids("""SELECT unnest(nds) FROM lake.ways WHERE id IN (
            SELECT ref FROM (SELECT unnest(members, recursive := true) FROM lake.relations
                             WHERE tags['route'] = 'subway') WHERE type = 'w')"""),
    }


def parquet_variant(ids):
    con = duckdb.connect()
    con.sql("LOAD ducklake; LOAD httpfs; SET parquet_metadata_cache = true")
    con.sql(f"ATTACH 'ducklake:{SRV}/lake/osm.ducklake' AS lake "
            f"(READ_ONLY, DATA_PATH '{SRV}/lake/data/', OVERRIDE_DATA_PATH true)")
    for (tbl,) in con.sql("SELECT table_name FROM ducklake_table_info('lake')").fetchall():
        con.sql(f"SELECT count(*) FROM lake.{tbl}").fetchall()  # footers + catalog, like a warm worker
    e = Engine(con, "lake")
    run = lambda: len(con.sql(f"SELECT id, lat, lon FROM lake.nodes WHERE {e._fetch_where('n', ids)}").fetchall())
    return run, lambda: e._locator_memo.clear()


def lance_variant(table, ids):
    holder = {}

    def run():
        if "ds" not in holder:
            holder["ds"] = lance.dataset(f"s3://lance/{table}.lance", storage_options=S3)
        return holder["ds"].to_table(columns=["id", "lat", "lon"], filter=f"id IN ({','.join(map(str, ids))})").num_rows

    return run, lambda: None


def main():
    latency = float(sys.argv[1]) if len(sys.argv) > 1 else 0
    st(f"__latency?ms={latency}")
    rows = []
    for label, ids in id_sets().items():
        for name, make in (("parquet/locator", lambda: parquet_variant(ids)),
                           ("lance/nodes_by_id", lambda: lance_variant("nodes_by_id", ids)),
                           ("lance/nodes_by_qt", lambda: lance_variant("nodes_by_qt", ids))):
            run, reset = make()
            for phase in ("cold", "warm"):
                n, dt, req, b = measure(run)
                reset()
                rows.append(dict(ids=label, variant=name, phase=phase, rows=n, s=round(dt, 3), req=req, kib=b // 1024))
                print(f"{label:<14} {name:<18} {phase:<5} rows={n:<5} {dt:6.3f}s {req:5} req {b / 2**20:8.2f} MiB", flush=True)
    st("__latency?ms=0")
    (ROOT / "results" / f"idlookup_lat{int(latency)}.json").write_text(json.dumps(rows, indent=1))


if __name__ == "__main__":
    main()
