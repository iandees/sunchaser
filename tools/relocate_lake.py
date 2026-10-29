"""Point a lake's catalog at a new data location, after copying its files there.

Build on local disk (fast, no upload during the build), copy the data and Lance directories to
R2/S3 (rclone, aws s3 sync), then relocate. DuckLake stores data file paths relative to the
catalog's data path and Lance datasets use relative paths too, so only the two bases change:

  python tools/relocate_lake.py --catalog /ssd/catalog.ducklake \
      --data-path s3://bucket/lake/data/ [--lance s3://bucket/lake/lance]

Works on a DuckDB-format catalog file (the object-storage setup) or sqlite:/path.
"""
import argparse
import pathlib
import sys

import duckdb

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import lakeconn  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True, help="catalog file (.ducklake) or sqlite:/path")
    ap.add_argument("--data-path", required=True, help="new data path, ending in /")
    ap.add_argument("--lance", help="new Lance base (if the lake has a Lance store)")
    args = ap.parse_args()
    data_path = args.data_path if args.data_path.endswith("/") else args.data_path + "/"

    # 1. the catalog's data path: a metadata row, outside any DuckLake snapshot
    meta = duckdb.connect()
    if args.catalog.startswith("sqlite:"):
        meta.sql("LOAD sqlite")
        meta.sql(f"ATTACH '{args.catalog[len('sqlite:'):]}' AS m (TYPE sqlite)")
    else:
        meta.sql(f"ATTACH '{args.catalog}' AS m")
    old = meta.sql("SELECT value FROM m.ducklake_metadata WHERE key = 'data_path'").fetchall()
    meta.sql(f"UPDATE m.ducklake_metadata SET value = '{data_path}' WHERE key = 'data_path'")
    meta.close()
    print(f"data_path: {old[0][0] if old else None} -> {data_path}")

    # 2. the Lance base recorded in lake.state (a normal DuckLake commit)
    if args.lance:
        con = duckdb.connect()
        lakeconn.attach(con, catalog=args.catalog, data_path="")
        base = con.sql("SELECT value FROM lake.state WHERE key = 'lance_base'").fetchall()
        if not base:
            sys.exit("the lake has no Lance store (state.lance_base)")
        con.sql(f"UPDATE lake.state SET value = '{args.lance.rstrip('/')}' WHERE key = 'lance_base'")
        print(f"lance_base: {base[0][0]} -> {args.lance.rstrip('/')}")


if __name__ == "__main__":
    main()
