"""Publish a frozen DuckLake: a read-only DuckDB-format catalog file next to the Parquet data.

Readers then need nothing but object storage: SUNCHASER_CATALOG=s3://bucket/lake/catalog.ducklake
(or an https:// URL for a public R2 bucket). The live lake keeps its writable catalog (SQLite or
Postgres) for the updater; this copies its metadata at a consistent point.

  python src/publish.py s3://osm/berlin/catalog.ducklake

Steps: flush inlined rows to Parquet (frozen catalogs carry no inlined data), snapshot the catalog,
let DuckLake create a fresh DuckDB-format catalog (so it owns the metadata schema), copy the
metadata rows across, upload with a single PUT (atomic for readers), and keep a timestamped copy.
"""
import os
import pathlib
import shutil
import sqlite3
import sys
import tempfile
import time

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import lakeconn  # noqa: E402


def _snapshot_catalog(catalog, workdir):
    """Consistent copy of the live catalog as something DuckDB can ATTACH read-only."""
    if catalog.startswith("sqlite:"):
        dst = os.path.join(workdir, "snapshot.sqlite")
        src = sqlite3.connect(catalog[len("sqlite:"):])
        out = sqlite3.connect(dst)
        src.backup(out)
        out.close()
        src.close()
        return f"'{dst}' AS src (TYPE sqlite, READ_ONLY)"
    if catalog.startswith("postgres:"):
        return f"'{catalog[len('postgres:'):]}' AS src (TYPE postgres, READ_ONLY)"  # a single transaction suffices
    dst = os.path.join(workdir, "snapshot.ducklake")
    shutil.copy(catalog, dst)
    return f"'{dst}' AS src (READ_ONLY)"


def build_frozen(con, catalog, data_path, out_file):
    con.sql("CALL ducklake_flush_inlined_data('lake')")
    with tempfile.TemporaryDirectory() as tmp:
        src = _snapshot_catalog(catalog, tmp)
        f = duckdb.connect()
        f.sql("LOAD ducklake; LOAD sqlite")
        if catalog.startswith("postgres:"):
            f.sql("LOAD postgres")
        if os.path.exists(out_file):
            os.remove(out_file)
        f.sql(f"ATTACH 'ducklake:{out_file}' AS fresh (DATA_PATH '{data_path}')")
        f.sql("DETACH fresh")
        f.sql(f"ATTACH '{out_file}' AS dst")
        f.sql(f"ATTACH {src}")
        tables = [t for (t,) in f.sql("SELECT table_name FROM duckdb_tables() WHERE database_name = 'src'").fetchall()]
        for t in tables:
            if t.startswith("ducklake_inlined_"):
                continue  # flushed above
            f.sql(f'DELETE FROM dst."{t}"')
            f.sql(f'INSERT INTO dst."{t}" SELECT * FROM src."{t}"')
        f.sql("DELETE FROM dst.ducklake_inlined_data_tables")
        f.sql("DETACH src")
        f.sql("CHECKPOINT dst")
        f.sql("DETACH dst")
        f.close()


def upload(local, url):
    if not url.startswith("s3://"):
        shutil.copy(local, url)
        return
    import boto3
    bucket, _, key = url[len("s3://"):].partition("/")
    endpoint = lakeconn.env("S3_ENDPOINT")
    if endpoint and not endpoint.startswith("http"):
        endpoint = ("https://" if lakeconn.env("S3_USE_SSL", "true") == "true" else "http://") + endpoint
    s3 = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=lakeconn.env("S3_KEY_ID"),
                      aws_secret_access_key=lakeconn.env("S3_SECRET"), region_name=lakeconn.env("S3_REGION", "auto"))
    with open(local, "rb") as fh:
        s3.put_object(Bucket=bucket, Key=key, Body=fh.read())


def publish(con, dest, catalog=None, data_path=None, keep_versions=True):
    """Build and upload a frozen catalog for the lake attached on `con` as `lake`."""
    t = time.time()
    catalog = catalog or lakeconn.env("CATALOG")
    # the copied ducklake_metadata rows carry the lake's real data path; this only seeds the fresh file
    data_path = data_path or lakeconn.env("DATA_PATH") or "data/"
    rows = con.sql("SELECT value FROM lake.state WHERE key = 'osm_base'").fetchall()
    osm_base = rows[0][0] if rows and rows[0][0] else time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "catalog.ducklake")
        build_frozen(con, catalog, data_path, out)
        upload(out, dest)
        if keep_versions:
            upload(out, dest.rsplit("/", 1)[0] + f"/catalogs/{osm_base.replace(':', '')}.ducklake")
        size = os.path.getsize(out)
    print(f"published {dest} @ {osm_base} ({size / 1e6:.1f} MB, {time.time() - t:.1f}s)", flush=True)


def main():
    if len(sys.argv) != 2:
        sys.exit("usage: publish.py <dest: s3://bucket/key.ducklake or a local path>")
    con = duckdb.connect()
    lakeconn.attach(con)
    publish(con, sys.argv[1])


if __name__ == "__main__":
    main()
