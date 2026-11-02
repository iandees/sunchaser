"""Shared DuckDB/DuckLake connection setup for the builder, the API server and the updater.

Environment:
  SUNCHASER_CATALOG     DuckLake catalog: a .ducklake file path or an s3:// URL (default data/lake/osm.ducklake)
  SUNCHASER_DATA_PATH   data path for new lakes, or an override for readers (s3://bucket/prefix/, https://...)
  SUNCHASER_S3_ENDPOINT, SUNCHASER_S3_KEY_ID, SUNCHASER_S3_SECRET, SUNCHASER_S3_REGION,
  SUNCHASER_S3_URL_STYLE (path|vhost), SUNCHASER_S3_USE_SSL (true|false)
                        S3-compatible credentials (AWS S3, Cloudflare R2, MinIO)
  SUNCHASER_S3_CREDENTIAL_CHAIN=true
                        on AWS: use the default credential chain (instance profile, task or function
                        role) instead of keys; set SUNCHASER_S3_REGION too
"""
import os

import duckdb


def env(name, default=None):
    return os.environ.get(f"SUNCHASER_{name}", default)


# The reverse indexes (node -> way, member -> relation, tag -> element) are insert-only between
# compactions: a row they should lose goes into the matching *_gone table instead, because deleting
# it in place means reading the row group it sits in, one scattered R2 range request per element
# tag (531 s of a planet batch). Readers anti-join the pair tables' tombstones (tag postings need
# none: every tag filter is re-applied on the element row); the tombstones are applied in one
# streaming pass by the updater's compaction (updater.apply_tombstones).
TOMBSTONE_DDL = """
    CREATE TABLE IF NOT EXISTS lake.node_parent_ways_gone (node_id BIGINT, way_id BIGINT);
    CREATE TABLE IF NOT EXISTS lake.member_parent_rels_gone (mtype VARCHAR, mid BIGINT, rel_id BIGINT);
    CREATE TABLE IF NOT EXISTS lake.tag_postings_gone (type VARCHAR, kv VARCHAR, qt_key31 UBIGINT, id BIGINT);
"""
TOMBSTONES = {"node_parent_ways": ("node_id", "way_id"), "member_parent_rels": ("mtype", "mid", "rel_id"),
              "tag_postings": ("type", "kv", "qt_key31", "id")}


def has_tombstones(con):
    """True once the lake has the *_gone tables (every lake built or updated since they exist)."""
    try:
        con.sql("SELECT 1 FROM lake.node_parent_ways_gone LIMIT 0")
        return True
    except duckdb.Error:
        return False


def configure_s3(con):
    if env("S3_CREDENTIAL_CHAIN", "false") == "true":
        # AWS: credentials from the environment, instance profile or task/function role
        region = f", REGION '{env('S3_REGION')}'" if env("S3_REGION") else ""
        con.sql(f"CREATE OR REPLACE SECRET lake_s3 (TYPE s3, PROVIDER credential_chain{region})")
        return
    if not (env("S3_ENDPOINT") or env("S3_KEY_ID")):
        return
    opts = {"KEY_ID": env("S3_KEY_ID", ""), "SECRET": env("S3_SECRET", ""),
            "REGION": env("S3_REGION", "auto"), "URL_STYLE": env("S3_URL_STYLE", "path")}
    if env("S3_ENDPOINT"):
        opts["ENDPOINT"] = env("S3_ENDPOINT")
    body = ", ".join(f"{k} '{v}'" for k, v in opts.items())
    con.sql(f"CREATE OR REPLACE SECRET lake_s3 (TYPE s3, {body}, USE_SSL {env('S3_USE_SSL', 'true')})")


def load(con, *extensions):
    """LOAD DuckDB extensions, installing any that aren't on this machine yet (a fresh box has none)."""
    for ext in extensions:
        try:
            con.sql(f"LOAD {ext}")
        except duckdb.IOException:
            con.sql(f"INSTALL {ext}")
            con.sql(f"LOAD {ext}")


def attach(con, catalog=None, data_path=None, read_only=False, override_data_path=False):
    """ATTACH the lake as `lake`. Loads the extensions the catalog and data path need."""
    catalog = catalog or env("CATALOG", "data/lake/osm.ducklake")
    data_path = data_path if data_path is not None else env("DATA_PATH")
    load(con, "ducklake", "httpfs")
    configure_s3(con)
    opts = []
    if read_only:
        opts.append("READ_ONLY")
    if data_path:
        opts.append(f"DATA_PATH '{data_path}'")
        if override_data_path:
            opts.append("OVERRIDE_DATA_PATH true")
    con.sql(f"ATTACH 'ducklake:{catalog}' AS lake" + (f" ({', '.join(opts)})" if opts else ""))
    return con
