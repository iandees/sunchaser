"""Lance id-lookup store kept in step with the lake.

Lance copies of nodes_tagged, nodes_untagged and ways (same columns as the lake tables, tile-sorted,
BTree index on id) serve id-driven fetches: recursion, id queries, global tag lookups. There Lance
measured up to 2.2x faster than DuckLake's locator + key-range reads (results/NOTES.md); spatial
scans stay on DuckLake, whose zone maps beat Lance there.

Consistency: every Lance write produces a new dataset version, and the updater records the versions
in lake.state ('lance_version:<table>') in the same DuckLake transaction as the diff. Readers open
Lance at exactly those versions, so the two stores always describe the same diff, whatever the
updater is doing. Versions newer than the catalog's (an update in progress, or a run that was
discarded) are invisible to readers; the next run deletes and re-appends the ids it touches, so it
converges regardless.

  python src/lancestore.py build s3://bucket/lake/lance     # add Lance to an existing lake
"""
import datetime as dt
import pathlib
import sys
import threading
import time

import lance
import pyarrow as pa

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import lakeconn  # noqa: E402

TABLES = {  # lance table -> (lake table, element type, sort key)
    "nodes_tagged": ("nodes_tagged", "n", "qt, id"),
    "nodes_untagged": ("nodes_untagged", "n", "qt, id"),
    "ways": ("ways", "w", "qt_key31, id"),
}
ROWS_PER_GROUP = 16384


def storage_options():
    """object_store options from the SUNCHASER_S3_* settings (none: the AWS default chain)."""
    env = lakeconn.env
    opts = {}
    if env("S3_KEY_ID"):
        opts["aws_access_key_id"] = env("S3_KEY_ID")
        opts["aws_secret_access_key"] = env("S3_SECRET", "")
    if env("S3_REGION"):
        opts["aws_region"] = env("S3_REGION")
    if env("S3_ENDPOINT"):
        endpoint = env("S3_ENDPOINT")
        if not endpoint.startswith("http"):
            endpoint = ("https://" if env("S3_USE_SSL", "true") == "true" else "http://") + endpoint
        opts["aws_endpoint"] = endpoint
        opts["allow_http"] = "true" if endpoint.startswith("http://") else "false"
        opts["aws_virtual_hosted_style_request"] = "false" if env("S3_URL_STYLE", "path") == "path" else "true"
    return opts


def state_versions(con):
    """(base, {table: version}) recorded in the lake, or (None, {}) without a Lance store."""
    rows = dict(con.sql("SELECT key, value FROM lake.state WHERE key = 'lance_base' "
                        "OR key LIKE 'lance_version:%'").fetchall())
    base = rows.pop("lance_base", None)
    return base, {k.split(":", 1)[1]: int(v) for k, v in rows.items()}


def _set_state(con, base, versions):
    con.sql("DELETE FROM lake.state WHERE key = 'lance_base' OR key LIKE 'lance_version:%'")
    rows = [("lance_base", base)] + [(f"lance_version:{t}", str(v)) for t, v in versions.items()]
    con.sql("INSERT INTO lake.state VALUES " + ", ".join(f"('{k}', '{v}')" for k, v in rows))


class Store:
    """Readers: datasets pinned at the versions a catalog records. Datasets are opened once and
    cached; checkout of another version shares the opened dataset's object-store client."""

    def __init__(self, base, versions):
        self.base, self.versions = base.rstrip("/"), versions
        self._open, self._lock = {}, threading.Lock()

    def uri(self, name):
        return f"{self.base}/{name}.lance"

    def dataset(self, name):
        with self._lock:
            ds = self._open.get(name)
            if ds is None:
                ds = lance.dataset(self.uri(name), version=self.versions.get(name),
                                   storage_options=storage_options())
                self._open[name] = ds
            return ds

    __call__ = dataset  # Engine(lance=store)


def build(con, base):
    """Write the Lance tables from the lake's current state and record their versions."""
    base = base.rstrip("/")
    versions = {}
    for name, (tbl, _, order) in TABLES.items():
        t = time.time()
        reader = con.sql(f"SELECT * FROM lake.{tbl} ORDER BY {order}").to_arrow_reader(ROWS_PER_GROUP * 8)
        ds = lance.write_dataset(reader, f"{base}/{name}.lance", mode="overwrite", max_rows_per_group=ROWS_PER_GROUP,
                                 storage_options=storage_options())
        ds.create_scalar_index("id", index_type="BTREE")
        ds = lance.dataset(f"{base}/{name}.lance", storage_options=storage_options())
        versions[name] = ds.version
        print(f"lance {name:<15} rows={ds.count_rows():>10} {time.time() - t:6.1f}s", flush=True)
    con.sql("BEGIN TRANSACTION")
    _set_state(con, base, versions)
    con.sql("COMMIT")
    return versions


def apply(con, base, versions, touched, rows):
    """Bring the Lance tables in line with one diff: delete every touched id, append the new rows.
    `rows`: lance table -> pyarrow table in the lake table's layout. Returns the new versions, to be
    recorded in lake.state in the diff's DuckLake transaction (the caller's)."""
    out = dict(versions)
    for name, (_, t, _) in TABLES.items():
        ids = touched.get(t)
        data = rows.get(name)
        if not ids and (data is None or not data.num_rows):
            continue
        ds = lance.dataset(f"{base}/{name}.lance", storage_options=storage_options())  # latest: see module doc
        if ids:
            ds.delete(f"id IN ({','.join(map(str, sorted(ids)))})")
        if data is not None and data.num_rows:
            ds = lance.write_dataset(data.cast(ds.schema), ds.uri, mode="append",
                                     max_rows_per_group=ROWS_PER_GROUP, storage_options=storage_options())
        out[name] = lance.dataset(f"{base}/{name}.lance", storage_options=storage_options()).version
    return out


def maintain(con, grace=3600):
    """Compaction for Lance: merge small fragments, index appended rows, drop versions older than
    the grace period. Records the new versions (one DuckLake commit)."""
    base, versions = state_versions(con)
    if not base:
        return
    new = {}
    for name in TABLES:
        t = time.time()
        ds = lance.dataset(f"{base}/{name}.lance", storage_options=storage_options())
        ds.optimize.compact_files(target_rows_per_fragment=1 << 20, max_rows_per_group=ROWS_PER_GROUP)
        ds = lance.dataset(ds.uri, storage_options=storage_options())
        ds.optimize.optimize_indices()
        ds = lance.dataset(ds.uri, storage_options=storage_options())
        ds.cleanup_old_versions(older_than=dt.timedelta(seconds=grace))
        new[name] = ds.version
        print(f"lance maintain {name:<15} v{versions.get(name)} -> v{ds.version} "
              f"({len(ds.get_fragments())} fragments) {time.time() - t:5.1f}s", flush=True)
    con.sql("BEGIN TRANSACTION")
    _set_state(con, base, new)
    con.sql("COMMIT")


def main():
    if len(sys.argv) != 3 or sys.argv[1] != "build":
        sys.exit(__doc__)
    import duckdb
    con = duckdb.connect()
    lakeconn.attach(con)
    build(con, sys.argv[2])


if __name__ == "__main__":
    main()
