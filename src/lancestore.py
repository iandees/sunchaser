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
import os
import pathlib
import sys
import threading
import time

# Lance's query/index memory pool is 1.8 GB unless LANCE_MEM_POOL_SIZE says otherwise: training the
# BTREE id index on the planet's 300 M tagged nodes died in its sort merge there (the sort spills to
# $TMPDIR, the merge cannot). Read at import, so it is set here: a quarter of RAM, 2 GB at least.
try:
    _ram = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
except (ValueError, OSError, AttributeError):
    _ram = 0
os.environ.setdefault("LANCE_MEM_POOL_SIZE", str(max(2 << 30, _ram // 4)))

import lance  # noqa: E402
import pyarrow as pa  # noqa: E402

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import lakeconn  # noqa: E402

TABLES = {  # lance table -> (lake table, element type, sort key)
    "nodes_tagged": ("nodes_tagged", "n", "qt, id"),
    "nodes_untagged": ("nodes_untagged", "n", "qt, id"),
    "ways": ("ways", "w", "qt_key31, id"),
}
ROWS_PER_GROUP = 16384
DELTA_MAX_ROWS = 50_000_000  # an index delta bigger than this is a base segment: new rows start another delta
ROWS_PER_FILE = 1 << 23  # 8 M rows per fragment at build (Lance's default 1 M: 10,000 uploads and a
#                          manifest of 10,000 entries for the planet's untagged nodes)
SEGMENT_ROWS = 1 << 28  # rows per id-index segment: 268 M sort in memory (300 M took 115 s, no spill)


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


def index_ids(ds, name):
    """The BTREE id index, trained one segment per group of fragments of about SEGMENT_ROWS rows and
    the segments merged (a streaming merge of sorted runs), instead of one sort of the whole column:
    that sort's merge step cannot spill and ran out of a 60 GB pool on the planet's 10.6 B untagged
    nodes. Should the merge fail, the segments are committed as they are: a lookup then probes each
    segment's tree. Returns the number of segments trained."""
    groups, cur, rows = [], [], 0
    for f in ds.get_fragments():
        cur.append(f.fragment_id)
        rows += f.metadata.physical_rows
        if rows >= SEGMENT_ROWS:
            groups.append(cur)
            cur, rows = [], 0
    if cur:
        groups.append(cur)
    t, segments = time.time(), []
    for i, g in enumerate(groups):
        segments.append(ds.create_index_uncommitted("id", "BTREE", name="id_idx", fragment_ids=g))
        if len(groups) > 1 and time.time() - t >= 60:
            t = time.time()
            print(f"  lance {name}: {i + 1}/{len(groups)} index segments trained", flush=True)
    if len(segments) > 1:
        try:
            segments = [ds.merge_existing_index_segments(segments)]
        except Exception as e:  # noqa: BLE001 - Lance raises OSError for its own errors
            print(f"  lance {name}: segment merge failed ({str(e)[:200]}); committing {len(segments)} segments", flush=True)
    ds.commit_existing_index_segments("id_idx", "id", segments)
    return len(groups)


def build(con, base, parallel=None, resume=False):
    """Write the Lance tables from the lake's current state and record their versions.
    A table is written as ranges of its leading sort key of about ROWS_PER_FILE rows (cut points from
    a 0.1% sample), `parallel` at a time: each range is sorted by its own DuckDB cursor, in memory,
    and written as fragments without a commit; the fragments are then committed together in key
    order, so the dataset is in the lake table's order as one sorted write would leave it. One
    sorted stream (a global sort spilling 450 GB, then one upload at a time, 60% of a core) took
    5.7 h for the planet's 10.6 B untagged nodes. The id index is built by `index_ids`.
    `resume`: a table whose dataset already holds the lake table's row count is kept (indexed if
    it has no id index yet) rather than written again."""
    from concurrent.futures import ThreadPoolExecutor
    base = base.rstrip("/")
    parallel = parallel or os.cpu_count() or 1
    versions = {}
    for name, (tbl, _, order) in TABLES.items():
        t = time.time()
        uri, key = f"{base}/{name}.lance", order.split(",")[0]
        n = con.sql(f"SELECT count(*) FROM lake.{tbl}").fetchall()[0][0]
        if resume:
            try:
                ds = lance.dataset(uri, storage_options=storage_options())
            except ValueError:  # no dataset there
                ds = None
            if ds is not None and ds.count_rows() == n:
                indexed = any(i["name"] == "id_idx" for i in ds.list_indices())
                segments = 0 if indexed else index_ids(ds, name)
                ds = lance.dataset(uri, storage_options=storage_options())
                versions[name] = ds.version
                print(f"lance {name:<15} rows={n:>10} {time.time() - t:6.1f}s  (kept from the last run"
                      f"{'' if indexed else f', index {segments} segments'})", flush=True)
                continue
        parts = max(1, -(-n // ROWS_PER_FILE))
        cuts = []
        if parts > 1:
            con.sql(f"CREATE OR REPLACE TEMP TABLE lance_sample AS SELECT {key} AS x FROM lake.{tbl} USING SAMPLE 0.1% (bernoulli)")
            cuts = con.sql(f"SELECT quantile_disc(x, {[i / parts for i in range(1, parts)]}) FROM lance_sample").fetchall()[0][0]
            cuts = sorted(set(c for c in cuts if c is not None))
        bounds = [None] + cuts + [None]
        ranges = [(bounds[i], bounds[i + 1]) for i in range(len(bounds) - 1)]  # half-open on the key: no row twice
        schema = con.sql(f"SELECT * FROM lake.{tbl} LIMIT 0").to_arrow_table().schema
        done, last_report, lock = [0], [t], threading.Lock()

        def write(rng):
            lo, hi = rng
            where = [f"{key} >= {lo}"] * (lo is not None) + [f"{key} < {hi}"] * (hi is not None)
            cur = con.cursor()
            try:
                reader = cur.sql(f"SELECT * FROM lake.{tbl} {'WHERE ' + ' AND '.join(where) if where else ''} "
                                 f"ORDER BY {order}").to_arrow_reader(ROWS_PER_GROUP * 8)
                frags = lance.fragment.write_fragments(reader, uri, schema, mode="overwrite", max_rows_per_file=ROWS_PER_FILE,
                                                       max_rows_per_group=ROWS_PER_GROUP, storage_options=storage_options())
            finally:
                cur.close()
            with lock:
                done[0] += 1
                if time.time() - last_report[0] >= 60:
                    last_report[0] = time.time()
                    print(f"  lance {name}: {done[0]}/{len(ranges)} ranges written, {last_report[0] - t:.0f}s", flush=True)
            return frags

        with ThreadPoolExecutor(min(parallel, len(ranges))) as pool:
            fragments = [f for frags in pool.map(write, ranges) for f in frags]  # map keeps the key order
        ds = lance.LanceDataset.commit(uri, lance.LanceOperation.Overwrite(schema, fragments), storage_options=storage_options())
        t_write = time.time() - t
        segments = index_ids(ds, name)
        ds = lance.dataset(uri, storage_options=storage_options())
        versions[name] = ds.version
        print(f"lance {name:<15} rows={ds.count_rows():>10} {time.time() - t:6.1f}s  ({len(ranges)} ranges, "
              f"{len(fragments)} fragments, write {t_write:.0f}s, index {time.time() - t - t_write:.0f}s in {segments} segments)", flush=True)
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


def maintain(con, grace=3600, tables=None):
    """Compaction for Lance: merge small fragments, index appended rows, drop versions older than
    the grace period. Records the new versions (one DuckLake commit). `tables` limits it to some
    of them (the updater does one per cycle); the others keep their recorded version."""
    base, versions = state_versions(con)
    if not base:
        return
    new = dict(versions)
    for name in tables or TABLES:
        t = time.time()
        ds = lance.dataset(f"{base}/{name}.lance", storage_options=storage_options())
        ds.optimize.compact_files(target_rows_per_fragment=1 << 20, max_rows_per_group=ROWS_PER_GROUP)
        ds = lance.dataset(ds.uri, storage_options=storage_options())
        # the appended rows join the newest index delta, unless that is a merged base segment (108 GB
        # for untagged nodes, which an hourly merge would rewrite): then a new delta starts. Lookups
        # probe every delta, so the planet keeps one base plus one growing hourly delta.
        deltas = ds.stats.index_stats("id_idx")["num_indexed_rows_per_delta"]
        ds.optimize.optimize_indices(num_indices_to_merge=0 if deltas and deltas[-1] > DELTA_MAX_ROWS else 1)
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
