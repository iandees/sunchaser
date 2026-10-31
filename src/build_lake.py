"""Stage 2: raw Parquet -> DuckLake tables keyed and sorted like Overpass.

Input: the bucketed output of rust/osm-extract (manifest.json format 2): keys, bboxes, the id
locator and the node -> way index are precomputed, and rows are split into key-range buckets. Each
table is filled bucket group by bucket group (sort a group, append it), so the build sorts at most
--group-rows rows at a time and its memory and temp disk stay bounded at planet scale. tag_postings
is sorted by key=value, which the buckets are not, so its rows are first staged on local disk in
key=value ranges of that size and then sorted range by range.
The SQL macros below are shared with the updater, which computes keys for changed rows.
"""
import argparse
import json
import os
from concurrent.futures import ThreadPoolExecutor
import pathlib
import shutil
import sys
import time

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import lakeconn  # noqa: E402

# Overpass bit tricks as DuckDB macros (all on UBIGINT so shifts never overflow).
MACROS = """
CREATE OR REPLACE MACRO ov_s1(v) AS ((v | (v >> 8)) & 4278255360);   -- 0xFF00FF00
CREATE OR REPLACE MACRO ov_s2(v) AS ((v | (v >> 4)) & 4042322160);   -- 0xF0F0F0F0
CREATE OR REPLACE MACRO ov_s3(v) AS ((v | (v >> 2)) & 3435973836);   -- 0xCCCCCCCC
CREATE OR REPLACE MACRO ov_s4(v) AS ((v | (v >> 1)) & 2863311530);   -- 0xAAAAAAAA
CREATE OR REPLACE MACRO ov_spread2(v) AS ov_s4(ov_s3(ov_s2(ov_s1(v::UBIGINT & 4294901760))));
-- Overpass node index: ll_upper_(lat, lon); lat/lon are int32 1e-7 degrees.
CREATE OR REPLACE MACRO ov_node_idx(lat, lon) AS
  xor(ov_spread2((lat::BIGINT + 910000000)) | (ov_spread2((lon::BIGINT & 4294967295)) >> 1), 1073741824)::UINTEGER;
CREATE OR REPLACE MACRO ov_c1(q) AS ((q | (q >> 1)) & 858993459);    -- 0x33333333
CREATE OR REPLACE MACRO ov_c2(q) AS ((q | (q >> 2)) & 252645135);    -- 0x0F0F0F0F
CREATE OR REPLACE MACRO ov_c3(q) AS ((q | (q >> 4)) & 16711935);     -- 0x00FF00FF
CREATE OR REPLACE MACRO ov_compact(q) AS ((ov_c3(ov_c2(ov_c1(q))) | (ov_c3(ov_c2(ov_c1(q))) >> 8)) & 65535);
CREATE OR REPLACE MACRO ov_upper_ilat(q) AS ov_compact(((q::UBIGINT & 2863311530) >> 1));
CREATE OR REPLACE MACRO ov_upper_ilon(q) AS ov_compact((q::UBIGINT & 1431655765));
-- calc_index for a way whose members are all plain node indexes, given the aggregated bit-masked extremes.
CREATE OR REPLACE MACRO ov_way_idx(first_idx, lat_min, lat_max, lon_min, lon_max) AS
  CASE
    WHEN first_idx IS NULL THEN 254
    WHEN lat_min = lat_max AND lon_min = lon_max THEN first_idx
    WHEN (ov_upper_ilat(lat_max) & 65534) - (ov_upper_ilat(lat_min) & 65534) < 4
     AND (ov_upper_ilon(lon_max) & 65534) - (ov_upper_ilon(lon_min) & 65534) < 4
      THEN ((lon_min | lat_min) & 4294967292) | 2147483649
    WHEN (ov_upper_ilat(lat_max) & 65528) - (ov_upper_ilat(lat_min) & 65528) < 16
     AND (ov_upper_ilon(lon_max) & 65528) - (ov_upper_ilon(lon_min) & 65528) < 16
      THEN ((lon_min | lat_min) & 4294967232) | 2147483650
    WHEN (ov_upper_ilat(lat_max) & 65504) - (ov_upper_ilat(lat_min) & 65504) < 64
     AND (ov_upper_ilon(lon_max) & 65504) - (ov_upper_ilon(lon_min) & 65504) < 64
      THEN ((lon_min | lat_min) & 4294966272) | 2147483652
    WHEN (ov_upper_ilat(lat_max) & 65408) - (ov_upper_ilat(lat_min) & 65408) < 256
     AND (ov_upper_ilon(lon_max) & 65408) - (ov_upper_ilon(lon_min) & 65408) < 256
      THEN ((lon_min | lat_min) & 4294950912) | 2147483656
    WHEN (ov_upper_ilat(lat_max) & 65024) - (ov_upper_ilat(lat_min) & 65024) < 1024
     AND (ov_upper_ilon(lon_max) & 65024) - (ov_upper_ilon(lon_min) & 65024) < 1024
      THEN ((lon_min | lat_min) & 4294705152) | 2147483664
    WHEN (ov_upper_ilat(lat_max) & 63488) - (ov_upper_ilat(lat_min) & 63488) < 4096
     AND (ov_upper_ilon(lon_max) & 63488) - (ov_upper_ilon(lon_min) & 63488) < 4096
      THEN ((lon_min | lat_min) & 4290772992) | 2147483680
    WHEN (ov_upper_ilat(lat_max) & 57344) - (ov_upper_ilat(lat_min) & 57344) < 16384
     AND (ov_upper_ilon(lon_max) & 57344) - (ov_upper_ilon(lon_min) & 57344) < 16384
      THEN ((lon_min | lat_min) & 4227858432) | 2147483712
    ELSE 2147483776
  END::UINTEGER;
CREATE OR REPLACE MACRO ov_key31(idx) AS (((idx::UBIGINT & 2147483647) << 1) | (idx::UBIGINT >> 31));
CREATE OR REPLACE MACRO ov_level(idx) AS CASE
    WHEN idx::UBIGINT < 2147483648 THEN 0 WHEN idx & 1 <> 0 THEN 1 WHEN idx & 2 <> 0 THEN 2
    WHEN idx & 4 <> 0 THEN 3 WHEN idx & 8 <> 0 THEN 4 WHEN idx & 16 <> 0 THEN 5
    WHEN idx & 32 <> 0 THEN 6 WHEN idx & 64 <> 0 THEN 7 ELSE 8 END::UTINYINT;
"""

MIN_RUN_ROWS = 2_000_000  # smallest sorted insert worth its own transaction and files


def rp(files):
    """A source for `fill`: a list of Parquet files, or SQL as is."""
    return files if isinstance(files, str) else f"read_parquet({files})"


def lit(v):
    """A DuckDB literal for a sampled cut point (string, integer or timestamp)."""
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    if hasattr(v, "isoformat"):  # the literal's type must match the column's for filter pushdown
        return f"'{v.isoformat()}'::{'TIMESTAMPTZ' if v.tzinfo else 'TIMESTAMP'}"
    return str(v)


def bucketed_sources(con, raw, manifest, group_rows, run_rows):
    """Each table's bucket files as groups of runs. Buckets are disjoint key ranges in key order, so
    consecutive files sort and append on their own: a group is about group_rows rows (what one sort's
    memory and temp disk are bounded by), made of runs of about run_rows rows that are inserted
    concurrently. A bucket bigger than a run (dense regions: the planet's largest node bucket holds
    480 M rows against a mean of 10 M) is a run of its own; `fill` stages it in key ranges."""
    if manifest.get("format") != 2:
        raise SystemExit(f"unsupported extract format {manifest.get('format')}")

    files, glob, rows = {}, {}, {}

    def chunks(items, size, count):
        out, cur, n = [], [], 0
        for it in items:
            if cur and n + count(it) > size:
                out.append(cur)
                cur, n = [], 0
            cur.append(it)
            n += count(it)
        out.append(cur)
        return out

    def groups(sub):
        files[sub] = [str(f) for f in sorted((raw / sub).glob("*.parquet"))]
        glob[sub] = f"read_parquet('{raw / sub}/*.parquet')"
        rows.update(con.sql(f"SELECT file_name, sum(num_rows) FROM parquet_file_metadata({files[sub]}) GROUP BY 1").fetchall())
        runs = chunks(files[sub], run_rows, lambda f: rows.get(f, 0))
        return chunks(runs, group_rows, lambda r: sum(rows.get(f, 0) for f in r))

    src = {"n": groups("nodes"), "w": groups("ways"), "r": groups("relations"),
           "parents": groups("node_parent_ways"), "locator": str(raw / "locator"), "history": False}
    if manifest.get("history"):  # osm-extract --history: every version since the first
        src.update(history=True, nh=groups("nodes_history"), wh=groups("ways_history"),
                   rh=groups("relations_history"), changes=groups("changes"))
    files["locator"] = [str(f) for f in sorted((raw / "locator").glob("*.parquet"))]
    src["files"], src["glob"], src["rows"] = files, glob, rows
    return src


def peak_rss_gb():
    """Peak resident memory of this process (DuckDB's memory included), in GB."""
    import resource
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / (1 << 30 if sys.platform == "darwin" else 1 << 20)  # bytes on macOS, KB on Linux


def main():
    sys.stdout.reconfigure(line_buffering=True)  # progress shows up in `| tee build.log` as it happens
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default="data/raw")
    ap.add_argument("--lake", default="data/lake", help="local working dir (and default catalog/data location)")
    ap.add_argument("--catalog", help="DuckLake catalog file (.ducklake); default <lake>/osm.ducklake or $SUNCHASER_CATALOG")
    ap.add_argument("--data-path", help="where Parquet goes (s3://bucket/prefix/ for S3/R2); default <lake>/data")
    ap.add_argument("--osm-base", default="", help="timestamp of the source data (state.osm_base); default: "
                                                   "the PBF header's replication timestamp (bucketed input)")
    ap.add_argument("--sequence", default="", help="replication sequence of the source data (default: from the header)")
    ap.add_argument("--row-group", type=int, default=16384)
    ap.add_argument("--target-file-mb", type=int, default=64)
    ap.add_argument("--consume", action="store_true",
                    help="bucketed input: delete each raw bucket once no later step needs it (less peak disk)")
    ap.add_argument("--memory-limit", help="DuckDB memory_limit, e.g. 40GB (default: 80%% of RAM)")
    ap.add_argument("--lance", metavar="BASE", help="also build the Lance id store here (e.g. s3://bucket/lake/lance)")
    ap.add_argument("--group-rows", type=int, default=50_000_000,
                    help="bucketed input: rows sorted per insert (bounds memory and temp disk)")
    ap.add_argument("--parallel", type=int, default=os.cpu_count() or 1,
                    help="key sub-ranges of a group inserted concurrently (a sorted insert writes on one thread)")
    args = ap.parse_args()
    raw, lake = pathlib.Path(args.raw), pathlib.Path(args.lake)
    if lake.exists():
        shutil.rmtree(lake)
    lake.mkdir(parents=True)
    catalog = args.catalog or lakeconn.env("CATALOG") or f"{lake}/osm.ducklake"
    data_path = args.data_path or lakeconn.env("DATA_PATH") or f"{lake}/data"

    con = duckdb.connect(str(lake / "build.duckdb"))
    con.sql("SET preserve_insertion_order = false")
    if args.memory_limit:
        con.sql(f"SET memory_limit = '{args.memory_limit}'")
    con.sql(MACROS)
    lakeconn.attach(con, catalog, data_path)
    for opt, val in (("parquet_compression", "'zstd'"), ("parquet_row_group_size", args.row_group),
                     ("target_file_size", f"'{args.target_file_mb}MB'"), ("data_inlining_row_limit", 0)):
        con.sql(f"CALL lake.set_option('{opt}', {val})")

    def step(name, sql):
        t = time.time()
        con.sql(sql)
        print(f"{name:<22} {time.time() - t:6.1f}s  peak rss {peak_rss_gb():.1f} GB")

    manifest = raw / "manifest.json"
    if not manifest.exists():
        sys.exit(f"error: {manifest} not found: --raw must be the output of osm-extract (bucketed, format 2)")
    man = json.loads(manifest.read_text())
    args.osm_base = args.osm_base or man.get("osm_base", "")
    args.sequence = args.sequence or str(man.get("sequence", ""))
    if not args.osm_base:
        sys.exit("error: the extract has no replication timestamp (its PBF header lacks one, as Geofabrik's "
                 "files do), so the updater would have nowhere to start. Pass --osm-base: the extract's "
                 "cut-off, e.g. the next hour boundary after `osmium fileinfo -e -g data.timestamp.last FILE`.")
    run_rows = max(args.group_rows // args.parallel, MIN_RUN_ROWS)
    src = bucketed_sources(con, raw, man, args.group_rows, run_rows)
    full = {"n": "nodes", "w": "ways", "r": "relations"}
    # current rows' valid_from comes from the extractor: the start of the row's last state (version or
    # geometry change), or with --no-history the version timestamp, as Overpass assumes without attic data
    vf = "valid_from"

    def run_all(stmts):
        """Run the statements concurrently, up to --parallel at a time, each on its own connection
        (DuckLake appends to one table commit side by side)."""
        if len(stmts) == 1:
            return con.sql(stmts[0])

        def run(stmt):
            cur = con.cursor()
            try:
                cur.sql(stmt)
            finally:
                cur.close()
        with ThreadPoolExecutor(min(len(stmts), args.parallel)) as pool:
            list(pool.map(run, stmts))  # list() re-raises the first failure

    def stage(rows_sql, key, dir, n=None):
        """Write the rows of rows_sql to local Parquet under `dir`, partitioned into ranges of the
        expression `key` of about run_rows rows each (cut points from a 0.1% sample; n rows, or
        counted from the sample), in one parallel pass. Returns the ranges as sources in key order:
        each sorts and appends on its own, like a bucket run, where sorting the whole input at once
        would not fit (one ORDER BY over the planet's node postings ran out of memory at 70 GB)."""
        con.sql(f"CREATE OR REPLACE TEMP TABLE key_sample AS SELECT {key} AS x FROM ({rows_sql}) USING SAMPLE 0.1% (bernoulli)")
        n = n or con.sql("SELECT count(*) FROM key_sample").fetchall()[0][0] * 1000
        parts = max(1, -(-n // run_rows))
        cuts = con.sql(f"SELECT quantile_disc(x, {[i / parts for i in range(1, parts)]}) "
                       "FROM key_sample").fetchall()[0][0] if parts > 1 else []
        cuts = sorted(set(c for c in cuts if c is not None))  # a value with more than a range's share repeats
        part = ("CASE " + " ".join(f"WHEN {key} < {lit(c)} THEN {i}" for i, c in enumerate(cuts)) +
                f" ELSE {len(cuts)} END") if cuts else "0"
        con.sql(f"COPY (SELECT *, {part} AS part FROM ({rows_sql})) TO '{dir}' (FORMAT parquet, PARTITION_BY (part))")
        return [f"read_parquet('{dir}/part={i}/*.parquet')" for i in range(len(cuts) + 1) if (dir / f"part={i}").exists()]

    def fill(table, sort, select, groups, where="", append=False, threads=0, key=None):
        """Create lake.<table> and append each group's runs sorted by `sort`, the runs of a group
        concurrently: a sorted DuckLake insert writes its files on one thread (zstd, checksums and
        all), and one insert at a time left 11 of 12 cores idle for most of the planet build. Runs
        are disjoint, increasing key ranges, so the table ends up globally sorted and every file
        covers a narrow key range, which is all the readers' zone-map pruning depends on. A run that
        is one bucket bigger than a run is first staged in ranges of `key`, the leading sort key as
        an expression over the bucket's columns (see stage).
        threads=1 for rows with long member lists: a parallel sorted insert into DuckLake runs out of
        memory on them (Berlin's relation history: 303 M members in 490 k rows, 4 threads, 10 GB),
        while one thread spills and finishes; such a group is inserted whole."""
        t = time.time()
        if threads:
            con.sql(f"SET threads = {threads}")
        if not append:
            con.sql(f"CREATE TABLE lake.{table} SORTED BY ({sort}) AS SELECT {select} FROM {rp(groups[0][0])} {where} LIMIT 0")
        last_report = t
        for i, runs in enumerate(groups):
            if threads and len(runs) > 1:
                runs = [[f for r in runs for f in r]]
            stmts, staged = [], []
            for r in runs:
                n = src["rows"].get(r[0], 0) if len(r) == 1 else 0
                if key and not threads and n > run_rows * 3 // 2:
                    staged.append(lake / f"staged_{table}_{len(staged)}")
                    parts = stage(f"SELECT * FROM {rp(r)} {where}", key, staged[-1], n)
                    stmts += [f"INSERT INTO lake.{table} BY NAME SELECT {select} FROM {p} ORDER BY {sort}" for p in parts]
                else:
                    stmts.append(f"INSERT INTO lake.{table} BY NAME SELECT {select} FROM {rp(r)} {where} ORDER BY {sort}")
            run_all(stmts)
            for d in staged:
                shutil.rmtree(d)
            if len(groups) > 1 and i + 1 < len(groups) and time.time() - last_report >= 30:
                last_report = time.time()  # a table of many groups takes hours at planet scale
                print(f"  lake.{table}: group {i + 1}/{len(groups)} done, {last_report - t:.0f}s, "
                      f"peak rss {peak_rss_gb():.1f} GB", flush=True)
        if threads:
            con.sql("RESET threads")
        print(f"{'lake.' + table:<22} {time.time() - t:6.1f}s  ({len(groups)} groups of up to {max(map(len, groups))})  "
              f"peak rss {peak_rss_gb():.1f} GB", flush=True)

    def consume(*subs):
        """--consume: raw buckets no later step reads are deleted, so the build's peak disk use
        shrinks as it goes (the extract's raw output is the largest thing on disk)."""
        if not args.consume:
            return
        freed = 0
        for sub in subs:
            for f in src["files"].get(sub, []):
                freed += os.path.getsize(f)
                os.remove(f)
        print(f"{'consumed ' + '+'.join(subs):<22} {freed / 1e9:6.1f} GB freed", flush=True)

    # Tables are built type by type so each raw input can be freed after its last use.
    # Layout mirrors Overpass: element skeletons apart from metadata (*_meta files), and tagged
    # nodes apart from untagged way vertices (Overpass reads tags_local before any skeleton).
    meta = "version, ts, changeset, uid, \"user\""
    # kv = 'key=value' as one sort/filter column: a single zone-map range per lookup, where separate
    # k/v predicates made DuckDB probe v in every row group (266 vs 15 requests, results/NOTES.md).
    posting = ("SELECT type, k || '=' || v AS kv, k, v, qt_key31, id FROM (SELECT '{t}' AS type, "
               "unnest(map_keys(tags)) AS k, unnest(map_values(tags)) AS v, ov_key31(qt) AS qt_key31, id FROM {s})")
    psort = "type, kv, qt_key31, id"

    def fill_postings(t):
        """Append one type's postings, sorted by kv across the whole type like every other table is
        sorted across its key: the buckets are not in kv order, so the rows are staged in kv ranges
        first, then sorted and appended --parallel ranges at a time."""
        t0 = time.time()
        staged = lake / f"staged_postings_{t}"
        ranges = stage(posting.format(t=t, s=src["glob"][full[t]]), "kv", staged)
        print(f"  tag_postings {t}: staged in {len(ranges)} kv ranges, {time.time() - t0:.0f}s, "
              f"peak rss {peak_rss_gb():.1f} GB", flush=True)
        fill("tag_postings", psort, "type, kv, k, v, qt_key31, id",
             [ranges[j:j + args.parallel] for j in range(0, len(ranges), args.parallel)], append=True)
        shutil.rmtree(staged)

    fill("nodes_tagged", "qt, id", f"qt, id, lat, lon, tags, {vf}", src["n"], "WHERE cardinality(tags) > 0", key="qt")
    fill("nodes_untagged", "qt, id", f"qt, id, lat, lon, tags, {vf}", src["n"], "WHERE cardinality(tags) = 0", key="qt")
    step("lake.nodes (view)", """
        CREATE VIEW lake.nodes AS
        SELECT * FROM lake.nodes_tagged UNION ALL SELECT * FROM lake.nodes_untagged""")
    fill("nodes_meta", "k, id", f"qt AS k, id, {meta}", src["n"], key="qt")
    step("lake.tag_postings", f"CREATE TABLE lake.tag_postings SORTED BY ({psort}) AS "
         f"{posting.format(t='n', s=rp(src['n'][0][0]))} WHERE false")
    fill_postings("n")
    consume("nodes")

    fill("ways", "qt_key31, id", f"""ov_key31(qt) AS qt_key31, ov_level(qt) AS qt_level, qt, id,
         min_lat, max_lat, min_lon, max_lon, tags, nds, nd_lat, nd_lon, {vf}""", src["w"], key="ov_key31(qt)")
    fill("ways_meta", "k, id", f"ov_key31(qt) AS k, id, {meta}", src["w"], key="ov_key31(qt)")
    fill_postings("w")
    consume("ways")

    fill("relations", "qt_key31, id", f"""ov_key31(qt) AS qt_key31, ov_level(qt) AS qt_level, qt, id,
         min_lat, max_lat, min_lon, max_lon, tags, members, {vf}""", src["r"], threads=1)
    fill("relations_meta", "k, id", f"ov_key31(qt) AS k, id, {meta}", src["r"], key="ov_key31(qt)")
    fill_postings("r")
    fill("member_parent_rels", "mtype, mid", "DISTINCT m.type AS mtype, m.ref AS mid, m.id AS rel_id",
         [[f"(SELECT id, unnest(members, recursive := true) FROM {src['glob']['relations']}) m"]])
    consume("relations")

    # History (attic): versions the updater replaced, valid in [valid_from, valid_to). Current rows carry
    # valid_from: the version timestamp at build (what Overpass assumes without attic data), the change
    # time after an update (including geometry-only changes from moved nodes).
    step("lake.*_history", """
        CREATE TABLE lake.nodes_history SORTED BY (qt, valid_from) AS
          SELECT t.*, t.valid_from AS valid_to, 0::INT AS version, NULL::TIMESTAMPTZ AS ts, 0::BIGINT AS changeset,
                 0::INT AS uid, ''::VARCHAR AS "user" FROM lake.nodes_tagged t WHERE false;
        CREATE TABLE lake.ways_history SORTED BY (qt_key31, valid_from) AS
          SELECT w.*, w.valid_from AS valid_to, 0::INT AS version, NULL::TIMESTAMPTZ AS ts, 0::BIGINT AS changeset,
                 0::INT AS uid, ''::VARCHAR AS "user" FROM lake.ways w WHERE false;
        CREATE TABLE lake.relations_history SORTED BY (qt_key31, valid_from) AS
          SELECT r.*, r.valid_from AS valid_to, 0::INT AS version, NULL::TIMESTAMPTZ AS ts, 0::BIGINT AS changeset,
                 0::INT AS uid, ''::VARCHAR AS "user" FROM lake.relations r WHERE false;
        CREATE TABLE lake.changes (ts TIMESTAMPTZ, type VARCHAR, id BIGINT, old_version INT, new_version INT,
                                   action VARCHAR, batch BIGINT);""")
    if src["history"]:
        hist = f"valid_from, valid_to, {meta}"
        fill("nodes_history", "qt, valid_from", f"qt, id, lat, lon, tags, {hist}", src["nh"], append=True, key="qt")
        consume("nodes_history")
        fill("ways_history", "qt_key31, valid_from", f"""ov_key31(qt) AS qt_key31, ov_level(qt) AS qt_level, qt, id,
             min_lat, max_lat, min_lon, max_lon, tags, nds, nd_lat, nd_lon, {hist}""", src["wh"], append=True,
             key="ov_key31(qt)")
        consume("ways_history")
        fill("relations_history", "qt_key31, valid_from", f"""ov_key31(qt) AS qt_key31, ov_level(qt) AS qt_level, qt,
             id, min_lat, max_lat, min_lon, max_lon, tags, members, {hist}""", src["rh"], append=True, threads=1)
        consume("relations_history")
        # buckets are consecutive periods of change time: sorted one at a time, the log is in time order
        fill("changes", "ts, type, id", "ts, type, id, old_version, new_version, action, batch", src["changes"],
             append=True, key="ts")
        consume("changes")

    t = time.time()  # already in id order: no sort
    con.sql("CREATE TABLE lake.elem_locator SORTED BY (type, id) AS " + " UNION ALL ".join(
        f"SELECT * FROM (SELECT '{k}' AS type, id, qt FROM '{src['locator']}/{k}.parquet')" for k in "nwr"))
    print(f"{'lake.elem_locator':<22} {time.time() - t:6.1f}s", flush=True)
    consume("locator")
    fill("node_parent_ways", "node_id", "node_id, way_id", src["parents"], key="node_id")  # distinct per way, bucketed by node id
    consume("node_parent_ways")

    step("lake.state", f"""
        CREATE TABLE lake.state AS SELECT * FROM (VALUES
          ('osm_base', '{args.osm_base}'), ('sequence', '{args.sequence}'), ('batch', '0')) v(key, value)""")
    # Minutely updates are small: let them inline into the catalog until the next flush/compaction.
    con.sql("CALL lake.set_option('data_inlining_row_limit', 100000)")

    if args.lance:
        import lancestore
        lancestore.build(con, args.lance)

    for tbl, files, mb in con.sql("""
            SELECT table_name, file_count, round(file_size_bytes / 1e6, 1)
            FROM ducklake_table_info('lake') ORDER BY 1""").fetchall():  # views are not listed
        print(f"  {tbl:<18} files={files:<4} {mb} MB")
    con.close()


if __name__ == "__main__":
    main()
