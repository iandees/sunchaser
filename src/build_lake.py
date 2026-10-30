"""Stage 2: raw Parquet -> DuckLake tables keyed and sorted like Overpass.

Two inputs:
- bucketed (rust/osm-extract, manifest.json format 2): keys, bboxes, the id locator and the
  node -> way index are precomputed, and rows are split into key-range buckets. Each table is
  filled bucket group by bucket group (sort a group, append it), so the build sorts at most
  --group-rows rows at a time and its memory and temp disk stay bounded at planet scale. Only
  tag_postings needs a global sort (by key=value), which DuckDB does out of core.
- flat (src/extract.py): nodes/ways/relations.parquet; node/way keys are computed in SQL (macros
  below) and relation keys with the Python port (src/quadtile.py).
"""
import argparse
import json
import os
import pathlib
import shutil
import sys
import time

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import lakeconn  # noqa: E402
import quadtile as qt  # noqa: E402

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

META_COLS = "version, ts, changeset, uid, \"user\""


def union(sources):
    return sources[0] if len(sources) == 1 else "(" + " UNION ALL ".join(f"SELECT * FROM {x}" for x in sources) + ")"


def bucketed_sources(con, raw, manifest, group_rows):
    """Group consecutive bucket files into read_parquet() sources of about group_rows rows."""
    if manifest.get("format") != 2:
        raise SystemExit(f"unsupported extract format {manifest.get('format')}")

    files = {}

    def groups(sub):
        files_ = sorted((raw / sub).glob("*.parquet"))
        rows = dict(con.sql(f"SELECT file_name, sum(num_rows) FROM parquet_file_metadata({[str(f) for f in files_]}) "
                            "GROUP BY 1").fetchall())
        out, cur, n = [], [], 0
        for f in files_:
            if cur and n + rows.get(str(f), 0) > group_rows:
                out.append(cur)
                cur, n = [], 0
            cur.append(str(f))
            n += rows.get(str(f), 0)
        out.append(cur)
        files[sub] = [str(f) for f in files_]
        return [f"read_parquet({g})" for g in out]

    src = {"n": groups("nodes"), "w": groups("ways"), "r": groups("relations"),
           "parents": groups("node_parent_ways"), "locator": str(raw / "locator"), "history": False}
    if manifest.get("history"):  # osm-extract --history: every version since the first
        src.update(history=True, nh=groups("nodes_history"), wh=groups("ways_history"),
                   rh=groups("relations_history"), changes=groups("changes"))
    files["locator"] = [str(f) for f in sorted((raw / "locator").glob("*.parquet"))]
    src["files"] = files
    return src


def stage_flat(con, raw, step):
    """Flat extract (src/extract.py): compute keys and bboxes into temp tables n, w, r."""
    step("stage nodes", f"""
        CREATE TEMP TABLE n AS
        SELECT ov_node_idx(lat, lon) AS qt, id, lat, lon, tags, {META_COLS}
        FROM '{raw}/nodes.parquet'""")

    # Way index: masked lat/lon extremes over member node indexes (calc_index for plain inputs).
    step("stage ways", f"""
        CREATE TEMP TABLE w AS
        WITH m AS (
          SELECT w.id, u.i AS seq, ov_node_idx(u.la, u.lo) AS nidx, u.la, u.lo
          FROM '{raw}/ways.parquet' w,
               LATERAL (SELECT unnest(range(len(w.nds))) AS i, unnest(w.nd_lat) AS la, unnest(w.nd_lon) AS lo) u
          WHERE u.la <> -2147483648),
        agg AS (
          SELECT id, arg_min(nidx, seq) AS first_idx,
                 min(nidx & 715827882) AS lat_min, max(nidx & 715827882) AS lat_max,
                 min(nidx & 1431655765) AS lon_min, max(nidx & 1431655765) AS lon_max,
                 min(la) AS min_lat, max(la) AS max_lat, min(lo) AS min_lon, max(lo) AS max_lon
          FROM m GROUP BY id)
        SELECT ov_way_idx(a.first_idx, a.lat_min, a.lat_max, a.lon_min, a.lon_max) AS qt,
               w.id, a.min_lat, a.max_lat, a.min_lon, a.max_lon, w.nds, w.nd_lat, w.nd_lon, w.tags,
               w.version, w.ts, w.changeset, w.uid, w."user"
        FROM '{raw}/ways.parquet' w LEFT JOIN agg a USING (id)""")

    t = time.time()
    rel_members = con.sql(f"""
        SELECT r.id, list(DISTINCT coalesce(n.qt, w.qt)) FILTER (WHERE coalesce(n.qt, w.qt) IS NOT NULL) AS idxs
        FROM (SELECT id, unnest(members, recursive := true) FROM '{raw}/relations.parquet') r
        LEFT JOIN n ON r.type = 'n' AND n.id = r.ref
        LEFT JOIN w ON r.type = 'w' AND w.id = r.ref
        GROUP BY r.id""").fetchall()
    rel_idx = [(rid, qt.calc_index(sorted(idxs or []))) for rid, idxs in rel_members]
    con.sql("CREATE TEMP TABLE rel_idx (id BIGINT, qt UINTEGER)")
    con.executemany("INSERT INTO rel_idx VALUES (?, ?)", rel_idx)
    print(f"{'relation idx (python)':<22} {time.time() - t:6.1f}s")
    step("stage relations", f"""
        CREATE TEMP TABLE r AS
        WITH mb AS (
          SELECT m.id, min(coalesce(n.lat, w.min_lat)) AS min_lat, max(coalesce(n.lat, w.max_lat)) AS max_lat,
                 min(coalesce(n.lon, w.min_lon)) AS min_lon, max(coalesce(n.lon, w.max_lon)) AS max_lon
          FROM (SELECT id, unnest(members, recursive := true) FROM '{raw}/relations.parquet') m
          LEFT JOIN n ON m.type = 'n' AND n.id = m.ref
          LEFT JOIN w ON m.type = 'w' AND w.id = m.ref
          GROUP BY m.id)
        SELECT coalesce(x.qt, 254)::UINTEGER AS qt, r.id, mb.min_lat, mb.max_lat, mb.min_lon, mb.max_lon,
               r.members, r.tags, {META_COLS}
        FROM '{raw}/relations.parquet' r LEFT JOIN rel_idx x USING (id) LEFT JOIN mb USING (id)""")


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
    ap.add_argument("--catalog", help="DuckLake catalog (sqlite:..., postgres:..., or a .ducklake file); "
                                      "default <lake>/osm.ducklake or $SUNCHASER_CATALOG")
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
    args = ap.parse_args()
    raw, lake = pathlib.Path(args.raw), pathlib.Path(args.lake)
    if lake.exists():
        shutil.rmtree(lake)
    lake.mkdir(parents=True)
    catalog = args.catalog or lakeconn.env("CATALOG") or f"{lake}/osm.ducklake"
    data_path = args.data_path or lakeconn.env("DATA_PATH") or f"{lake}/data"
    if catalog.startswith("sqlite:"):
        pathlib.Path(catalog[7:]).unlink(missing_ok=True)

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
    if manifest.exists():
        man = json.loads(manifest.read_text())
        args.osm_base = args.osm_base or man.get("osm_base", "")
        args.sequence = args.sequence or str(man.get("sequence", ""))
        if not args.osm_base:
            sys.exit("error: the extract has no replication timestamp (its PBF header lacks one, as Geofabrik's "
                     "files do), so the updater would have nowhere to start. Pass --osm-base: the extract's "
                     "cut-off, e.g. the next hour boundary after `osmium fileinfo -e -g data.timestamp.last FILE`.")
        src = bucketed_sources(con, raw, man, args.group_rows)
    else:
        stage_flat(con, raw, step)
        src = {k: [k] for k in ("n", "w", "r")} | {"locator": None, "parents": None, "history": False}
    # current rows' valid_from: from a history extract the start of the row's last state (version or
    # geometry change); from a snapshot the version timestamp, as Overpass assumes without attic data
    vf = "valid_from" if src["history"] else "ts AS valid_from"

    def fill(table, sort, select, sources, where="", append=False, threads=0):
        """Create lake.<table> from the first source and append the rest, each sorted by `sort`.
        Sources are disjoint, increasing key ranges, so the table ends up globally sorted.
        threads=1 for rows with long member lists: a parallel sorted insert into DuckLake runs out of
        memory on them (Berlin's relation history: 303 M members in 490 k rows, 4 threads, 10 GB),
        while one thread spills and finishes."""
        t = time.time()
        if threads:
            con.sql(f"SET threads = {threads}")
        last_report = t
        for i, source in enumerate(sources):
            sql = f"SELECT {select} FROM {source} {where} ORDER BY {sort}"
            con.sql(f"CREATE TABLE lake.{table} SORTED BY ({sort}) AS {sql}" if i == 0 and not append
                    else f"INSERT INTO lake.{table} BY NAME {sql}")
            if len(sources) > 1 and i + 1 < len(sources) and time.time() - last_report >= 30:
                last_report = time.time()  # a table of many groups takes hours at planet scale
                print(f"  lake.{table}: group {i + 1}/{len(sources)} done, {last_report - t:.0f}s, "
                      f"peak rss {peak_rss_gb():.1f} GB", flush=True)
        if threads:
            con.sql("RESET threads")
        print(f"{'lake.' + table:<22} {time.time() - t:6.1f}s  ({len(sources)} groups)  "
              f"peak rss {peak_rss_gb():.1f} GB", flush=True)

    def consume(*subs):
        """--consume: raw buckets no later step reads are deleted, so the build's peak disk use
        shrinks as it goes (the extract's raw output is the largest thing on disk)."""
        if not args.consume or "files" not in src:
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
    # The one global sort of the build: per type, out of core; types are appended in order.
    posting = "SELECT '{t}' AS type, unnest(map_keys(tags)) AS k, unnest(map_values(tags)) AS v, ov_key31(qt) AS qt_key31, id FROM {s}"
    postings = lambda t: [f"({posting.format(t=t, s=union(src[t]))})"]
    pcols = "type, k || '=' || v AS kv, k, v, qt_key31, id"

    fill("nodes_tagged", "qt, id", f"qt, id, lat, lon, tags, {vf}", src["n"], "WHERE cardinality(tags) > 0")
    fill("nodes_untagged", "qt, id", f"qt, id, lat, lon, tags, {vf}", src["n"], "WHERE cardinality(tags) = 0")
    step("lake.nodes (view)", """
        CREATE VIEW lake.nodes AS
        SELECT * FROM lake.nodes_tagged UNION ALL SELECT * FROM lake.nodes_untagged""")
    fill("nodes_meta", "k, id", f"qt AS k, id, {meta}", src["n"])
    fill("tag_postings", "type, kv, qt_key31, id", pcols, postings("n"))
    consume("nodes")

    fill("ways", "qt_key31, id", f"""ov_key31(qt) AS qt_key31, ov_level(qt) AS qt_level, qt, id,
         min_lat, max_lat, min_lon, max_lon, tags, nds, nd_lat, nd_lon, {vf}""", src["w"])
    fill("ways_meta", "k, id", f"ov_key31(qt) AS k, id, {meta}", src["w"])
    fill("tag_postings", "type, kv, qt_key31, id", pcols, postings("w"), append=True)
    consume("ways")

    fill("relations", "qt_key31, id", f"""ov_key31(qt) AS qt_key31, ov_level(qt) AS qt_level, qt, id,
         min_lat, max_lat, min_lon, max_lon, tags, members, {vf}""", src["r"], threads=1)
    fill("relations_meta", "k, id", f"ov_key31(qt) AS k, id, {meta}", src["r"])
    fill("tag_postings", "type, kv, qt_key31, id", pcols, postings("r"), append=True)
    fill("member_parent_rels", "mtype, mid", "DISTINCT m.type AS mtype, m.ref AS mid, m.id AS rel_id",
         [f"(SELECT id, unnest(members, recursive := true) FROM {union(src['r'])}) m"])
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
        fill("nodes_history", "qt, valid_from", f"qt, id, lat, lon, tags, {hist}", src["nh"], append=True)
        consume("nodes_history")
        fill("ways_history", "qt_key31, valid_from", f"""ov_key31(qt) AS qt_key31, ov_level(qt) AS qt_level, qt, id,
             min_lat, max_lat, min_lon, max_lon, tags, nds, nd_lat, nd_lon, {hist}""", src["wh"], append=True)
        consume("ways_history")
        fill("relations_history", "qt_key31, valid_from", f"""ov_key31(qt) AS qt_key31, ov_level(qt) AS qt_level, qt,
             id, min_lat, max_lat, min_lon, max_lon, tags, members, {hist}""", src["rh"], append=True, threads=1)
        consume("relations_history")
        # buckets are consecutive periods of change time: sorted one at a time, the log is in time order
        fill("changes", "ts, type, id", "ts, type, id, old_version, new_version, action, batch", src["changes"],
             append=True)
        consume("changes")

    if src["locator"]:  # already in id order: no sort
        t = time.time()
        con.sql("CREATE TABLE lake.elem_locator SORTED BY (type, id) AS " + " UNION ALL ".join(
            f"SELECT * FROM (SELECT '{k}' AS type, id, qt FROM '{src['locator']}/{k}.parquet')" for k in "nwr"))
        print(f"{'lake.elem_locator':<22} {time.time() - t:6.1f}s", flush=True)
        consume("locator")
    else:
        step("lake.elem_locator", """
            CREATE TABLE lake.elem_locator SORTED BY (type, id) AS
            SELECT * FROM (SELECT 'n' AS type, id, qt FROM n UNION ALL SELECT 'w', id, qt FROM w
                           UNION ALL SELECT 'r', id, qt FROM r) ORDER BY type, id""")
    if src["parents"]:  # distinct per way already, bucketed by node id
        fill("node_parent_ways", "node_id", "node_id, way_id", src["parents"])
        consume("node_parent_ways")
    else:
        step("lake.node_parent_ways", """
            CREATE TABLE lake.node_parent_ways SORTED BY (node_id) AS
            SELECT DISTINCT unnest(nds) AS node_id, id AS way_id FROM w ORDER BY node_id""")

    step("lake.state", f"""
        CREATE TABLE lake.state AS SELECT * FROM (VALUES
          ('osm_base', '{args.osm_base}'), ('sequence', '{args.sequence}'), ('batch', '0')) v(key, value)""")
    # Minutely updates are small: let them inline into the catalog until the next flush/compaction.
    con.sql("CALL lake.set_option('data_inlining_row_limit', 100000)")
    if catalog.startswith("sqlite:"):
        # WAL: API readers must not block the updater's commits ("database is locked")
        import sqlite3
        sqlite3.connect(catalog[len("sqlite:"):], timeout=30).execute("PRAGMA journal_mode=WAL").fetchall()

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
