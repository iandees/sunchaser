"""Stage 2: raw Parquet -> DuckLake tables keyed and sorted like Overpass.

Node/way spatial keys are computed in SQL (macros below) so the build scales to the planet in
DuckDB; relations use the Python port (src/quadtile.py) because their inputs mix compound indexes.
"""
import argparse
import pathlib
import shutil
import sys
import time

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).parent))
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default="data/raw")
    ap.add_argument("--lake", default="data/lake")
    ap.add_argument("--row-group", type=int, default=16384)
    ap.add_argument("--target-file-mb", type=int, default=64)
    args = ap.parse_args()
    raw, lake = pathlib.Path(args.raw), pathlib.Path(args.lake)
    if lake.exists():
        shutil.rmtree(lake)
    lake.mkdir(parents=True)

    con = duckdb.connect(str(lake / "build.duckdb"))
    con.sql("SET preserve_insertion_order = false")
    con.sql(MACROS)
    con.sql(f"ATTACH 'ducklake:{lake}/osm.ducklake' AS lake (DATA_PATH '{lake}/data')")
    for opt, val in (("parquet_compression", "'zstd'"), ("parquet_row_group_size", args.row_group),
                     ("target_file_size", f"'{args.target_file_mb}MB'"), ("data_inlining_row_limit", 0)):
        con.sql(f"CALL lake.set_option('{opt}', {val})")

    def step(name, sql):
        t = time.time()
        con.sql(sql)
        print(f"{name:<22} {time.time() - t:6.1f}s")

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

    # Layout mirrors Overpass: element skeletons apart from metadata (*_meta files), and tagged
    # nodes apart from untagged way vertices (Overpass reads tags_local before any skeleton).
    meta = "version, ts, changeset, uid, \"user\""
    step("lake.nodes_tagged", """
        CREATE TABLE lake.nodes_tagged SORTED BY (qt, id) AS
        SELECT qt, id, lat, lon, tags FROM n WHERE cardinality(tags) > 0 ORDER BY qt, id""")
    step("lake.nodes_untagged", """
        CREATE TABLE lake.nodes_untagged SORTED BY (qt, id) AS
        SELECT qt, id, lat, lon, tags FROM n WHERE cardinality(tags) = 0 ORDER BY qt, id""")
    step("lake.nodes (view)", """
        CREATE VIEW lake.nodes AS
        SELECT * FROM lake.nodes_tagged UNION ALL SELECT * FROM lake.nodes_untagged""")
    step("lake.ways", """
        CREATE TABLE lake.ways SORTED BY (qt_key31, id) AS
        SELECT ov_key31(qt) AS qt_key31, ov_level(qt) AS qt_level, qt, id, min_lat, max_lat, min_lon, max_lon,
               tags, nds, nd_lat, nd_lon
        FROM w ORDER BY qt_key31, id""")
    step("lake.relations", """
        CREATE TABLE lake.relations SORTED BY (qt_key31, id) AS
        SELECT ov_key31(qt) AS qt_key31, ov_level(qt) AS qt_level, qt, id, min_lat, max_lat, min_lon, max_lon,
               tags, members
        FROM r ORDER BY qt_key31, id""")
    for src, tbl, key in (("n", "nodes_meta", "qt"), ("w", "ways_meta", "ov_key31(qt)"), ("r", "relations_meta", "ov_key31(qt)")):
        step(f"lake.{tbl}", f"""
            CREATE TABLE lake.{tbl} SORTED BY (k, id) AS
            SELECT {key} AS k, id, {meta} FROM {src} ORDER BY k, id""")
    # kv = 'key=value' as one sort/filter column: a single zone-map range per lookup, where separate
    # k/v predicates made DuckDB probe v in every row group (266 vs 15 requests, results/NOTES.md).
    step("lake.tag_postings", """
        CREATE TABLE lake.tag_postings SORTED BY (type, kv, qt_key31) AS
        SELECT type, k || '=' || v AS kv, k, v, qt_key31, id FROM (
          SELECT 'n' AS type, unnest(map_keys(tags)) AS k, unnest(map_values(tags)) AS v, ov_key31(qt) AS qt_key31, id FROM n
          UNION ALL
          SELECT 'w', unnest(map_keys(tags)), unnest(map_values(tags)), ov_key31(qt), id FROM w
          UNION ALL
          SELECT 'r', unnest(map_keys(tags)), unnest(map_values(tags)), ov_key31(qt), id FROM r)
        ORDER BY type, kv, qt_key31, id""")
    step("lake.elem_locator", """
        CREATE TABLE lake.elem_locator SORTED BY (type, id) AS
        SELECT * FROM (SELECT 'n' AS type, id, qt FROM n UNION ALL SELECT 'w', id, qt FROM w
                       UNION ALL SELECT 'r', id, qt FROM r) ORDER BY type, id""")
    step("lake.node_parent_ways", """
        CREATE TABLE lake.node_parent_ways SORTED BY (node_id) AS
        SELECT DISTINCT unnest(nds) AS node_id, id AS way_id FROM w ORDER BY node_id""")
    step("lake.member_parent_rels", """
        CREATE TABLE lake.member_parent_rels SORTED BY (mtype, mid) AS
        SELECT DISTINCT m.type AS mtype, m.ref AS mid, m.id AS rel_id
        FROM (SELECT id, unnest(members, recursive := true) FROM r) m ORDER BY mtype, mid""")

    for tbl, files, mb in con.sql("""
            SELECT table_name, file_count, round(file_size_bytes / 1e6, 1)
            FROM ducklake_table_info('lake') ORDER BY 1""").fetchall():  # views are not listed
        print(f"  {tbl:<18} files={files:<4} {mb} MB")
    con.close()


if __name__ == "__main__":
    main()
