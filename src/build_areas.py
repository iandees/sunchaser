"""Build (or rebuild) lake.areas the way Overpass's area rules do (src/rules/areas.osm3s).

Pivot relations: type=multipolygon+name, type=boundary+name, admin_level+name, postal_code,
addr:postcode. For each, make-area (statements/make_area.cc) takes all member ways and:
  - skips the area if any way endpoint occurs an odd number of times (rings not closed), or
  - if a node of those ways is missing (an extract boundary);
otherwise the area (id = relation id + 3600000000, tags = relation tags) covers the even-odd
interior of all member ways' segments, built here with ST_BuildArea.

Incremental by default: only relations the updater changed since the last area build are
reprocessed. The changes log has a row for every relation whose version, tags or member geometry
changed (moved member nodes, member ways with new coordinates), stamped with a monotonic batch
number; state.areas_batch remembers how far the areas are. --full rebuilds everything (first build,
or a periodic safety net, e.g. weekly).
  python src/build_areas.py [--full]
"""
import pathlib
import sys
import time

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import lakeconn  # noqa: E402

AREA_OFFSET_RELATION = 3600000000

PIVOT = """(tags['type'] = 'multipolygon' AND tags['name'] IS NOT NULL)
        OR (tags['type'] = 'boundary' AND tags['name'] IS NOT NULL)
        OR (tags['admin_level'] IS NOT NULL AND tags['name'] IS NOT NULL)
        OR tags['postal_code'] IS NOT NULL OR tags['addr:postcode'] IS NOT NULL"""


def _state(con, key):
    rows = con.sql(f"SELECT value FROM lake.state WHERE key = '{key}'").fetchall()
    return rows[0][0] if rows else None


def _has_areas(con):
    return bool(con.sql("SELECT 1 FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = 'areas'").fetchall())


def build(con, full=False):
    """Bring lake.areas up to date; returns the number of relations processed."""
    t = time.time()
    con.sql("LOAD spatial")
    batch = _state(con, "batch")
    done = _state(con, "areas_batch")
    incremental = not full and _has_areas(con) and done is not None and batch is not None
    if incremental:
        con.sql(f"""CREATE OR REPLACE TEMP TABLE area_todo AS
                    SELECT DISTINCT id FROM lake.changes WHERE type = 'r' AND batch > {int(done)}""")
        n_todo = con.sql("SELECT count(*) FROM area_todo").fetchall()[0][0]
        scope = "AND id IN (SELECT id FROM area_todo)"
    else:
        scope = ""
    if incremental:
        # few relations: fetch their member ways by id (locator key ranges), not a join over all ways
        from engine import Engine
        members = con.sql(f"""
            SELECT r.id AS rel_id, r.ref AS way_id
            FROM (SELECT id, unnest(members, recursive := true) FROM lake.relations WHERE ({PIVOT}) {scope}) r
            WHERE r.type = 'w'""").arrow().read_all()
        way_ids = sorted(set(members.column("way_id").to_pylist()))
        import pyarrow as pa
        rows = Engine(con, "lake").elements_by_id("w", way_ids, ["id", "nds", "nd_lat", "nd_lon"])
        con.register("area_members", members)
        con.register("area_way_rows", pa.table({
            "way_id": pa.array([r[0] for r in rows], pa.int64()), "nds": [r[1] for r in rows],
            "nd_lat": pa.array([r[2] for r in rows], pa.list_(pa.int32())),
            "nd_lon": pa.array([r[3] for r in rows], pa.list_(pa.int32()))}))
        con.sql("""CREATE OR REPLACE TEMP TABLE area_ways AS
                   SELECT m.rel_id, w.way_id, w.nds, w.nd_lat, w.nd_lon
                   FROM area_members m JOIN area_way_rows w USING (way_id)""")
        con.unregister("area_members")
        con.unregister("area_way_rows")
    else:
        con.sql(f"""
            CREATE OR REPLACE TEMP TABLE area_ways AS
            SELECT r.id AS rel_id, w.id AS way_id, w.nds, w.nd_lat, w.nd_lon
            FROM (SELECT id, unnest(members, recursive := true) FROM lake.relations WHERE {PIVOT}) r
            JOIN lake.ways w ON r.type = 'w' AND w.id = r.ref""")
    # make_area.cc check_node_parity: endpoints of ways with >= 2 nodes must pair up
    con.sql("""
        CREATE OR REPLACE TEMP TABLE area_ok AS
        WITH ends AS (
          SELECT rel_id, nds[1] AS n FROM area_ways WHERE len(nds) >= 2
          UNION ALL SELECT rel_id, nds[len(nds)] FROM area_ways WHERE len(nds) >= 2),
        odd AS (SELECT rel_id FROM ends GROUP BY rel_id, n HAVING count(*) % 2 = 1),
        missing AS (SELECT DISTINCT rel_id FROM area_ways WHERE list_contains(nd_lat, -2147483648))
        SELECT DISTINCT rel_id FROM area_ways
        WHERE rel_id NOT IN (SELECT rel_id FROM odd) AND rel_id NOT IN (SELECT rel_id FROM missing)""")
    con.sql(f"""
        CREATE OR REPLACE TEMP TABLE new_areas AS
        WITH lines AS (
          SELECT rel_id,
                 ST_MakeLine(list_transform(range(1, len(nd_lat) + 1),
                   lambda i: ST_Point(nd_lon[i] / 1e7, (nd_lat[i]::BIGINT + 910000000) / 1e7 - 91.0))) AS g,
                 list_min(nd_lat) AS min_lat, list_max(nd_lat) AS max_lat,
                 list_min(nd_lon) AS min_lon, list_max(nd_lon) AS max_lon
          FROM area_ways WHERE len(nd_lat) >= 2 AND rel_id IN (SELECT rel_id FROM area_ok)),
        coll AS (
          SELECT rel_id, ST_Collect(list(g)) AS lines,
                 min(min_lat) AS min_lat, max(max_lat) AS max_lat, min(min_lon) AS min_lon, max(max_lon) AS max_lon
          FROM lines GROUP BY rel_id),
        agg AS (
          -- invalid linework (self-touching rings) makes GEOS throw; retry on noded linework
          SELECT rel_id, coalesce(TRY(ST_BuildArea(lines)), TRY(ST_BuildArea(ST_Node(lines)))) AS geom,
                 min_lat, max_lat, min_lon, max_lon
          FROM coll)
        SELECT a.rel_id + {AREA_OFFSET_RELATION} AS id, a.rel_id, r.tags,
               a.min_lat, a.max_lat, a.min_lon, a.max_lon, ST_AsWKB(a.geom) AS geom_wkb
        FROM agg a JOIN lake.relations r ON r.id = a.rel_id
        WHERE a.geom IS NOT NULL AND NOT ST_IsEmpty(a.geom)""")
    con.sql("BEGIN TRANSACTION")
    con.sql("CREATE TABLE IF NOT EXISTS lake.state (key VARCHAR, value VARCHAR)")
    if incremental:
        # areas of reprocessed relations are replaced; relations that stopped qualifying (tags changed,
        # deleted, rings broken) just lose theirs
        con.sql("DELETE FROM lake.areas WHERE rel_id IN (SELECT id FROM area_todo)")
        con.sql("INSERT INTO lake.areas SELECT * FROM new_areas ORDER BY id")
    else:
        con.sql("CREATE OR REPLACE TABLE lake.areas AS SELECT * FROM new_areas ORDER BY id")
    for key, value in (("areas_base", _state(con, "osm_base")), ("areas_batch", batch or "0")):
        con.sql(f"DELETE FROM lake.state WHERE key = '{key}'")
        con.sql(f"INSERT INTO lake.state VALUES ('{key}', '{value}')")
    con.sql("COMMIT")
    n = con.sql("SELECT count(*) FROM lake.areas").fetchall()[0][0]
    what = f"{n_todo} changed relations" if incremental else \
        f"{con.sql(f'SELECT count(*) FROM lake.relations WHERE {PIVOT}').fetchall()[0][0]} pivot relations (full)"
    print(f"areas: {n} total, processed {what} ({time.time() - t:.1f}s)", flush=True)
    return n_todo if incremental else None


def main():
    con = duckdb.connect()
    lakeconn.attach(con)
    build(con, full="--full" in sys.argv[1:])


if __name__ == "__main__":
    main()
