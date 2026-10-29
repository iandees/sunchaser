"""Build (or rebuild) lake.areas the way Overpass's area rules do (src/rules/areas.osm3s).

Pivot relations: type=multipolygon+name, type=boundary+name, admin_level+name, postal_code,
addr:postcode. For each, make-area (statements/make_area.cc) takes all member ways and:
  - skips the area if any way endpoint occurs an odd number of times (rings not closed), or
  - if a node of those ways is missing (an extract boundary);
otherwise the area (id = relation id + 3600000000, tags = relation tags) covers the even-odd
interior of all member ways' segments, built here with ST_BuildArea.

Like Overpass's rules loop this is a batch job: re-run it periodically on a live lake.
  python src/build_areas.py
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


def build(con):
    t = time.time()
    con.sql("LOAD spatial")
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
    con.sql("CREATE OR REPLACE TABLE lake.areas AS SELECT * FROM new_areas ORDER BY id")
    con.sql("""INSERT INTO lake.state SELECT 'areas_base', value FROM lake.state WHERE key = 'osm_base'
               AND NOT EXISTS (SELECT 1 FROM lake.state WHERE key = 'areas_base')""")
    con.sql("""UPDATE lake.state SET value = (SELECT value FROM lake.state WHERE key = 'osm_base')
               WHERE key = 'areas_base'""")
    con.sql("COMMIT")
    n = con.sql("SELECT count(*) FROM lake.areas").fetchall()[0][0]
    pivots = con.sql(f"SELECT count(*) FROM lake.relations WHERE {PIVOT}").fetchall()[0][0]
    print(f"areas: {n} of {pivots} pivot relations ({time.time() - t:.1f}s)")


def main():
    con = duckdb.connect()
    lakeconn.attach(con)
    build(con)


if __name__ == "__main__":
    main()
