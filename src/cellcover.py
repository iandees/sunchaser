"""Cell cover of areas (the idea of Overpass's area_blocks): lake.area_cells(area_id, cell, kind) lists the
0.1 degree grid cells an area covers. kind 1 = inside (the cell lies strictly in the polygon's interior), 0 = boundary
(the polygon's border crosses or touches the cell). Cells absent from the table do not meet the area.

A (area) filter then needs the exact geometry test only for candidates in boundary cells (nodes), or ways that span
cells; candidates in inside cells are in, candidates in cells without a row are out.

Only areas whose bounding box spans at least MIN_COVER_CELLS cells get a cover; smaller ones are cheap to test
exactly. Readers fall back to the exact test for areas without rows, and for lakes without the table.
"""
CELL = 0.1
STRIDE = 4096          # cell = (cy + 900) * STRIDE + (cx + 1800), cx = floor(lon * 10), cy = floor(lat * 10)
MIN_COVER_CELLS = 100
EPS = 1e-6             # degrees; classification uses cells grown by this, so float noise in the geometry
                       # (the Overpass latitude formula) cannot move a point across a verdict


def cell_sql(lat, lon):
    """SQL for the cell of a point given as 1e7-scaled integer columns/expressions."""
    return f"((floor(({lat}) / 1e6)::BIGINT + 900) * {STRIDE} + floor(({lon}) / 1e6)::BIGINT + 1800)"


def build_cells(con, src):
    """Create temp table new_cells(area_id, cell, kind) for the areas in `src` (a table or subquery with
    id, min_lat, max_lat, min_lon, max_lon, geom_wkb). Returns the number of areas covered."""
    con.sql(f"""
        CREATE OR REPLACE TEMP TABLE cov_src AS
        SELECT id, g, ST_Boundary(g) AS bg, x0, x1, y0, y1 FROM (
          SELECT id, ST_GeomFromWKB(geom_wkb) AS g,
                 floor(min_lon / 1e7)::INT AS x0, floor(max_lon / 1e7)::INT AS x1,
                 floor(min_lat / 1e7)::INT AS y0, floor(max_lat / 1e7)::INT AS y1,
                 ((floor(max_lon / 1e6) - floor(min_lon / 1e6) + 1) * (floor(max_lat / 1e6) - floor(min_lat / 1e6) + 1)) AS n
          FROM ({src})) WHERE n >= {MIN_COVER_CELLS}""")
    # 1 degree cells first, then only the ones the border passes through are refined to 0.1 degree
    con.sql(f"""
        CREATE OR REPLACE TEMP TABLE cov_coarse AS
        WITH xs AS (SELECT id, g, bg, y0, y1, unnest(range(x0, x1 + 1)) AS gx FROM cov_src),
             xy AS (SELECT id, g, bg, gx, unnest(range(y0, y1 + 1)) AS gy FROM xs),
             b AS (SELECT id, g, bg, gx, gy,
                          ST_MakeEnvelope(gx - {EPS}, gy - {EPS}, gx + 1 + {EPS}, gy + 1 + {EPS}) AS e FROM xy)
        SELECT id, g, gx, gy,
               CASE WHEN ST_Within(e, g) AND NOT ST_Intersects(bg, e) THEN 1 ELSE 0 END AS kind
        FROM b WHERE ST_Intersects(e, g)""")
    con.sql(f"""
        CREATE OR REPLACE TEMP TABLE new_cells AS
        SELECT id AS area_id, ((gy * 10 + j + 900) * {STRIDE} + gx * 10 + i + 1800)::BIGINT AS cell, 1::TINYINT AS kind
        FROM cov_coarse, range(10) t1(i), range(10) t2(j) WHERE kind = 1
        UNION ALL
        SELECT area_id, cell, kind::TINYINT FROM (
          SELECT c.id AS area_id, ((c.gy * 10 + j + 900) * {STRIDE} + c.gx * 10 + i + 1800)::BIGINT AS cell,
                 CASE WHEN ST_Within(e, c.g) AND NOT ST_Intersects(s.bg, e) THEN 1 ELSE 0 END AS kind,
                 ST_Intersects(e, c.g) AS hit
          FROM (SELECT *, ST_MakeEnvelope((gx * 10 + i) / 10.0 - {EPS}, (gy * 10 + j) / 10.0 - {EPS},
                                          (gx * 10 + i + 1) / 10.0 + {EPS}, (gy * 10 + j + 1) / 10.0 + {EPS}) AS e
                FROM cov_coarse, range(10) t1(i), range(10) t2(j) WHERE kind = 0) c
          JOIN cov_src s ON s.id = c.id) WHERE hit""")
    return con.sql("SELECT count(DISTINCT area_id) FROM new_cells").fetchall()[0][0]
