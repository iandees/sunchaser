"""The cell cover of an area (cellcover.py) must give exactly the elements the exact geometry test gives.

  python -I tests/test_cellcover.py        (or pytest)
Uses an in-memory DuckDB with a star-shaped polygon with a hole, random nodes (plus nodes on cell edges and on
polygon vertices) and random ways (plus ways along the border), and compares Engine._in_area with and without cover.
"""
import math
import pathlib
import random
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import duckdb  # noqa: E402

import cellcover  # noqa: E402
import engine  # noqa: E402


class FakeEngine(engine.Engine):
    """Engine whose elements() reads an in-memory table instead of the lake."""
    def elements(self, s, t, cols):
        tbl = {"n": "fake_nodes", "w": "fake_ways"}[t]
        return self.con.sql(f"SELECT {', '.join(cols)} FROM {tbl}").fetchall()


def _setup():
    random.seed(7)
    con = duckdb.connect()
    con.sql("LOAD spatial")
    # star with a hole around (10.3, 50.2), roughly 3 x 3 degrees
    outer = []
    for i in range(400):
        a = 2 * math.pi * i / 400
        r = 1.5 + 0.35 * math.sin(9 * a) + 0.05 * math.sin(61 * a)
        outer.append((10.3 + r * math.cos(a), 50.2 + 0.8 * r * math.sin(a)))
    outer.append(outer[0])
    hole = [(10.3 + 0.4 * math.cos(2 * math.pi * i / 30), 50.2 + 0.3 * math.sin(2 * math.pi * i / 30)) for i in range(30)]
    hole.append(hole[0])
    ring = lambda pts: "(" + ",".join(f"{x} {y}" for x, y in pts) + ")"
    con.sql(f"""CREATE TABLE new_areas AS SELECT 3600000001::BIGINT AS id,
        (48.0 * 1e7)::INT AS min_lat, (52.5 * 1e7)::INT AS max_lat, (8.0 * 1e7)::INT AS min_lon, (13.0 * 1e7)::INT AS max_lon,
        ST_AsWKB(ST_GeomFromText('POLYGON({ring(outer)},{ring(hole)})')) AS geom_wkb""")
    assert cellcover.build_cells(con, "SELECT * FROM new_areas") == 1
    # a small area gets no cover
    con.sql("CREATE TABLE small AS SELECT 1::BIGINT AS id, 0 AS min_lat, 10000000 AS max_lat, 0 AS min_lon, 10000000 AS max_lon, geom_wkb FROM new_areas")
    assert cellcover.build_cells(con, "SELECT * FROM small") == 0
    con.sql("CREATE OR REPLACE TEMP TABLE new_areas2 AS SELECT * FROM new_areas")
    assert cellcover.build_cells(con, "SELECT * FROM new_areas2") == 1
    kinds = dict(con.sql("SELECT kind, count(*) FROM new_cells GROUP BY kind").fetchall())
    assert kinds[0] > 0 and kinds[1] > 0, kinds
    con.sql("CREATE TEMP TABLE cover AS SELECT cell, kind FROM new_cells")
    con.sql("CREATE TEMP TABLE area AS SELECT ST_GeomFromWKB(geom_wkb) AS g FROM new_areas")
    nodes = []
    for i in range(60000):
        nodes.append((i, round(random.uniform(47.9, 52.6) * 1e7), round(random.uniform(7.9, 13.1) * 1e7)))
    for k in range(3000):  # exactly on cell edges and corners
        nodes.append((100000 + k, random.randint(480, 525) * 10**6, random.randint(80, 130) * 10**6))
        nodes.append((200000 + k, random.randint(480, 525) * 10**6, round(random.uniform(8, 13) * 1e7)))
    # on the polygon's vertices
    for k, (x, y) in enumerate(outer[:-1] + hole[:-1]):
        nodes.append((300000 + k, round(y * 1e7), round(x * 1e7)))
    con.sql("CREATE TABLE fake_nodes(id BIGINT, lat INT, lon INT)")
    con.executemany("INSERT INTO fake_nodes VALUES (?, ?, ?)", nodes)
    ways = []
    for i in range(6000):
        la, lo = random.uniform(47.9, 52.6), random.uniform(7.9, 13.1)
        n = random.choice([1, 2, 3, 5])
        step = random.choice([0.003, 0.02, 0.3, 1.5])
        pts = [(la + random.uniform(-step, step), lo + random.uniform(-step, step)) for _ in range(n)]
        lat = [round(p[0] * 1e7) for p in pts]
        lon = [round(p[1] * 1e7) for p in pts]
        if i % 50 == 0 and n > 1:  # a missing node; with 2 nodes one is left, which is a point, not a line
            lat[0] = -2147483648  # missing node
        ways.append((i, lat, lon))
    for k in range(300):  # along the border: consecutive vertices of the outer ring
        j = random.randrange(len(outer) - 3)
        seg = outer[j:j + 3]
        ways.append((50000 + k, [round(y * 1e7) for x, y in seg], [round(x * 1e7) for x, y in seg]))
    con.sql("CREATE TABLE fake_ways(id BIGINT, nd_lat INT[], nd_lon INT[])")
    con.executemany("INSERT INTO fake_ways VALUES (?, ?, ?)", ways)
    return con


def test_cover_matches_exact():
    con = _setup()
    e = FakeEngine(con, "lake")
    for t in ("n", "w"):
        for border in (None, False, True):
            exact = e._in_area(t, "unused", "area", border)
            covered = e._in_area(t, "unused", "area", border, cover="cover")
            assert exact == covered, (t, border, len(exact - covered), len(covered - exact))
            assert 0 < len(exact)
    # the cover must actually skip work: most nodes resolve without the exact test
    nin = con.sql("SELECT count(*) FROM fake_nodes n JOIN cover c ON c.cell = " + cellcover.cell_sql("lat", "lon")
                  + " WHERE c.kind = 1").fetchall()[0][0]
    assert nin > 1000


if __name__ == "__main__":
    test_cover_matches_exact()
    print("ok")
