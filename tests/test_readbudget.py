"""Pre-read budget (src/readbudget.py, SUNCHASER_MAX_READ_MB): predicate parsing, the catalog-based
estimate on a small synthetic lake, and the server refusing an oversized query before it scans.

  python -m pytest tests/test_readbudget.py
"""
import pathlib
import re
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src")]
import duckdb  # noqa: E402

import lakeconn  # noqa: E402
import readbudget  # noqa: E402
from build_lake import MACROS  # noqa: E402
from engine import SQL_MACROS, Engine  # noqa: E402

INF = readbudget.INF


# ---- predicate parsing ---------------------------------------------------------

def test_parse_bbox_and_or_chain():
    got = readbudget.parse_ranges([
        "lat BETWEEN 10 AND 20 AND lon BETWEEN -5 AND 5",
        "(qt BETWEEN 1 AND 3 OR qt BETWEEN 7 AND 7 OR qt BETWEEN 9 AND 12)"])
    assert got == [("lat", [(10, 20)]), ("lon", [(-5, 5)]), ("qt", [(1, 3), (7, 7), (9, 12)])]


def test_parse_way_bbox_comparisons():
    got = readbudget.parse_ranges(["max_lat >= 5 AND min_lat <= 9"])
    assert got == [("max_lat", [(5, INF)]), ("min_lat", [(-INF, 9)])]


def test_parse_ignores_what_it_does_not_understand():
    # an unparsed conjunct only weakens the filter (more files survive), it never drops a file wrongly
    assert readbudget.parse_ranges(["id IN (SELECT id FROM s1)", "false", "qt BETWEEN 1 AND 2 OR lat BETWEEN 3 AND 4"]) == []
    assert readbudget.parse_ranges(["id IN (30, 10, 20)"]) == [("id", [(10, 30)])]


# ---- a small synthetic lake ----------------------------------------------------

N = 60000


@pytest.fixture(scope="module")
def lake(tmp_path_factory):
    d = tmp_path_factory.mktemp("lake")
    con = duckdb.connect()
    try:
        con.sql("LOAD ducklake")
    except duckdb.Error:
        try:
            con.sql("INSTALL ducklake")
            con.sql("LOAD ducklake")
        except duckdb.Error:
            pytest.skip("ducklake extension unavailable")
    con.sql("SET preserve_insertion_order=false")
    con.sql(MACROS)
    con.sql(f"ATTACH 'ducklake:{d}/osm.ducklake' AS lake (DATA_PATH '{d}/data/')")
    for opt, val in (("parquet_compression", "'zstd'"), ("parquet_row_group_size", 2000),
                     ("target_file_size", "'100KB'"), ("data_inlining_row_limit", 0)):
        con.sql(f"CALL lake.set_option('{opt}', {val})")
    # two dense clusters (Berlin, Sydney) and a thin worldwide scatter
    con.sql(f"""CREATE TEMP TABLE n AS
        WITH g AS (SELECT i, random() r, random() a, random() b FROM range({N}) t(i))
        SELECT i + 1 AS id,
               (CASE WHEN r < .4 THEN 52.5 + (a - .5) * .4 WHEN r < .8 THEN -33.9 + (a - .5) * .4 ELSE -60 + a * 130 END * 1e7)::INT AS lat,
               (CASE WHEN r < .4 THEN 13.4 + (b - .5) * .6 WHEN r < .8 THEN 151.2 + (b - .5) * .6 ELSE -180 + b * 360 END * 1e7)::INT AS lon,
               map {{'amenity': 'cafe', 'name': 'n' || i}} AS tags
        FROM g""")
    con.sql("""CREATE TABLE lake.nodes_tagged SORTED BY (qt, id) AS
               SELECT ov_node_idx(lat, lon) AS qt, id, lat, lon, tags, 1 AS version FROM n ORDER BY qt, id""")
    con.sql("CREATE VIEW lake.nodes AS SELECT * FROM lake.nodes_tagged")
    assert con.sql("SELECT count(*) FROM ducklake_list_files('lake', 'nodes_tagged')").fetchall()[0][0] >= 8
    con.close()
    return d


def attach(lake):
    con = duckdb.connect()
    lakeconn.attach(con, f"{lake}/osm.ducklake", read_only=True)
    con.sql(SQL_MACROS)
    return con


def scan_sql(con, query_bbox, **kw):
    """The estimate and the scan's SQL for node(bbox) on the lake."""
    import ql
    b = readbudget.ReadBudget(con, 0)
    e = Engine(con, "lake", init_macros=False)
    e.budget = b
    seen = []
    e.trace = lambda sql, fn: (seen.append(sql), fn())[1]
    ql.Executor(e, "").run(ql.parse(f"[out:json];node({query_bbox});out count;"),
                           doc=ql.Document(fmt="json", timestamp=""))
    return b, seen


def test_estimate_tracks_pruned_files(lake):
    con = attach(lake)
    tiny, seen = scan_sql(con, "52.50,13.40,52.51,13.41")
    world, _ = scan_sql(con, "-90,-180,90,180")
    (_, kept, total, tiny_bytes), (_, kept_w, total_w, world_bytes) = tiny.log[0], world.log[0]
    assert total == total_w >= 8
    assert 1 <= kept < total          # a city block touches few files
    assert kept_w == total_w          # the world touches all of them
    assert tiny_bytes < world_bytes / 4
    # the files the estimate keeps are the files DuckDB reads (it pruned with the same ranges)
    scan = next(s for s in seen if "FROM lake.nodes WHERE" in s).split("AS SELECT DISTINCT type, id FROM (", 1)[1][:-1]
    analyzed = con.sql("EXPLAIN ANALYZE " + scan).fetchall()[0][1]
    assert int(re.search(r"Total Files Read: (\d+)", analyzed)[1]) == kept
    # an unfiltered world scan reads the referenced columns of every file, never more than the file sizes
    sizes = sum(f["size"] for f in world.files("nodes_tagged"))
    assert 0 < world_bytes <= sizes


def test_budget_raises_before_the_scan(lake):
    con = attach(lake)
    import ql
    e = Engine(con, "lake", init_macros=False)
    e.budget = readbudget.ReadBudget(con, 10_000)
    ran = []
    e.trace = lambda sql, fn: (ran.append(sql), fn())[1]
    with pytest.raises(readbudget.ReadBudgetExceeded) as err:
        ql.Executor(e, "").run(ql.parse("[out:json];node(-90,-180,90,180);out count;"),
                               doc=ql.Document(fmt="json", timestamp=""))
    assert err.value.estimated > err.value.limit == 10_000
    assert not any("FROM lake.node" in s for s in ran)  # nothing was scanned


# ---- the server -----------------------------------------------------------------

def run(server, q):
    return server.run_query(q)[1]


def test_server_refuses_oversized_query_only_when_enabled(lake, monkeypatch):
    import server
    monkeypatch.setenv("SUNCHASER_CATALOG", f"{lake}/osm.ducklake")
    monkeypatch.setattr(server, "DB", None)
    monkeypatch.setattr(server, "lance_store", lambda cur: None)
    world = "[out:json];node(-90,-180,90,180);out count;"
    tiny = "[out:json];node(52.50,13.40,52.51,13.41);out count;"

    monkeypatch.delenv("SUNCHASER_MAX_READ_MB", raising=False)   # default: off
    assert "runtime error" not in run(server, world)

    monkeypatch.setenv("SUNCHASER_MAX_READ_MB", "0.2")           # 200 KB: the city block passes, the world not
    out = run(server, world)
    assert "runtime error: Query would read about" in out and "limit of 0.2 MB" in out
    assert '"total"' not in out                                  # no count was produced
    ok = run(server, tiny)
    assert "runtime error" not in ok and '"total"' in ok
