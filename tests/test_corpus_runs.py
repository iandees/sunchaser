"""Every QL request in queries/ql_corpus.py runs to completion on a lake built by build_lake.py and
build_areas.py from a tiny synthetic extract (results are mostly empty; this catches broken code paths,
not wrong answers: tools/compare_overpass.py against a reference Overpass does that).

  python -m pytest tests/test_corpus_runs.py
"""
import json
import os
import pathlib
import subprocess
import sys

import duckdb
import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "queries")]
from build_lake import MACROS  # noqa: E402
from ql_corpus import QL, QL_HISTORY  # noqa: E402

META = ("1::INT AS version, TIMESTAMPTZ '2025-12-01 00:00:00+00' AS ts, 1::BIGINT AS changeset, 1::INT AS uid, "
        "'u' AS \"user\", TIMESTAMPTZ '2025-12-01 00:00:00+00' AS valid_from")


def write_raw(raw):
    """Three nodes, one way over them and one relation, in the extractor's bucketed layout (format 2)."""
    for d in ("nodes", "ways", "relations", "node_parent_ways", "locator"):
        (raw / d).mkdir(parents=True)
    con = duckdb.connect()
    con.sql(MACROS)
    con.sql(f"""CREATE TABLE n AS SELECT ov_node_idx(lat, lon) AS qt, id, lat, lon, MAP {{'amenity': 'cafe'}} AS tags, {META}
      FROM (VALUES (1, 525000000, 134000000), (2, 525010000, 134010000), (3, 525020000, 134020000)) v(id, lat, lon)""")
    con.sql(f"COPY n TO '{raw}/nodes/0.parquet'")
    con.sql(f"""CREATE TABLE w AS
      WITH m AS (SELECT ov_node_idx(lat, lon) AS nidx, lat, lon FROM n)
      SELECT ov_way_idx(min(nidx), min(nidx & 715827882), max(nidx & 715827882), min(nidx & 1431655765), max(nidx & 1431655765)) AS qt,
             10::BIGINT AS id, min(lat) AS min_lat, max(lat) AS max_lat, min(lon) AS min_lon, max(lon) AS max_lon,
             [1, 2, 3]::BIGINT[] AS nds, list(lat ORDER BY lat)::INT[] AS nd_lat, list(lon ORDER BY lon)::INT[] AS nd_lon,
             MAP {{'highway': 'primary', 'name': 'Unter den Linden'}} AS tags, {META} FROM m""")
    con.sql(f"COPY w TO '{raw}/ways/0.parquet'")
    con.sql(f"""COPY (SELECT qt, 20::BIGINT AS id, min_lat, max_lat, min_lon, max_lon,
      [{{'type': 'w', 'ref': 10::BIGINT, 'role': 'outer'}}, {{'type': 'n', 'ref': 1::BIGINT, 'role': ''}}] AS members,
      MAP {{'type': 'route', 'route': 'bus'}} AS tags, {META} FROM w) TO '{raw}/relations/0.parquet'""")
    con.sql(f"COPY (SELECT unnest([1, 2, 3])::BIGINT AS node_id, 10::BIGINT AS way_id) TO '{raw}/node_parent_ways/0.parquet'")
    con.sql(f"COPY (SELECT id, qt FROM n) TO '{raw}/locator/n.parquet'")
    con.sql(f"COPY (SELECT id, qt FROM w) TO '{raw}/locator/w.parquet'")
    con.sql(f"COPY (SELECT 20::BIGINT AS id, qt FROM w) TO '{raw}/locator/r.parquet'")
    (raw / "manifest.json").write_text(json.dumps({"format": 2, "osm_base": "2026-01-01T00:00:00Z", "sequence": 10}))


@pytest.fixture(scope="module")
def lake(tmp_path_factory):
    d = tmp_path_factory.mktemp("corpus")
    write_raw(d / "raw")
    cat = d / "catalog.ducklake"
    env = os.environ | {"SUNCHASER_CATALOG": str(cat)}

    def run(*args):
        r = subprocess.run([sys.executable, *map(str, args)], cwd=ROOT, env=env, capture_output=True, text=True)
        assert r.returncode == 0, r.stderr[-2000:]

    run("src/build_lake.py", "--raw", d / "raw", "--lake", d / "build", "--catalog", cat, "--data-path", f"{d}/data/",
        "--group-rows", 1000)
    run("src/build_areas.py", "--full")
    return cat


@pytest.mark.parametrize("name,query", QL + QL_HISTORY, ids=[n for n, _ in QL + QL_HISTORY])
def test_corpus_query_runs(lake, monkeypatch, name, query):
    import engine
    import server
    monkeypatch.setenv("SUNCHASER_CATALOG", str(lake))
    monkeypatch.setattr(server, "DB", None)
    monkeypatch.setattr(engine, "REL_MEMBER_PROBE_MAX", 0)  # take the many-members path too; the fixture is tiny
    body = server.run_query(query)[1]
    assert "runtime error" not in body and "static error" not in body, body[:500]
