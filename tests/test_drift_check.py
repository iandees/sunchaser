"""tools/drift_check.py on a tiny lake, local files only.

Recipe (also what this test does):
  1. write a flat raw extract (nodes/ways/relations.parquet) for the base state and for the state after a diff
  2. build lake BASE and lake FRESH (src/build_lake.py) from them
  3. roll BASE forward with the updater (Updater.apply) using the diff
  4. python tools/drift_check.py BASE/osm.ducklake FRESH/osm.ducklake   -> OK
  5. corrupt one row of the rolled-forward lake                         -> DRIFT, exit 1
"""
import gzip
import pathlib
import subprocess
import sys

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import extract  # noqa: E402

pytest.importorskip("osmium")
BASE_TS, NEW_TS = "2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"
E7 = 10_000_000


def meta(n, version=1, ts="2024-01-01T00:00:00+00:00"):
    import datetime
    t = datetime.datetime.fromisoformat(ts)
    return {"version": [version] * n, "ts": [t] * n, "changeset": [100] * n, "uid": [1] * n, "user": ["u"] * n}


def write_raw(d, nodes, ways, rels):
    """nodes: {id: (lat, lon, tags)}, ways: {id: (node ids, tags)}, rels: {id: (members, tags)}; versions in a dict."""
    d.mkdir(parents=True)
    ids = sorted(nodes)
    pq.write_table(pa.table({"id": ids, "lat": [nodes[i][0] for i in ids], "lon": [nodes[i][1] for i in ids],
                             "tags": [list(nodes[i][2].items()) for i in ids], **meta(len(ids), 1)},
                            schema=extract.SCHEMAS["nodes"]), d / "nodes.parquet")
    wids = sorted(ways)
    pq.write_table(pa.table({"id": wids, "nds": [ways[i][0] for i in wids],
                             "nd_lat": [[nodes[n][0] for n in ways[i][0]] for i in wids],
                             "nd_lon": [[nodes[n][1] for n in ways[i][0]] for i in wids],
                             "tags": [list(ways[i][1].items()) for i in wids], **meta(len(wids), 1)},
                            schema=extract.SCHEMAS["ways"]), d / "ways.parquet")
    rids = sorted(rels)
    pq.write_table(pa.table({"id": rids, "members": [[{"type": t, "ref": r, "role": ro} for t, r, ro in rels[i][0]]
                                                    for i in rids],
                             "tags": [list(rels[i][1].items()) for i in rids], **meta(len(rids), 1)},
                            schema=extract.SCHEMAS["relations"]), d / "relations.parquet")


def build(raw, lake, seq):
    subprocess.run([sys.executable, str(ROOT / "src/build_lake.py"), "--raw", str(raw), "--lake", str(lake),
                    "--osm-base", NEW_TS if seq == "2" else BASE_TS, "--sequence", seq],
                   check=True, capture_output=True)
    return str(lake / "osm.ducklake")


def base_state():
    nodes = {1: (450_000_000, 100_000_000, {}), 2: (450_100_000, 100_100_000, {}),
             3: (450_200_000, 100_200_000, {"amenity": "cafe", "name": "Old"}), 4: (460_000_000, 110_000_000, {})}
    ways = {10: ([1, 2, 3], {"highway": "residential"})}
    rels = {20: ([("n", 3, "stop"), ("w", 10, "route")], {"type": "route", "name": "R"})}
    return nodes, ways, rels


DIFF = f"""<?xml version='1.0' encoding='UTF-8'?>
<osmChange version="0.6">
  <modify>
    <node id="3" version="2" timestamp="{NEW_TS}" changeset="101" uid="2" user="v" lat="45.03" lon="10.03">
      <tag k="amenity" v="cafe"/><tag k="name" v="New"/></node>
  </modify>
  <create>
    <node id="5" version="1" timestamp="{NEW_TS}" changeset="101" uid="2" user="v" lat="45.05" lon="10.05">
      <tag k="shop" v="bakery"/></node>
  </create>
  <delete>
    <node id="4" version="2" timestamp="{NEW_TS}" changeset="101" uid="2" user="v" lat="46.0" lon="11.0"/>
  </delete>
</osmChange>"""


def final_state():
    nodes, ways, rels = base_state()
    nodes[3] = (450_300_000, 100_300_000, {"amenity": "cafe", "name": "New"})
    nodes[5] = (450_500_000, 100_500_000, {"shop": "bakery"})
    del nodes[4]
    return nodes, ways, rels


@pytest.fixture(scope="module")
def lakes(tmp_path_factory):
    import updater
    d = tmp_path_factory.mktemp("drift")
    write_raw(d / "raw0", *base_state())
    write_raw(d / "raw1", *final_state())
    rolled = build(d / "raw0", d / "lake_rolled", "1")
    fresh = build(d / "raw1", d / "lake_fresh", "2")
    # the rolled lake: version/ts of the diff's elements differ from the flat fixture, so fix them up below
    con = duckdb.connect()
    import lakeconn
    lakeconn.attach(con, catalog=rolled, data_path="")
    updater.Updater(con).apply([gzip.compress(DIFF.encode())], osm_base=NEW_TS, sequence="2")
    con.close()
    # the fixture's "fresh" build must carry the diff's versions to be the same moment
    con = duckdb.connect()
    lakeconn.attach(con, catalog=fresh, data_path="")
    for t, ids in (("nodes", (3, 5)),):
        con.sql(f"UPDATE lake.{t}_meta SET version = 2, ts = TIMESTAMPTZ '{NEW_TS}', changeset = 101, uid = 2, "
                f"\"user\" = 'v' WHERE id = 3")
        con.sql(f"UPDATE lake.{t}_meta SET ts = TIMESTAMPTZ '{NEW_TS}', changeset = 101, uid = 2, "
                f"\"user\" = 'v' WHERE id = 5")
    con.close()
    return rolled, fresh


def run(a, b, *extra):
    return subprocess.run([sys.executable, str(ROOT / "tools/drift_check.py"), a, b, *extra],
                          capture_output=True, text=True)


def test_identical_lakes_pass(lakes):
    rolled, fresh = lakes
    r = run(rolled, rolled)
    assert r.returncode == 0, r.stdout + r.stderr
    r = run(rolled, fresh)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "OK" in r.stdout


def test_corrupted_lake_fails(lakes, tmp_path):
    import shutil
    import lakeconn
    rolled, fresh = lakes
    bad_dir = tmp_path / "bad"
    shutil.copytree(pathlib.Path(rolled).parent, bad_dir)
    bad = str(bad_dir / "osm.ducklake")
    con = duckdb.connect()
    # the catalog stores absolute data paths: attach the copy with its own
    lakeconn.attach(con, catalog=bad, data_path=str(bad_dir / "data") + "/", override_data_path=True)
    con.sql("UPDATE lake.nodes_tagged SET lat = lat + 1 WHERE id = 3")
    con.sql("DELETE FROM lake.node_parent_ways WHERE node_id = 1")
    con.close()
    r = run(bad, fresh, "--data-path-a", str(bad_dir / "data") + "/")
    assert r.returncode == 1, r.stdout + r.stderr
    assert "nodes_tagged" in r.stdout and "DRIFT" in r.stdout
    assert "node_parent_ways" in r.stdout and "only in b" in r.stdout


def test_different_moment_is_not_comparable(lakes, tmp_path):
    rolled, fresh = lakes
    old = build(pathlib.Path(rolled).parent.parent / "raw0", tmp_path / "old", "1")
    r = run(rolled, old)
    assert r.returncode == 2, r.stdout + r.stderr
    assert "not at the same moment" in r.stdout
