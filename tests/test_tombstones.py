"""Insert-only reverse indexes: dropped pairs become tombstones, regained pairs revive the row that
is still there, readers anti-join the tombstones, and compaction applies them."""
import sys
import pathlib
import time

import duckdb
import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))
import lakeconn  # noqa: E402
import updater  # noqa: E402
from engine import Engine  # noqa: E402


@pytest.fixture
def lake(tmp_path):
    con = duckdb.connect()
    lakeconn.attach(con, catalog=str(tmp_path / "t.ducklake"), data_path=str(tmp_path / "data"))
    con.sql("CREATE TABLE lake.node_parent_ways (node_id BIGINT, way_id BIGINT)")
    con.sql("CREATE TABLE lake.tag_postings (type VARCHAR, kv VARCHAR, k VARCHAR, v VARCHAR, qt_key31 UBIGINT, id BIGINT)")
    con.sql("CREATE TABLE lake.member_parent_rels (mtype VARCHAR, mid BIGINT, rel_id BIGINT)")
    con.sql("CREATE TABLE lake.state (key VARCHAR, value VARCHAR)")
    con.sql(lakeconn.TOMBSTONE_DDL)
    con.sql("INSERT INTO lake.node_parent_ways VALUES (1, 10), (2, 10), (3, 10), (2, 20)")
    return con


def fake_updater(con):
    """An Updater without __init__ (no diff state, no Lance): con and an engine are all _tombstone needs."""
    up = updater.Updater.__new__(updater.Updater)
    up.con, up.e = con, Engine(con)
    return up


def pairs(con):
    return sorted(con.sql("SELECT node_id, way_id FROM lake.node_parent_ways").fetchall())


def gone(con):
    return sorted(con.sql("SELECT node_id, way_id FROM lake.node_parent_ways_gone").fetchall())


def test_dropped_pairs_become_tombstones_and_readers_skip_them(lake):
    con = lake
    up = fake_updater(con)
    # way 10 drops node 3 and gains node 4
    updater.Updater._rewrite_pairs(up, "node_parent_ways", "node_id", "way_id", {10: {1, 2, 3}}, {10: {1, 2, 4}})
    assert pairs(con) == [(1, 10), (2, 10), (2, 20), (3, 10), (4, 10)]  # nothing deleted in place
    assert gone(con) == [(3, 10)]
    e = Engine(con)
    assert e.tombstoned
    assert con.sql(e._parents_sql("node_parent_ways", "way_id", "node_id IN (3)")).fetchall() == []
    assert con.sql(e._parents_sql("node_parent_ways", "way_id", "node_id IN (2)")).fetchall() == [(10,), (20,)]


def test_regained_pair_revives_the_row_instead_of_duplicating_it(lake):
    con = lake
    up = fake_updater(con)
    updater.Updater._rewrite_pairs(up, "node_parent_ways", "node_id", "way_id", {10: {1, 2, 3}}, {10: {1, 2}})
    assert gone(con) == [(3, 10)]
    updater.Updater._rewrite_pairs(up, "node_parent_ways", "node_id", "way_id", {10: {1, 2}}, {10: {1, 2, 3}})
    assert gone(con) == []
    assert pairs(con) == [(1, 10), (2, 10), (2, 20), (3, 10)]  # the old row serves again, once


def test_postings_tombstones_carry_k_and_v_on_insert(lake):
    con = lake
    up = fake_updater(con)
    con.sql("CREATE TEMP TABLE old_n AS SELECT 7 AS id, 123456 AS qt, MAP {'amenity': 'cafe', 'name': 'A'} AS tags")
    con.sql("CREATE TEMP TABLE upd_n AS SELECT 7 AS id, 123456 AS qt, MAP {'amenity': 'cafe', 'cuisine': 'x'} AS tags")
    updater.Updater._rewrite_postings(up, "n", "upd_n", True)
    assert con.sql("SELECT kv, k, v, id FROM lake.tag_postings").fetchall() == [("cuisine=x", "cuisine", "x", 7)]
    assert con.sql("SELECT type, kv, id FROM lake.tag_postings_gone").fetchall() == [("n", "name=A", 7)]


def test_apply_tombstones_deletes_in_one_pass(lake, monkeypatch):
    con = lake
    up = fake_updater(con)
    updater.Updater._rewrite_pairs(up, "node_parent_ways", "node_id", "way_id", {10: {1, 2, 3}, 20: {2}}, {10: {1}, 20: set()})
    assert gone(con) == [(2, 10), (2, 20), (3, 10)]
    monkeypatch.setattr(updater, "TOMBSTONE_DAYS", 7)
    con.sql(f"INSERT INTO lake.state VALUES ('tombstones_applied_at', '{time.time():.0f}')")
    assert updater.apply_tombstones(con) is False  # not due
    monkeypatch.setattr(updater, "TOMBSTONE_DAYS", 0)
    assert updater.apply_tombstones(con) is True
    assert pairs(con) == [(1, 10)]
    assert gone(con) == []
    assert updater.apply_tombstones(con) is False  # nothing pending
