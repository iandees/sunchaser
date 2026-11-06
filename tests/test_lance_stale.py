"""Lance id stores whose index misses too many appended rows are maintained before the next batch."""
import sys
import pathlib

import duckdb
import lance
import pyarrow as pa

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))
import lakeconn  # noqa: E402
import lancestore  # noqa: E402


def test_stale_lists_tables_with_unindexed_rows_over_the_limit(tmp_path):
    base = str(tmp_path / "lance")
    for name in lancestore.TABLES:
        ds = lance.write_dataset(pa.table({"id": pa.array([1, 2, 3], pa.int64())}), f"{base}/{name}.lance")
        ds.create_scalar_index("id", "BTREE", name="id_idx")
        assert lancestore.unindexed(ds) == 0
    ways = lance.dataset(f"{base}/ways.lance")
    ways = lance.write_dataset(pa.table({"id": pa.array([4, 5, 6, 7], pa.int64())}), ways.uri, mode="append")
    assert lancestore.unindexed(ways) == 4

    con = duckdb.connect()
    lakeconn.attach(con, catalog=str(tmp_path / "t.ducklake"), data_path=str(tmp_path / "data"))
    con.sql("CREATE TABLE lake.state (key VARCHAR, value VARCHAR)")
    assert lancestore.stale(con) == []  # no Lance store recorded
    lancestore._set_state(con, base, {name: 1 for name in lancestore.TABLES})
    assert lancestore.stale(con, limit=3) == ["ways"]
    assert lancestore.stale(con, limit=4) == []
