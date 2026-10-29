"""Lance copies of the element tables, each with BTree scalar indexes.

Benchmark tables (bench_idlookup.py):
  nodes_by_id / nodes_by_qt / ways_by_qt     BTree on id
Element store mirroring the lake layout (engine.Engine(lance=...)):
  nodes_tagged / nodes_untagged              sorted by qt,       BTree on id and qt
  ways                                       sorted by qt_key31, BTree on id and qt_key31
"""
import pathlib
import shutil
import time

import duckdb
import lance

OUT = pathlib.Path("data/lance")


def write(name, con, sql, rows_per_group=16384, extra_indexes=()):
    path = OUT / f"{name}.lance"
    if path.exists():
        shutil.rmtree(path)
    t = time.time()
    reader = con.sql(sql).to_arrow_reader(rows_per_group * 8)
    ds = lance.write_dataset(reader, str(path), max_rows_per_group=rows_per_group)
    ds.create_scalar_index("id", index_type="BTREE")
    for col in extra_indexes:
        ds.create_scalar_index(col, index_type="BTREE")
    size = sum(f.stat().st_size for f in path.rglob("*") if f.is_file())
    print(f"{name:<14} rows={ds.count_rows():>9} {size / 1e6:7.1f} MB {time.time() - t:6.1f}s")


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.sql("LOAD ducklake")
    con.sql("ATTACH 'ducklake:data/lake/osm.ducklake' AS lake (READ_ONLY)")
    write("nodes_by_id", con, "SELECT id, qt, lat, lon, tags FROM lake.nodes ORDER BY id")
    write("nodes_by_qt", con, "SELECT id, qt, lat, lon, tags FROM lake.nodes ORDER BY qt, id")
    write("ways_by_qt", con, "SELECT id, qt_key31, nds, nd_lat, nd_lon, tags FROM lake.ways ORDER BY qt_key31, id")
    for t in ("nodes_tagged", "nodes_untagged"):
        write(t, con, f"SELECT qt, id, lat, lon, tags FROM lake.{t} ORDER BY qt, id", extra_indexes=("qt",))
    write("ways", con, """SELECT qt_key31, id, min_lat, max_lat, min_lon, max_lon, tags, nds, nd_lat, nd_lon
                          FROM lake.ways ORDER BY qt_key31, id""", extra_indexes=("qt_key31",))


if __name__ == "__main__":
    main()
