#!/usr/bin/env python3
"""Drift check: compare a lake rolled forward by the updater with a fresh build of the same moment.

  python tools/drift_check.py CATALOG_A CATALOG_B [--data-path-a P] [--data-path-b P]
                              [--tables a,b] [--history] [--limit 10] [--shard I/N]

A and B are DuckLake catalogs as lakeconn takes them (.ducklake file, sqlite:..., postgres:...).
Convention: A is the rolled-forward lake (the one being checked), B the reference (a build from a
planet/extract replayed to the same replication sequence). Exit status 0: no drift, 1: drift,
2: not comparable (different state.sequence). S3 credentials come from the SUNCHASER_S3_* environment
(see lakeconn.py). Both catalogs are attached read-only; nothing is written.

Per table: row counts, then rows that exist on one side only or differ, matched by key and compared
through a hash of the comparable columns (MAP columns are key-sorted first). The first --limit
differing keys per table and kind are printed. Memory and time grow with the table (EXCEPT / hash
join): at planet scale run it in slices with --shard I/N (rows whose hash(key) % N == I) and point
DuckDB's temp directory at a big disk (SUNCHASER_DRIFT_TEMP).

WHAT IS COMPARED (current state of the data tables)
  nodes_tagged, nodes_untagged, nodes_meta, ways, ways_meta, relations, relations_meta, tag_postings,
  elem_locator, node_parent_ways, member_parent_rels, areas (when both sides have it), and
  state.osm_base / state.sequence (the two lakes must describe the same moment, or nothing else means
  anything).

WHAT LEGITIMATELY DIFFERS, AND IS EXCLUDED
  valid_from (current-row tables)  a snapshot build sets it to the version timestamp; the updater sets
                                   it to the change time (also for geometry-only changes from moved
                                   nodes). It cannot be compared against a snapshot build.
  nodes_history, ways_history,     only the updater (or a history extract) fills them; a snapshot build
  relations_history, changes       has none. Skipped unless --history, which is only meaningful when B
                                   was built from a --history extract. changes.batch and changes.ts are
                                   then excluded (batch counts applied diffs, ts is wall-clock).
  state.batch, compacted_at,       bookkeeping of the updater, not data.
  areas_batch
  Lance id store, Parquet file     physical layout, compaction state, snapshot ids: not compared.
  layout, row order
Any other table in the catalogs is listed as "not covered", never silently ignored.
"""
import argparse
import os
import pathlib
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))
import lakeconn  # noqa: E402

# table -> (key columns, or None for "the whole row"; excluded columns)
CURRENT = {
    "nodes_tagged": (["id"], ["valid_from"]),
    "nodes_untagged": (["id"], ["valid_from"]),
    "nodes_meta": (["id"], []),
    "ways": (["id"], ["valid_from"]),
    "ways_meta": (["id"], []),
    "relations": (["id"], ["valid_from"]),
    "relations_meta": (["id"], []),
    "tag_postings": (None, []),
    "elem_locator": (["type", "id"], []),
    "node_parent_ways": (None, []),
    "member_parent_rels": (None, []),
    "areas": (["id"], []),
}
HISTORY = {
    "nodes_history": (["id", "valid_from"], []),
    "ways_history": (["id", "valid_from"], []),
    "relations_history": (["id", "valid_from"], []),
    "changes": (["type", "id", "new_version", "action"], ["batch", "ts"]),
}
STATE_COMPARED = ("osm_base", "sequence")


def attach(con, alias, catalog, data_path):
    lakeconn.load(con, "ducklake", "httpfs")
    if catalog.startswith("sqlite:"):
        lakeconn.load(con, "sqlite")
    elif catalog.startswith("postgres:"):
        lakeconn.load(con, "postgres")
    lakeconn.configure_s3(con)
    opts = ["READ_ONLY"] + ([f"DATA_PATH '{data_path}'", "OVERRIDE_DATA_PATH true"] if data_path else [])
    con.sql(f"ATTACH 'ducklake:{catalog}' AS {alias} ({', '.join(opts)})")


def tables_of(con, alias):
    return {t for (t,) in con.sql(f"SELECT table_name FROM duckdb_tables() WHERE database_name = '{alias}'").fetchall()}


def columns_of(con, alias, table):
    return con.sql(f"SELECT column_name, data_type FROM duckdb_columns() WHERE database_name = '{alias}' "
                   f"AND table_name = '{table}' ORDER BY column_index").fetchall()


def q(c):
    return '"' + c.replace('"', '""') + '"'


def norm(col, typ):
    """Expression that is equal for equal values: MAPs are key-sorted (DuckDB keeps insertion order)."""
    return f"list_sort(map_entries({q(col)}))" if typ.startswith("MAP") else q(col)


def compare_table(con, table, key, excl, limit, shard, report):
    ca = [(c, t) for c, t in columns_of(con, "a", table) if c not in excl]
    cb = [(c, t) for c, t in columns_of(con, "b", table) if c not in excl]
    if ca != cb:
        report(table, f"SCHEMA DIFFERS: a={ca} b={cb}")
        return False
    key = key or [c for c, _ in ca]
    types = dict(ca)
    kcols = ", ".join(q(c) for c in key)
    row_hash = "hash(" + ", ".join(norm(c, t) for c, t in ca) + ")"
    where = ""
    if shard:
        where = f"WHERE hash({', '.join(norm(c, types[c]) for c in key)}) % {shard[1]} = {shard[0]}"

    def view(side):
        return f"(SELECT {kcols}, {row_hash} AS _h FROM {side}.{table} {where})"

    def keys(side):
        return f"SELECT {kcols} FROM {view(side)}"

    n = {s: con.sql(f"SELECT count(*) FROM {view(s)}").fetchall()[0][0] for s in "ab"}
    sets = {
        "only in a": f"{keys('a')} EXCEPT {keys('b')}",
        "only in b": f"{keys('b')} EXCEPT {keys('a')}",
        # same key on both sides, different content hash (duplicate keys count as one value each)
        "values differ": f"SELECT {kcols} FROM (SELECT * FROM {view('a')} EXCEPT SELECT * FROM {view('b')}) "
                         f"WHERE ({kcols}) IN ({keys('b')})",
    }
    counts = {k: con.sql(f"SELECT count(*) FROM ({sql})").fetchall()[0][0] for k, sql in sets.items()}
    line = f"rows a={n['a']} b={n['b']}"
    if n["a"] == n["b"] and not any(counts.values()):
        report(table, line + "; identical")
        return True
    report(table, line + (f" (diff {n['a'] - n['b']:+d})" if n["a"] != n["b"] else "")
           + "; " + ", ".join(f"{k}: {v}" for k, v in counts.items()))
    for label, sql in sets.items():
        if counts[label]:
            rows = con.sql(f"SELECT * FROM ({sql}) ORDER BY ALL LIMIT {limit}").fetchall()
            print(f"    {label} ({', '.join(key)}): " + "; ".join(",".join(map(str, r)) for r in rows))
    return False


def main():
    ap = argparse.ArgumentParser(description="Compare an updater-rolled lake (A) with a fresh build at the same moment (B).")
    ap.add_argument("catalog_a", help="rolled-forward lake (being checked)")
    ap.add_argument("catalog_b", help="reference lake (fresh build replayed to the same sequence)")
    ap.add_argument("--data-path-a")
    ap.add_argument("--data-path-b")
    ap.add_argument("--tables", help="comma-separated subset")
    ap.add_argument("--history", action="store_true", help="also compare *_history and changes (B built with --history)")
    ap.add_argument("--limit", type=int, default=10, help="differing keys to print per table and kind")
    ap.add_argument("--shard", help="I/N: compare only rows with hash(key) %% N == I")
    args = ap.parse_args()
    shard = tuple(map(int, args.shard.split("/"))) if args.shard else None

    con = duckdb.connect()
    if os.environ.get("SUNCHASER_DRIFT_TEMP"):
        con.sql(f"SET temp_directory = '{os.environ['SUNCHASER_DRIFT_TEMP']}'")
    attach(con, "a", args.catalog_a, args.data_path_a)
    attach(con, "b", args.catalog_b, args.data_path_b)

    spec = CURRENT | (HISTORY if args.history else {})
    wanted = set(args.tables.split(",")) if args.tables else None
    ta, tb = tables_of(con, "a"), tables_of(con, "b")
    width = max(len(t) for t in spec)

    def report(t, msg):
        print(f"{t:<{width}}  {msg}", flush=True)

    st = {s: dict(con.sql(f"SELECT key, value FROM {s}.state").fetchall()) for s in "ab"}
    for k in STATE_COMPARED:
        if st["a"].get(k) != st["b"].get(k):
            report(f"state.{k}", f"a={st['a'].get(k)!r} b={st['b'].get(k)!r}: the lakes are not at the same moment; "
                                 "replay the reference to A's sequence (updater --until) and retry")
            return 2
    report("state", "same moment: " + ", ".join(f"{k}={st['a'].get(k)}" for k in STATE_COMPARED))

    ok = True
    for t, (key, excl) in spec.items():
        if wanted and t not in wanted:
            continue
        if t not in ta and t not in tb:
            report(t, "absent on both sides, skipped")
        elif t not in ta or t not in tb:
            report(t, f"MISSING on side {'a' if t not in ta else 'b'}")
            ok = False
        else:
            ok &= compare_table(con, t, key, excl, args.limit, shard, report)

    extra = sorted((ta | tb) - set(CURRENT) - set(HISTORY) - {"state"})
    if extra:
        report("not covered", ", ".join(extra))
    if not args.history:
        report("skipped", "nodes_history, ways_history, relations_history, changes (use --history)")
    print("OK: no drift in the compared tables" if ok else "DRIFT")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
