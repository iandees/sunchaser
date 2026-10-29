"""Regression test: every engine-corpus query gives the same element set on the indexed lake as on
the brute-force raw evaluator (no object store or reference Overpass needed).

  python -I tests/test_corpus.py [--lake data/lake/osm.ducklake] [--raw data/raw]
"""
import argparse
import pathlib
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "queries")]
import duckdb  # noqa: E402
from corpus import CORPUS  # noqa: E402
from engine import Engine  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lake", default=str(ROOT / "data/lake/osm.ducklake"))
    ap.add_argument("--raw", default=str(ROOT / "data/raw"))
    args = ap.parse_args()
    lake = duckdb.connect()
    lake.sql("LOAD ducklake")
    lake.sql(f"ATTACH 'ducklake:{args.lake}' AS lake (READ_ONLY)")
    raw = duckdb.connect()
    bad = 0
    for name, _, prog in CORPUS:
        t = time.perf_counter()
        got = Engine(lake, "lake").ids(prog(Engine(lake, "lake")))
        t_lake = time.perf_counter() - t
        exp = Engine(raw, "raw", raw_dir=args.raw).ids(prog(Engine(raw, "raw", raw_dir=args.raw)))
        ok = got == exp
        bad += not ok
        print(f"{name:<28} {'OK ' if ok else 'BAD'} n={len(exp):<6} lake={t_lake:.2f}s"
              + ("" if ok else f" missing={len(exp - got)} extra={len(got - exp)}"))
    print(f"{len(CORPUS) - bad}/{len(CORPUS)} match")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
