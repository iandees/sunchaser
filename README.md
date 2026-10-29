# overpass-lake-spike

A single-region spike of an Overpass-compatible query engine on DuckDB + DuckLake (with a Lance comparison).
Results and findings: [`results/NOTES.md`](results/NOTES.md).

## Run it

```bash
python3 -m venv .venv && .venv/bin/pip install duckdb osmium pyarrow pylance numpy shapely
curl -o data/Berlin.osm.pbf https://download.bbbike.org/osm/bbbike/Berlin/Berlin.osm.pbf
.venv/bin/python -I src/extract.py data/Berlin.osm.pbf --out data/raw   # PBF -> raw Parquet (~5 min)
.venv/bin/python -I src/build_lake.py                                   # -> data/lake (DuckLake, ~70 s)
.venv/bin/python -I src/build_lance.py                                  # -> data/lance (optional)
.venv/bin/python -I tools/range_server.py data --port 8765 &            # object-store stand-in
.venv/bin/python -I src/run_corpus.py --latency-ms 30                   # 24 queries, lake vs brute force
.venv/bin/python -I src/bench_idlookup.py 30                            # Parquet vs Lance id lookups
.venv/bin/python -I tools/profile_query.py <query-name>                 # per-statement I/O breakdown
```

The quadtile port test needs the Overpass source (for its header) and a C++ compiler:

```bash
g++ -O2 -std=c++14 -I<Overpass-API>/src tools/qt_oracle.cc -o tools/qt_oracle && .venv/bin/python -I tests/test_quadtile.py
```

## Layout

| Path | What |
|---|---|
| `src/quadtile.py` | Exact port of Overpass's index math (node/way/relation keys, bbox → ranges, parents) |
| `src/extract.py` | PBF → raw Parquet |
| `src/build_lake.py` | Raw → DuckLake tables keyed/sorted like Overpass (SQL macros for the keys) |
| `src/engine.py` | Overpass statement semantics (query, recurse, set ops) over DuckDB; `lake` and brute-force `raw` modes |
| `queries/corpus.py` | Overpass QL corpus + equivalent engine programs |
| `tools/range_server.py` | HTTP/S3-ish server with latency injection and request/byte accounting |
| `results/` | JSON results and `NOTES.md` |
