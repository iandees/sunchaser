# sunchaser

An Overpass API–compatible query service over OpenStreetMap data stored as a lakehouse on object
storage (S3, Cloudflare R2, or anything S3-compatible), kept current by a minutely updater.

- **Queries:** an Overpass QL parser and executor that mirrors the C++ Overpass semantics (bbox
  intersection rules, recursion, sets), and produces Overpass's JSON, XML and CSV output.
  Overpass Turbo can point at it directly.
- **Storage:** DuckLake tables (Parquet on S3/R2, catalog in SQLite or Postgres). Rows are clustered
  by Overpass's own quadtile index, so Parquet zone maps play the role of Overpass's block indexes.
- **Updates:** a single-writer updater applies OSM replication diffs with the node → way → relation
  cascade. Each diff is one DuckLake transaction, so readers see whole diffs only.

See [`docs/design.md`](docs/design.md) for the design and [`results/NOTES.md`](results/NOTES.md)
for the measurements that shaped it.

## Status

| | |
|---|---|
| Differential test vs. a real Overpass (Berlin extract, 58 requests) | 57/58 element-identical. The one difference is Overpass's relation `out bb` for members missing from an extract |
| QL coverage | Settings `[out:json\|xml\|csv]`, `[timeout]`, `[bbox]`; `node/way/rel/nwr/nw/nr/wr`; tag filters incl. regex and `,i`; `(bbox)`, `(id:…)`, `(.set)`, `(w)`, `(r[:role])`, `(bn)`, `(bw)`, `(br)`, `(around:…)`, `(around.set:…)`; unions, differences, named sets, `> >> < <<`; `out ids/skel/body/tags/meta/count` × `geom/bb/center` × `asc/qt`, limit, `noids` |
| Not yet | Areas (`area`, `(area)`, `map_to_area`, `is_in`), `poly`, attic queries (`[date]`, `[diff]`, `[adiff]`, `changed`, `newer`), evaluators (`if:`, `make`, `convert`, `foreach`) |

## Layout

| Path | What |
|---|---|
| `src/server.py` | HTTP API: `/api/interpreter` (GET/POST `data=`), `/api/status`, `/api/timestamp`; CORS for Turbo |
| `src/ql.py` | Overpass QL parser and executor |
| `src/output.py` | `out` statement and the JSON/XML/CSV writers |
| `src/engine.py` | Overpass statement semantics over DuckDB; DuckLake or Lance element access |
| `src/updater.py` | Replication follower: catch-up then minutely; `compact` for maintenance |
| `src/build_lake.py` | Raw Parquet → DuckLake tables with Overpass keys |
| `src/extract.py`, `rust/osm-extract` | PBF → raw Parquet (Python reference / 23× faster Rust) |
| `src/quadtile.py` | Exact port of Overpass's index math (fuzz-tested against the C++ header) |
| `src/lakeconn.py` | Catalog and S3/R2 connection setup shared by all tools |
| `tools/compare_overpass.py` | Differential test against `osm3s_query` |
| `tools/range_server.py` | S3-compatible test store with latency injection and I/O accounting |
| `queries/` | Engine corpus and full-QL differential corpus |

## Running it

```bash
python3 -m venv .venv
.venv/bin/pip install duckdb osmium pyarrow pylance numpy starlette uvicorn python-multipart pytz
```

Extract and build. The Rust extractor is the fast path; it needs `cargo` and outputs the same Parquet:

```bash
cargo build --release --manifest-path rust/osm-extract/Cargo.toml
```

```bash
rust/osm-extract/target/release/osm-extract region.osm.pbf data/raw
```

Configure storage. `deploy/local.env` targets the local test store; for R2, set
`SUNCHASER_S3_ENDPOINT=<account>.r2.cloudflarestorage.com`, `SUNCHASER_S3_USE_SSL=true`,
`SUNCHASER_S3_REGION=auto` and an R2 token's key id and secret:

```bash
. deploy/local.env
```

```bash
.venv/bin/python -I src/build_lake.py --raw data/raw --lake /tmp/build --osm-base 2026-10-24T00:00:00Z
```

Follow replication (omit `--region` for a planet lake):

```bash
.venv/bin/python -I src/updater.py run --region 52.23,12.76,52.82,13.98
```

Serve. Point Overpass Turbo's server setting at `http://localhost:8080/api/`:

```bash
SUNCHASER_PORT=8080 .venv/bin/python -I src/server.py
```

Compact periodically, e.g. hourly. This flushes inlined rows to sorted Parquet, merges small files
and expires old snapshots:

```bash
.venv/bin/python -I src/updater.py compact
```

## Differential testing

Build Overpass from source, load the same extract (`update_database --meta
--map-compression-method=gz`; without map compression the id-indexed `.map` files are tens of GB even
for a city), then:

```bash
.venv/bin/python -I tools/compare_overpass.py --db-dir /path/to/overpass-db
```
