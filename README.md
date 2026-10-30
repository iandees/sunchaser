# sunchaser

An Overpass API–compatible query service over OpenStreetMap data stored as a lakehouse on object
storage (S3, Cloudflare R2, or anything S3-compatible), kept current by a minutely updater.

- **Queries:** an Overpass QL parser and executor that mirrors the C++ Overpass semantics (bbox
  intersection rules, recursion, sets, areas, `(if:)` evaluators) and produces Overpass's JSON, XML
  and CSV output, streamed. Overpass Turbo can point at it directly.
- **Storage:** DuckLake tables: Parquet on S3/R2, clustered by Overpass's own quadtile index so
  Parquet zone maps play the role of Overpass's block indexes. The catalog is a single DuckDB file in
  the same bucket, so no database server is needed. A Lance copy of nodes and ways serves
  id-driven fetches.
- **History:** every replaced version is kept, so `[date:]`, `newer` and `changed` work. A lake can be
  built from a full-history planet, back to the first edits.
- **Updates:** a single writer applies OSM replication diffs with the node → way → relation
  cascade, one DuckLake transaction per diff. It runs as a scheduled job or as an always-on loop,
  and commits the catalog with a conditional PUT. API servers are read-only and stateless.

**Docs:**
- [`docs/architecture.md`](docs/architecture.md): how it works, as built, with the measurements behind the design.
- [`docs/runbook.md`](docs/runbook.md): planet build and operation on Cloudflare R2 (or S3) with a Hetzner box or a laptop; failure handling; planet sizing.
- [`docs/design.md`](docs/design.md): the original design exploration (historical).
- [`results/NOTES.md`](results/NOTES.md): the October 2026 Berlin spike record; [`results/README.md`](results/README.md) indexes the result files.

## Status

| | |
|---|---|
| Correctness vs. a real Overpass 0.7.62 (Berlin) | 81/82 queries element-identical over a fixed 24 h window, including 13/13 history queries against an Overpass with attic data. The one difference is Overpass's `out bb` placeholder for relation members missing from an extract. The full-history build gives the same answers. Record: `results/overpass_diff_history_build.json` |
| Overpass Turbo | The real overpass-turbo.eu loaded and rendered results from the service when it was checked (`results/turbo.png`); the checking script has since been removed |
| Updates | OSMF replication (hourly catch-up, then minutely); node, way, relation cascade; incremental areas; Lance kept in step. Minnesota on R2: a batch of 2-3 minutely diffs commits in 3-14 s |
| Speed | Berlin on a local S3 stand-in at 30 ms per request: median query 0.81 s with Lance, 1.44 s without. Minnesota on real R2 from the Hetzner box: median 4.4 s with Lance, 5.5 s without, 1.32× geometric mean (`docs/architecture.md`, Measurements) |
| QL coverage | Settings `[out:json\|xml\|csv]`, `[timeout]`, `[maxsize]`, `[bbox]`, `[date]`; `(newer)`, `(changed)`; `node/way/rel/nwr/nw/nr/wr/area`; tag filters incl. regex and `,i`; `(bbox)`, `(id:...)`, `(.set)`, `(w)`, `(r[:role])`, `(bn)`, `(bw)`, `(br)`, `(around:...)`, `(around.set:...)`, `(poly:...)`, `(area[.x])`, `(pivot[.x])`, `(if: ...)` evaluators; unions, differences, named sets, `> >> < <<`, `foreach`, `map_to_area`, `is_in`; `out ids/skel/body/tags/meta/count` x `geom[(bbox)]/bb/center` x `asc/qt`, limit, `noids` |
| Planet | Not built yet. Germany (12.4 GB history PBF) and Minnesota have been built end to end on the target box. The planet extract has an open risk: its dense index (about 490 GB) will not fit in the box's RAM ([runbook](docs/runbook.md)) |
| Not yet | `[diff]`, `[adiff]`, `timeline`, `retro`; `make`/`convert`/`for`/`complete`/`compare`/`if` blocks; antimeridian-crossing boxes |

## Layout

| Path | What |
|---|---|
| `src/server.py` | HTTP API: `/api/interpreter` (GET/POST `data=`), `/api/status`, `/api/timestamp`; CORS for Turbo; downloads the s3 catalog to a local file and re-fetches it when the ETag changes |
| `src/ql.py` | Overpass QL parser and executor |
| `src/output.py` | `out` statement and the JSON/XML/CSV writers |
| `src/engine.py` | Overpass statement semantics over DuckDB; DuckLake or Lance element access |
| `src/geometry.py` | Overpass's geometry rules (`Bbox`, `Bbox_Double::intersects`, great-circle distance) |
| `src/evaluator.py` | `(if: ...)` evaluator expressions |
| `src/readbudget.py` | Pre-read budget (`SUNCHASER_MAX_READ_MB`): refuses a query that catalog statistics say would read too much |
| `src/quadtile.py` | Exact port of Overpass's index math (fuzz-tested against the C++ header) |
| `src/lakeconn.py` | Catalog and S3/R2 connection setup shared by all tools |
| `src/build_lake.py` | Raw Parquet to DuckLake tables with Overpass keys (bucketed input, sorted one key-range group at a time); optional Lance store |
| `src/build_areas.py` | Overpass's area rules into `lake.areas`, incrementally from the changes log (`--full` rebuilds) |
| `src/updater.py` | The only writer: `job` (one scheduled run), `job --loop` (always on), `run` (`job --loop` on a local catalog from `SUNCHASER_CATALOG`), `apply`; incremental areas and compaction run inside |
| `src/objcatalog.py` | The DuckLake catalog as an object in S3/R2: conditional upload, writer lease (no database server) |
| `src/lancestore.py` | Lance id store for id-driven fetches, kept in step with the lake and pinned per catalog |
| `rust/osm-extract/` | PBF to raw Parquet (`src/main.rs`, `history.rs`, `relstate.rs`, `tables.rs`, `quadtile.rs`): computes all keys and bboxes, writes key-range buckets, has a dense mmap index for the planet, builds full history from a history PBF (`--no-history` for a current-only file) |
| `tools/compare_overpass.py` | Differential test against `osm3s_query` (`--history` for attic queries) |
| `tools/sync_reference.py` | Replays the lake's diffs into the reference Overpass |
| `tools/make_history_extract.py` | Synthesises a history file (snapshot + applied diffs) to test the full-history build |
| `tools/fetch_parallel.py` | Ordered multi-connection download to stdout (streams the planet into the extractor) |
| `tools/measure.py` | Time, peak memory and peak disk of a command |
| `tools/relocate_lake.py` | Points a lake built on local disk at its new location after copying it to R2/S3 |
| `tools/qt_oracle.cc` | Harness around Overpass's index header, used by `tests/test_quadtile.py` |
| `queries/` | `corpus.py` Berlin query set; `ql_corpus.py` full-request differential corpus |
| `tests/` | `test_quadtile.py`, `test_evaluator.py`, `test_output_json.py`, `test_ql_corpus.py` (every corpus request parses), `test_readbudget.py`, `test_compare_tools.py` |
| `deploy/hetzner/` | `setup.sh`, `build-planet.sh`, `installimage.conf`, systemd units, `sunchaser.env.example`, `Caddyfile.example` |
| `docs/` | `architecture.md`, `runbook.md`, `design.md` |
| `results/` | Spike record and result files; see `results/README.md` |

## Running it (an extract, locally)

For a planet deployment, follow [`docs/runbook.md`](docs/runbook.md).

1. **Dependencies** are managed with [uv](https://docs.astral.sh/uv/) (`pyproject.toml`, `uv.lock`); the extractor needs `cargo`:

   ```bash
   uv sync
   ```

   ```bash
   cargo build --release --manifest-path rust/osm-extract/Cargo.toml
   ```

2. **Extract** a regional PBF (`--no-history`, since a normal extract has one version per element). Use `--history` on a history file (`.osh.pbf`):

   ```bash
   rust/osm-extract/target/release/osm-extract region.osm.pbf data/raw --no-history
   ```

3. **Build the lake**, here with a catalog file and data on local disk. `--data-path s3://...` writes to a bucket instead (`SUNCHASER_S3_*` settings). `osm_base` comes from the PBF header, or pass `--osm-base`:

   ```bash
   uv run python src/build_lake.py --raw data/raw --lake data/build --catalog data/catalog.ducklake --data-path data/lake/
   ```

   ```bash
   SUNCHASER_CATALOG=data/catalog.ducklake uv run python src/build_areas.py --full
   ```

4. **Follow replication.** `--region` keeps an extract consistent; omit it for the planet. `run` is `job --loop` on the local catalog:

   ```bash
   SUNCHASER_CATALOG=data/catalog.ducklake uv run python src/updater.py run --region 52.23,12.76,52.82,13.98
   ```

5. **Serve.** Point Overpass Turbo's server setting at `http://localhost:8080/api/`:

   ```bash
   SUNCHASER_CATALOG=data/catalog.ducklake SUNCHASER_PORT=8080 uv run python src/server.py
   ```

With the catalog in a bucket instead, the writer is `updater.py job --loop --catalog-url s3://.../catalog.ducklake --workdir DIR`, and servers use `SUNCHASER_CATALOG=s3://.../catalog.ducklake`. A catalog is a `.ducklake` file or an `s3://` URL; SQLite and Postgres catalogs are gone.

Tests: `uv run python -m pytest tests`.

## Differential testing

Build Overpass from source, load the same extract (`update_database --meta
--map-compression-method=gz`; without map compression the id-indexed `.map` files are tens of GB even
for a city), then:

```bash
uv run python tools/compare_overpass.py --db-dir /path/to/overpass-db --osm3s-query /path/to/osm3s_query
```

For history queries, load the reference with `--keep-attic`, replay the lake's diffs over a fixed window, and compare with `--history`. The window is set with `updater.py job --catalog-url data/catalog.ducklake --until T` (one run that stops before the first diff ending after T), and the reference is synced with `tools/sync_reference.py --keep-attic`.
