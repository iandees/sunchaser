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
- [`docs/architecture.md`](docs/architecture.md): how it works.
- [`docs/hetzner-r2.md`](docs/hetzner-r2.md): runbook for a planet build on Cloudflare R2 with a Hetzner box or a laptop.
- [`docs/aws.md`](docs/aws.md): the same on AWS, scaling to zero.
- [`docs/planet.md`](docs/planet.md): planet sizing.
- [`HANDOFF.md`](HANDOFF.md): step-by-step for running it on the box.
- [`results/NOTES.md`](results/NOTES.md): the measurements behind the design.
- [`docs/design.md`](docs/design.md): the original design exploration.

## Status

| | |
|---|---|
| Correctness vs. a real Overpass 0.7.62 (Berlin) | 81/82 queries element-identical over a fixed 24 h window, including 13/13 history queries against an Overpass with attic data. The one difference is Overpass's `out bb` placeholder for relation members missing from an extract. The full-history build gives the same answers. Details: `results/overpass_diff*.json` |
| Overpass Turbo | The real overpass-turbo.eu loads and renders results from the service (`tools/turbo_check.js`, `results/turbo.png`) |
| Updates | OSMF replication (hourly catch-up, then minutely); node → way → relation cascade; incremental areas; Lance kept in step |
| Speed (Berlin, 30 ms per storage request) | Median query 0.81 s with Lance, 1.44 s without; large JSON outputs about 32 MB/s |
| QL coverage | Settings `[out:json\|xml\|csv]`, `[timeout]`, `[maxsize]`, `[bbox]`, `[date]`; `(newer)`, `(changed)`; `node/way/rel/nwr/nw/nr/wr/area`; tag filters incl. regex and `,i`; `(bbox)`, `(id:…)`, `(.set)`, `(w)`, `(r[:role])`, `(bn)`, `(bw)`, `(br)`, `(around:…)`, `(around.set:…)`, `(poly:…)`, `(area[.x])`, `(pivot[.x])`, `(if: …)` evaluators; unions, differences, named sets, `> >> < <<`, `foreach`, `map_to_area`, `is_in`; `out ids/skel/body/tags/meta/count` × `geom[(bbox)]/bb/center` × `asc/qt`, limit, `noids` |
| Planet | Tools and runbooks are ready, and sizing comes from cuts of the real history planet. The planet build itself hasn't run yet |
| Not yet | `[diff]`, `[adiff]`, `timeline`, `retro`; `make`/`convert`/`for`/`complete`/`compare`/`if` blocks; antimeridian-crossing boxes |

## Layout

| Path | What |
|---|---|
| `src/server.py` | HTTP API: `/api/interpreter` (GET/POST `data=`), `/api/status`, `/api/timestamp`; CORS for Turbo |
| `src/ql.py` | Overpass QL parser and executor |
| `src/output.py` | `out` statement and the JSON/XML/CSV writers |
| `src/engine.py` | Overpass statement semantics over DuckDB; DuckLake or Lance element access |
| `src/updater.py` | The only writer: `job` (scheduled run against the catalog in S3/R2), `job --loop` (always-on), or `run` (local catalog); incremental areas, compaction |
| `src/objcatalog.py` | The DuckLake catalog as an object in S3/R2: conditional upload, writer lease (no database server) |
| `src/lancestore.py` | Lance id store for id-driven fetches, kept in step with the lake and pinned per catalog |
| `src/publish.py` | Frozen copy of a SQLite/Postgres catalog for remote readers (only for those catalogs; the object-storage catalog needs none) |
| `src/build_areas.py` | Overpass's area rules: `lake.areas`, incrementally from the changes log (`--full` to rebuild) |
| `src/evaluator.py` | `(if: …)` evaluator expressions |
| `src/build_lake.py` | Raw Parquet → DuckLake tables with Overpass keys (bucketed input: sorted one key-range group at a time) |
| `src/extract.py`, `rust/osm-extract` | PBF → raw Parquet. Python is the flat reference; Rust computes all keys and bboxes, writes key-range buckets, has a dense mmap index for the planet, and builds full history from a history PBF (`--history`) |
| `src/quadtile.py` | Exact port of Overpass's index math (fuzz-tested against the C++ header) |
| `src/lakeconn.py` | Catalog and S3/R2 connection setup shared by all tools |
| `tools/compare_overpass.py` | Differential test against `osm3s_query` (`--history` for attic queries) |
| `tools/sync_reference.py` | Replays the lake's diffs into the reference Overpass |
| `tools/fetch_parallel.py` | Ordered multi-connection download to stdout (streams the planet into the extractor) |
| `tools/measure.py`, `tools/size_planet.py` | Time / peak memory / peak disk of a command; planet estimate from regional cuts |
| `tools/range_server.py` | S3-compatible test store (conditional PUTs, ETags) with latency injection and I/O accounting |
| `tools/relocate_lake.py` | Points a lake built on local disk at its new location after copying it to R2/S3 |
| `tools/make_history_extract.py` | Synthesises a history file (snapshot + applied diffs) to test the full-history build |
| `queries/` | Engine corpus and full-QL differential corpus |
| `deploy/hetzner/` | Box setup, sizing and planet build scripts, systemd units, env template |
| `deploy/local.env` | Settings for the local S3 stand-in (`tools/range_server.py`) |

## Running it (an extract, locally)

For a planet deployment, follow [`docs/hetzner-r2.md`](docs/hetzner-r2.md) or [`docs/aws.md`](docs/aws.md).

1. **Dependencies** are managed with [uv](https://docs.astral.sh/uv/) (`pyproject.toml`, `uv.lock`); the extractor needs `cargo`:

   ```bash
   uv sync
   ```

   ```bash
   cargo build --release --manifest-path rust/osm-extract/Cargo.toml
   ```

2. **Extract** an extract's PBF, or `--history` for a history file:

   ```bash
   rust/osm-extract/target/release/osm-extract region.osm.pbf data/raw
   ```

3. **Build the lake**, here with a catalog file and data on local disk. `--data-path s3://…` writes to a bucket instead (`SUNCHASER_S3_*` settings). `osm_base` comes from the PBF header, or pass `--osm-base`:

   ```bash
   uv run python src/build_lake.py --raw data/raw --lake data/build --catalog data/catalog.ducklake --data-path data/lake/
   ```

   ```bash
   SUNCHASER_CATALOG=data/catalog.ducklake uv run python src/build_areas.py --full
   ```

4. **Follow replication.** `--region` keeps an extract consistent; omit it for the planet:

   ```bash
   SUNCHASER_CATALOG=data/catalog.ducklake uv run python src/updater.py run --region 52.23,12.76,52.82,13.98
   ```

5. **Serve.** Point Overpass Turbo's server setting at `http://localhost:8080/api/`:

   ```bash
   SUNCHASER_CATALOG=data/catalog.ducklake SUNCHASER_PORT=8080 uv run python src/server.py
   ```

With the catalog in a bucket instead, the writer is `updater.py job --catalog-url s3://…/catalog.ducklake` (add `--loop` on an always-on host), and servers use `SUNCHASER_CATALOG=s3://…/catalog.ducklake`.

## Differential testing

Build Overpass from source, load the same extract (`update_database --meta
--map-compression-method=gz`; without map compression the id-indexed `.map` files are tens of GB even
for a city), then:

```bash
uv run python tools/compare_overpass.py --db-dir /path/to/overpass-db
```

For history queries, load the reference with `--keep-attic`, replay the lake's diffs over a fixed window, and compare with `--history`. The window is set with `updater.py run --once --until T`, and the reference is synced with `tools/sync_reference.py --keep-attic`.
