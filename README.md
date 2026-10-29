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

See [`docs/architecture.md`](docs/architecture.md) for how it works, [`docs/aws.md`](docs/aws.md) for a planet
build with full history on AWS that scales to zero, [`docs/design.md`](docs/design.md)
for the original design exploration, and [`results/NOTES.md`](results/NOTES.md) for the measurements
that shaped it.

## Status

| | |
|---|---|
| Differential test vs. a real Overpass (Berlin extract) | See `results/overpass_diff*.json`. At the extract snapshot: 57/58 element-identical. After 5 days of replayed diffs: 66/69 non-area and 9/10 area-related queries identical; the rest come from the reference's state (planet-wide diffs, missing extract members), not from the engine. Fixed 24-hour window with both sides given the same edits: 68/69, plus 13/13 history queries (`[date:]`, `newer`, `changed`) against an attic Overpass |
| Overpass Turbo | The real overpass-turbo.eu loads and renders results from the service (`tools/turbo_check.js`, `results/turbo.png`) |
| Live updates | The updater follows OSMF replication (hourly catch-up, then minutely) with the node → way → relation cascade. Readers stay consistent while it commits |
| QL coverage | Settings `[out:json\|xml\|csv]`, `[timeout]`, `[maxsize]`, `[bbox]`, `[date]`; `(newer)`, `(changed)`; `node/way/rel/nwr/nw/nr/wr/area`; tag filters incl. regex and `,i`; `(bbox)`, `(id:…)`, `(.set)`, `(w)`, `(r[:role])`, `(bn)`, `(bw)`, `(br)`, `(around:…)`, `(around.set:…)`, `(poly:…)`, `(area[.x])`, `(pivot[.x])`, `(if: …)` evaluators; unions, differences, named sets, `> >> < <<`, `foreach`, `map_to_area`, `is_in`; `out ids/skel/body/tags/meta/count` × `geom[(bbox)]/bb/center` × `asc/qt`, limit, `noids` |
| Planet scale | Streaming responses; bucketed Rust extract and bounded-memory lake build; plan and sizing in [`docs/planet.md`](docs/planet.md) |
| Not yet | `[diff]`, `[adiff]`, `timeline`, `retro`; `make`/`convert`/`for`/`complete`/`compare`/`if` blocks; antimeridian-crossing boxes; incremental areas |

## Layout

| Path | What |
|---|---|
| `src/server.py` | HTTP API: `/api/interpreter` (GET/POST `data=`), `/api/status`, `/api/timestamp`; CORS for Turbo |
| `src/ql.py` | Overpass QL parser and executor |
| `src/output.py` | `out` statement and the JSON/XML/CSV writers |
| `src/engine.py` | Overpass statement semantics over DuckDB; DuckLake or Lance element access |
| `src/updater.py` | Replication follower: `job` (scheduled run against a catalog on S3) or `run` (long-running); incremental areas, `compact` |
| `src/objcatalog.py` | The DuckLake catalog as an object in S3/R2: conditional upload, writer lease (no database server) |
| `src/lancestore.py` | Lance id store for id-driven fetches, kept in step with the lake and pinned per catalog |
| `src/publish.py` | Frozen DuckLake publishing: a read-only catalog file on S3/R2 for stateless readers |
| `src/build_areas.py` | Overpass's area rules: `lake.areas`, incrementally from the changes log (`--full` to rebuild) |
| `src/evaluator.py` | `(if: …)` evaluator expressions |
| `src/build_lake.py` | Raw Parquet → DuckLake tables with Overpass keys (bucketed input: sorted one key-range group at a time) |
| `src/extract.py`, `rust/osm-extract` | PBF → raw Parquet. Python is the flat reference; Rust computes all keys and bboxes, writes key-range buckets, has a dense mmap index for the planet, and builds full history from a history PBF (`--history`) |
| `src/quadtile.py` | Exact port of Overpass's index math (fuzz-tested against the C++ header) |
| `src/lakeconn.py` | Catalog and S3/R2 connection setup shared by all tools |
| `tools/compare_overpass.py` | Differential test against `osm3s_query` |
| `tools/range_server.py` | S3-compatible test store (conditional PUTs, ETags) with latency injection and I/O accounting |
| `tools/make_history_extract.py` | Synthesises a history file (snapshot + applied diffs) to test the full-history build |
| `queries/` | Engine corpus and full-QL differential corpus |

## Running it

Python dependencies are managed with [uv](https://docs.astral.sh/uv/) (`pyproject.toml`, `uv.lock`):

```bash
uv sync
```

Extract and build. The Rust extractor is the fast path; it needs `cargo` and outputs the same Parquet:

```bash
cargo build --release --manifest-path rust/osm-extract/Cargo.toml
```

```bash
rust/osm-extract/target/release/osm-extract region.osm.pbf data/raw
```

For the planet, add `--bucket-bits 12 --dense-index /nvme/idx`. See [`docs/planet.md`](docs/planet.md).

Configure storage. `deploy/local.env` targets the local test store; for R2, set
`SUNCHASER_S3_ENDPOINT=<account>.r2.cloudflarestorage.com`, `SUNCHASER_S3_USE_SSL=true`,
`SUNCHASER_S3_REGION=auto` and an R2 token's key id and secret:

```bash
. deploy/local.env
```

```bash
uv run python src/build_lake.py --raw data/raw --lake /tmp/build --osm-base 2026-10-24T00:00:00Z
```

Follow replication (omit `--region` for a planet lake):

```bash
uv run python src/updater.py run --region 52.23,12.76,52.82,13.98
```

Serve. Point Overpass Turbo's server setting at `http://localhost:8080/api/`:

```bash
SUNCHASER_PORT=8080 uv run python src/server.py
```

Compact periodically, e.g. hourly. This flushes inlined rows to sorted Parquet, merges small files
and expires old snapshots:

```bash
uv run python src/updater.py compact
```

## Differential testing

Build Overpass from source, load the same extract (`update_database --meta
--map-compression-method=gz`; without map compression the id-indexed `.map` files are tens of GB even
for a city), then:

```bash
uv run python tools/compare_overpass.py --db-dir /path/to/overpass-db
```

For history queries, load the reference with `--keep-attic`, replay the lake's diffs over a fixed window, and compare with `--history`. The window is set with `updater.py run --once --until T`, and the reference is synced with `tools/sync_reference.py --keep-attic`.
