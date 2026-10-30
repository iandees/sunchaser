# sunchaser architecture (as built)

`docs/design.md` is the original design exploration. This document describes what is implemented
now and why.

## Components

```
            OSMF replication (minute/hour diffs)
                          │
            ┌─────────────▼──────────────┐  the only writer: a scheduled job (or a long-running
            │  src/updater.py job        │  process); diffs, incremental areas, compaction
            └─────────────┬──────────────┘
   GET catalog, PUT Parquet, conditional PUT catalog (If-Match)
   ┌──────────────────────▼──────────────────────────────────┐
   │ Object storage (S3 / R2), nothing else                  │
   │  s3://…/lake/catalog.ducklake   DuckLake catalog (DuckDB file)
   │  s3://…/lake/data/…             Parquet                 │
   └──────────────────────┬──────────────────────────────────┘
                          │ read-only; fetch a local copy when the catalog's ETag changes
                ┌─────────▼──────────┐
                │  src/server.py     │  /api/interpreter  ← Overpass Turbo, scripts
                │  ql → engine → out │  any number, stateless, can scale to zero
                └────────────────────┘
```

| Component | Role |
|---|---|
| **Updater** | The only writer, like Overpass's `update_database`. Each replication diff is one DuckLake transaction. Either `updater.py job` on a schedule, e.g. every minute (take the writer lease, download the catalog, apply what's pending, update areas incrementally, compact when due, upload with a conditional PUT), or `job --loop` on an always-on host (holds and renews the lease, keeps the catalog on local disk, uploads it at most every `--upload-every` seconds, default 120). `run` is `job --loop` on a local catalog. Compaction runs inside the job (default every 3600 s), one step per cycle. |
| **Catalog** | A DuckDB-format DuckLake file stored in the bucket (`src/objcatalog.py`), so no database server is needed. The conditional PUT (If-Match on the downloaded ETag) means overlapping runs can't fork it: the later one is refused and discarded, and the Parquet files it wrote are orphans that compaction deletes after a grace period. A catalog is a `.ducklake` file or an `s3://` URL; SQLite and Postgres catalogs were removed. |
| **Lance id store** | Lance copies of `nodes_tagged`, `nodes_untagged` and `ways` (BTree on `id`) for id-driven fetches: recursion, id queries, global tag lookups (up to 2.2× faster than DuckLake key ranges). Spatial scans stay on DuckLake. The updater deletes and re-appends touched ids and records the Lance versions in `lake.state` in the same transaction; readers open Lance at those versions, so both stores always show the same diff. Compaction merges Lance fragments and updates the index. Cost: extra storage, extra requests and a longer build; see Measurements for the ratios. |
| **Server** | Read-only and stateless. Downloads the s3 catalog to a local file and attaches that, re-fetching when the object's ETag changes (`SUNCHASER_CATALOG_REFRESH` seconds, default 30). Attaching the object lazily over S3 failed mid-query with an ETag error whenever the updater uploaded a new catalog during the read; the local copy avoids that. `SUNCHASER_MAX_READ_MB` refuses a query before it scans when catalog file statistics estimate too many bytes. One DuckDB cursor per request; `[timeout:N]` interrupts the query; responses stream. |

## Data layout (one DuckLake schema)

The layout follows Overpass's own files, because the same access paths exist.

| Table | Sorted by | Overpass analogue | Used for |
|---|---|---|---|
| `nodes_tagged` / `nodes_untagged` (+ view `nodes`) | `qt` | `nodes.bin` + `node_tags_local` | spatial scans; tag queries read only the 12% tagged |
| `ways` | `qt_key31` | `ways.bin` (+ geometry) | spatial scans, `out geom` without node lookups |
| `relations` | `qt_key31` | `relations.bin` | member recursion, bbox prefilter (`min/max_lat/lon`) |
| `*_meta` | key | `*_meta.bin` | `out meta` only |
| `tag_postings` | `type, kv, qt_key31` | `*_tags_global` (+ frequent-tag split) | global and bbox+tag lookups |
| `elem_locator` | `type, id` | `*.map` | id → spatial key, then key-range fetch |
| `node_parent_ways`, `member_parent_rels` | member id | (Overpass scans) | `<`, `(bn)`, `(bw)`, `(br)`, update cascade |
| `areas` | id | `area_blocks` | `(area)`, `is_in`, `map_to_area` |
| `state` | – | `osm_base_version` | `osm_base`, replication sequence |

Keys:
- **`qt`** is Overpass's 32-bit quadtile index. `qt_key31` is its `Uint31_Index` ordering, where a compound-tile element is stored next to the tile it's anchored on.
- **`src/quadtile.py`** is an exact port, fuzz-tested against the C++ header. The build computes the same keys in SQL macros (or in Rust, `rust/osm-extract`).

## Query execution

`ql.py` parses Overpass QL into statements. The executor runs them over *sets*, DuckDB temp tables of `(type, id)`, exactly as Overpass statements consume and produce sets. Per element type, the engine picks an access path:

- **bbox only:**
  - Nodes: key ranges from `calc_ranges` plus lat/lon zone maps.
  - Ways and relations: `calc_parents` ranges plus bbox-overlap columns, then the exact segment test (`Bbox_Double::intersects`, with Overpass's coordinate rounding).
- **Tag only:** `tag_postings` (`kv = 'k=v'` or a `k=` prefix range), then a key-range plus id semi-join fetch.
- **bbox + selective tag:** postings restricted to the bbox's key ranges (Overpass's `query.cc` choice), otherwise the spatial scan.
- **ids and recursion:** `elem_locator` → key ranges → fetch. Id lists are loaded through Arrow and pruned with coalesced ranges, because long OR chains and huge literal lists are both CPU-bound in DuckDB.
- **Relations in a bbox:** filter candidates first, then test their members. Or, when candidate relations are huge, test the box's elements against their member lists.
- **Areas, `poly`, `around`:**
  - Areas and `poly` prefilter by bbox, then test with DuckDB spatial, using Overpass's border rules (nodes on the border count, ways merely touching it don't, relation members must be inside).
  - `around` uses great-circle distance to points and segments.

`output.py` renders `out` in Overpass's JSON, XML or CSV, with its field order and 7-decimal coordinates.

## Full history

`osm-extract --history` reads a full-history PBF (every version; deletions as `visible=false`) and writes the current tables, the `*_history` tables and the `changes` log, as if the updater had applied every change since the first version:
- Each version is valid from its timestamp to the next version's; versions sharing a second collapse to the last.
- A way gets a new row whenever a member node moves (`[date:]` needs its coordinates then).
- A relation gets a new row only when what it stores changes (members, tags, key, bbox); every member move is still a `geometry` entry in `changes`, as Overpass reports it.
- Node position and way geometry timelines are kept in RAM (extracts) or in id-indexed, memory-mapped files (`--dense-index`, the planet). With `--dense-index` the process's resident size is mostly the mapped index, which the kernel can evict; Germany peaked at 84-98 GB RSS of which only about 11 GB was anonymous memory.
- `--no-history` extracts a current-only file (an ordinary regional PBF); there is no separate snapshot mode.
- The PBF header's replication timestamp becomes `state.osm_base`, so the updater continues from there.

## Updates

The updater, per replication diff:
1. **Parse:** read the osmChange and keep the newest version per element.
2. **Regional filter** (only for extracts): keep existing elements, nodes in the region, ways using kept nodes, and relations with kept members (a fixpoint, so relation-of-relation works).
3. **Cascade:** moved nodes → ways using them (`node_parent_ways`) → relations containing either (`member_parent_rels`). As in Overpass, only real geometry changes cascade: a moved or deleted node, or a way whose coordinates changed; tags-only edits don't. Cascaded rows are logged as `geometry` changes, which `(changed)` counts, as Overpass does. Coordinates, bboxes and Overpass keys are recomputed with the build's macros.
4. **Rewrite:** delete and re-insert every affected row in all tables. Deletes are pruned by the rows' old keys. Then advance `state`, all in one transaction.

Catch-up uses hourly diffs (a planet day diff is about 100 MB, too much for one batch), then minutely.

**Maintenance** runs between batches:
- **`build_areas.py`:** Overpass's area rules, incrementally for the relations the last batches changed (`--full` rebuilds). Areas lag the data, as in Overpass.
- **`compact`:** flush inlined rows to sorted Parquet, merge small files, compact Lance, expire snapshots, and delete files unreferenced for longer than the grace period.

## Validation

The oracle is a real Overpass: `tools/compare_overpass.py` runs `queries/ql_corpus.py` through a reference `osm3s_query` and through this engine, and compares elements. Unit tests: `tests/test_quadtile.py` (fuzz against the C++ header), `tests/test_evaluator.py`, `tests/test_output_json.py`, `tests/test_readbudget.py`, and `tests/test_ql_corpus.py` (every corpus request parses). The earlier brute-force engine evaluator and `tests/test_corpus.py` were deleted: their oracle was this project's own reading of the semantics.

| What | Result |
|---|---|
| Index math (`tests/test_quadtile.py`) | bit-identical to Overpass's C++ |
| First differential run (58 requests, `results/overpass_diff.json`) | 56/58 recorded in that file; the exceptions are Overpass's `out bb` for relations with members missing from an extract, and `buildings_city` |
| After 5 days of replayed diffs | 66/69 non-area and 9/10 area-related queries identical; the differences come from the reference's state |
| Fixed 24-hour window (both sides given the lake's edits) | 68/69 current-data queries; 13/13 history queries (`[date:]` incl. `out meta`/`geom`, `newer`, `changed`) vs Overpass with `--keep-attic` |
| Object-storage catalog, updated only by scheduled jobs (24 h window) | 81/82 queries incl. 13/13 history; two overlapping jobs: one commits, the other is refused (412) and discarded |
| Lance id store kept by jobs through 24 diffs and a compaction | pinned versions equal the lake tables row for row; 81/82 queries incl. 13/13 history with id fetches on Lance; a second concurrent job exits on the lease |
| Full-history build (`osm-extract --history`) from a synthetic Berlin history file (snapshot + 24 h of the lake's diffs) | 81/82 queries incl. 13/13 history vs Overpass (`results/overpass_diff_history_build.json`); identical to the incrementally updated lake on 32 `[date:]` queries at 8 times; RAM and disk timelines byte-identical |
| Bucketed vs flat build | identical tables, files in key order (checked when the flat extractor still existed) |
| Overpass Turbo | loaded and rendered results from the service (`results/turbo.png`; the checking script was removed) |

The one exception in the 81/82 rows is Overpass's `out bb` placeholder for relation members missing from an extract.

## Known gaps

- **Not implemented:**
  - `[diff]`, `[adiff]`, `timeline`, `retro`.
  - `make`, `convert`, `for`, `complete`, `compare` and `if` blocks.
  - Antimeridian-crossing bboxes.
  - Ways as areas: this Overpass version's rules build areas from relations only.
- **Performance:**
  - Single queries take 0.3–3 s on a 4-core box, against 0.1–2.4 s for local-disk Overpass.
  - Large outputs: JSON nodes and ways are serialised in SQL (about 32 MB/s); other formats and modes in Python (about 10 MB/s).
- **Planet scale:** not built yet; sizes come from scaling regional lakes and times were rehearsed on Germany. See `docs/runbook.md`, including the open risk that the planet extract's dense index does not fit in RAM.

## Measurements

Durable findings from the October 2026 Berlin spike (`results/NOTES.md` is the record). Conditions: BBBike Berlin extract (snapshot 2026-10-24, 12.9 M nodes, 2.27 M ways, 30 K relations), 4 vCPU and 15 GB RAM container, DuckDB 2.0.0 with DuckLake 1.0, reads through a local S3 stand-in that injected 30 ms per request (the stand-in has since been removed). These are not R2 numbers.

### Query cost, 24-query corpus

Scenarios: cold (new process), worker (long-lived process with catalog and footers cached, this query's data unread), hot (all cached).

| | cold median / p90 / max | worker median / p90 / max | hot median / p90 |
|---|---|---|---|
| 0 ms/request | 0.80 / 1.60 / 3.99 s | 0.74 / 1.71 / 4.00 s | 0.36 / 0.80 s |
| 30 ms/request | 1.92 / 4.30 / 7.68 s | 1.20 / 3.61 / 7.45 s | 0.35 / 0.84 s |

Worker I/O is a median of about 92 requests and 17 MiB per query. The slowest are big recursions (`park_multipolygons_downrel`, `subway_routes_down`), at about 1,000-1,150 requests.

### Optimisations that mattered

| Change | Effect |
|---|---|
| Literal `IN (...)` id lists instead of `IN (subquery)` / join | 779 to 73 requests, 20 to 2 MiB. DuckDB turns a constant list into zone-map pruning; a join only pushes down min/max. |
| Huge literal lists to at most 32 coalesced `BETWEEN` ranges + hash semi-join | 2.76 s to 0.28 s CPU for 9 K ids, at about 1.8× the bytes. DuckDB evaluates OR chains per row. |
| `tag_postings.kv = 'key=value'` single column | 266 to 15 requests per tag lookup. Separate `k AND v` predicates made DuckDB probe `v` in every row group. |
| Split tagged and untagged nodes, metadata in `*_meta` (Overpass's own layout) | `node[amenity=cafe](bbox)`: 19 to 2.2 MiB. `node[name~...,i](bbox)`: 190 to 13 MiB. |
| bbox+tag planner: drive from `tag_postings` restricted to the bbox's key ranges when at most 20 K candidates (Overpass's `query.cc` choice) | `nwr[shop=bakery](bbox)`: 66 to 7 MiB. Global tag queries: 92 to 30 MiB. |
| Locator memo per worker (Overpass keeps `.map` hot in page cache) | About 2× on repeated recursion. |

### Things that surprised me

- **DuckDB's Parquet prefetcher coalesces reads across skipped row groups.** For one bbox it read 14 MiB in 29 requests; with prefetch off it read the exact 2.2 MiB in 159 requests. Row-group pruning does not equal bytes read. The way to cut I/O is narrower tables, not finer row groups.
- **Disabling DuckDB's external file cache multiplies bytes about 3×**, because each column pass re-fetches the same span. Keep it on in workers.
- **`qt` is almost free.** The column is 68 KiB for 12.9 M nodes (sorted, RLE); lat and lon are the two largest columns at about 15 MB each.
- **The spatial sort alone makes lat/lon zone maps prune** about as well as Overpass's `calc_ranges`. The `qt` ranges matter more for ways and relations (`calc_parents`) and for the tag and locator paths.
- **Index overhead:** `tag_postings` 71 MB, `elem_locator` 29 MB, `node_parent_ways` + `member_parent_rels` 42 MB, of a 490 MB lake: about 29%.

### Lance vs Parquet for id lookups

Cold cache, 30 ms per request; fetching `(id, lat, lon)` for an id list:

| ids | DuckLake locator to nodes | Lance, BTree on id |
|---|---|---|
| 5 | 0.42 s, 17 req, 1.7 MiB | 0.30 s, 43 req, 0.6 MiB |
| 208 (a street) | 1.28 s, 166 req, 6.8 MiB | 0.51 s, 197 req, 4.6 MiB |
| 8,957 (subway routes) | 3.68 s, 841 req, 41 MiB | 1.02 s, 601 req, 53 MiB |

Lance wins id lookups by 1.4-3.6×, from higher request concurrency and a single hop. On bbox scans it loses (Mitte, 67 K nodes: 0.35 s against 0.27 s for Parquet with footers warm; up to 4.5× slower in the corpus, with 5-10× more requests), so spatial scans stay on DuckLake and Lance serves only id-driven fetches. Lance files are about 2× the size of zstd Parquet at default encodings (nodes: 306 MB against about 140 MB).

### Lance in the live path

Berlin, local S3 stand-in at 30 ms per request (the real-R2 figure follows the table). Engine as deployed (Lance for id-driven fetches, including the `out` element fetch; `out meta` still reads DuckLake). Lake updated by scheduled jobs through 24 diffs plus a compaction. The 69 current-data queries of the differential corpus, alternating Lance and no Lance twice, fresh process each, best of 2.

| | No Lance | Lance |
|---|---|---|
| Sum of 69 queries | 148.2 s | 110.3 s |
| Median query | 1.44 s | 0.81 s |
| Geometric-mean speedup | 1.00× | 1.54× |
| Faster by more than 1.2× / slower by more than 1.2× | – | 51 / 1 (`out meta` on a bbox) |
| Object-store requests per run | about 6,170 | about 12,720 (2×) |
| Bytes read per run | 0.53 GB | 1.24 GB |

Wins reach 3.5× (`regex_value_icase` 0.87 to 0.25 s). On R2 the extra requests cost about $0.03 per thousand queries at $0.36 per million reads. Lance writing took 17 s on top of a 35 s build (+50%).

**On real R2** (Minnesota lake, Hetzner box in Helsinki, 2026-10-30): 22 Minneapolis queries of the same classes, two API processes on the same catalog snapshot (one with `SUNCHASER_LANCE=off`), three rounds alternating which went first, the live updater running throughout. Outputs identical on every query.

| | No Lance | Lance |
|---|---|---|
| Sum of 22 queries, warm (best of rounds 2-3) | 191.7 s | 137.7 s |
| Median query, warm | 5.48 s | 4.37 s |
| Geometric-mean speedup, warm / cold (round 1) | 1.00× | 1.32× / 1.10× |
| Faster by more than 1.2× / slower by more than 1.2× | – | 12 / 1 (`cafes_core`: bbox + `out meta`, 0.74×) |

The wins are where the Berlin spike put them: global tag queries 1.7-2.3×, `named_sets` 2.2×, `around` 2.1×, a 184 k-way id fetch 1.9×, Turbo-style queries about 1.45×. Plain bbox scans with `out meta` are even or slower. Query times on R2 are also several times longer than the stand-in's: the stand-in's 30 ms per request understated real latency from this box, so the absolute numbers in the table above do not transfer.

**Lance storage ratio.** The ratio depends on what it is divided by:

| Lance size over | Ratio | Source |
|---|---|---|
| the Berlin current-data lake's Parquet (645 MB over 512 MB) | 1.25× | measured |
| the current-data tables it copies (nodes and ways) | about 2× | measured on nodes (306 vs 140 MB); planet estimate 490-550 GB over 240-265 GB |
| the planet's full-history Parquet lake (610-620 GB) | 0.85× | sizing-run estimate |

Planning figure for the full-history planet: Lance adds 490-550 GB next to 610-620 GB of Parquet. Germany's measured lake was 57 GB, of which 20 GB was Lance (37 GB Parquet), a ratio of 0.54× the Parquet; the sizing-run estimate has not been reconciled with that.
