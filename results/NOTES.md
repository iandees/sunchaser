# Single-region spike: results

*Region: BBBike "Berlin" extract, snapshot 2026-10-24T00:00Z: 12.9 M nodes, 2.27 M ways, 30 K relations, 183 MB PBF.
Machine: 4 vCPU and 15 GB RAM, in a container. Engine: DuckDB 2.0.0 + DuckLake 1.0, pylance 14.0.1.*

## What was built

1. **`src/quadtile.py`**: an exact Python port of Overpass's spatial index math (`index_computations.h`). It's checked against the original C++ header by a differential fuzz test (`tests/test_quadtile.py`, which uses `tools/qt_oracle`, a 40-line harness around the header):
   - node index: 6000/6000
   - way index: 3000/3000
   - relation index: 2000/2000
   - `calc_ranges`: 402/402
   - `calc_parents`: 150/150

   One subtlety the port had to reproduce: Overpass orders way and relation keys by `(idx & 0x7fffffff, idx)`. A compound-tile way is stored next to the plain tile it's anchored on. The lake stores this ordering as `qt_key31`.
2. **`src/extract.py`**: PBF → raw Parquet. Takes 5 min (pyosmium, single-threaded Python).
3. **`src/build_lake.py`**: raw Parquet → DuckLake in about 70 s.
   - Node and way spatial keys are computed in pure SQL macros, verified against the port on 200 K nodes and 30 K ways, so the build scales to the planet in DuckDB.
   - Tables:

     | Table | Sorted by | Notes |
     |---|---|---|
     | `nodes_tagged` | `qt` | 12% of nodes |
     | `nodes_untagged` | `qt` | the rest; `nodes` is a view over both |
     | `ways` | `qt_key31` | includes geometry |
     | `relations` | `qt_key31` | includes bbox |
     | `*_meta` | key | version, ts, changeset, uid, user |
     | `tag_postings` | `(type, kv, qt_key31)` | |
     | `elem_locator` | `(type, id)` | id → tile key |
     | `node_parent_ways` | node id | |
     | `member_parent_rels` | `(type, member id)` | |
4. **`src/engine.py`**: Overpass statement semantics over DuckDB set tables.
   - Covers `query` with bbox, tag, and id filters; `>`, `>>`, `<`, `<<`; union; and difference.
   - bbox, recursion, and tag semantics follow the C++ source: `bbox_filter.cc`, `geometry.cc` (segment-intersection test for ways), and `recurse.cc` (which inputs each recursion keeps).
   - Two backends: `lake`, which uses the indexes, and `raw`, which brute-forces over unkeyed Parquet.
5. **`queries/corpus.py`**: 24 queries, each kept as Overpass QL text plus the equivalent engine program.
6. **`tools/range_server.py`**: an object-store stand-in. It provides HTTP Range, a minimal S3 `ListObjectsV2`, injected per-request latency, and request and byte accounting.

## Correctness

**24/24 queries are element-level identical**, compared as `(type, id)` sets, between the indexed lake plans and the brute-force evaluator. This holds in every cache scenario and at both latency settings (`corpus_lat0.json`, `corpus_lat30.json`).

What this does **not** yet show is agreement with a real Overpass. The brute-force side reimplements Overpass semantics from source, so it validates the *indexing* (nothing lost or invented by pruning), not my reading of the semantics. Closing that needs a reference Overpass (see "Blocked" below). Each corpus entry keeps its QL text so it can be replayed with `[date:"2026-10-24T00:00:00Z"]`.

## Performance (per query, 24-query corpus)

The scenarios below are served through the range server:
- **cold**: a new process.
- **worker**: a long-lived process with catalog and Parquet footers cached, but this query's data never read before.
- **hot**: everything cached.

| | cold median / p90 / max | worker median / p90 / max | hot median / p90 |
|---|---|---|---|
| 0 ms/request | 0.80 / 1.60 / 3.99 s | 0.74 / 1.71 / 4.00 s | 0.36 / 0.80 s |
| 30 ms/request (S3-like) | 1.92 / 4.30 / 7.68 s | 1.20 / 3.61 / 7.45 s | 0.35 / 0.84 s |

- **Worker I/O**: a median of about 92 requests and 17 MiB per query.
- **Slowest queries**: big recursions such as `park_multipolygons_downrel` and `subway_routes_down`, which make about 1,000–1,150 requests.
- **Bbox and tag queries**: under a second when hot. The cold/worker time depends on the request count.

### Optimisations that mattered
Each figure is measured on the target step (`tools/profile_query.py`):

| Change | Effect |
|---|---|
| Literal `IN (…)` id lists instead of `IN (subquery)` / join | 779 → 73 requests, 20 → 2 MiB. DuckDB turns a constant list into zone-map pruning, but a join only pushes down min/max. |
| Huge literal lists → **≤32 coalesced `BETWEEN` ranges + hash semi-join** | 2.76 s → 0.28 s CPU for 9 K ids, at about 1.8× the bytes. DuckDB evaluates OR chains per row. |
| `tag_postings.kv = 'key=value'` single column | 266 → 15 requests per tag lookup. Separate `k AND v` predicates made DuckDB probe `v` in every row group. |
| Split tagged and untagged nodes, and move metadata to `*_meta` (Overpass's own layout) | `node[amenity=cafe](bbox)`: 19 → 2.2 MiB. `node[name~…,i](bbox)`: 190 → 13 MiB. |
| bbox+tag planner: drive from `tag_postings` restricted to the bbox's key ranges when ≤20 K candidates (Overpass's `query.cc` choice) | `nwr[shop=bakery](bbox)`: 66 → 7 MiB. Global tag queries: 92 → 30 MiB. |
| Locator memo per worker (Overpass keeps `.map` hot in page cache) | About 2× on repeated recursion. |

### Things that surprised me
- **DuckDB's Parquet prefetcher coalesces reads across skipped row groups.** For one bbox it read 14 MiB in 29 requests. With prefetch disabled it read the exact 2.2 MiB, but in 159 requests. At S3 latencies the default is roughly right, but it means *row-group pruning ≠ bytes read*. The way to cut I/O is narrower tables, not finer row groups.
- **Disabling DuckDB's external file cache multiplies bytes about 3×**, because each column pass re-fetches the same span. Keep it on in workers.
- **`qt` is almost free.** The column is 68 KiB for 12.9 M nodes (sorted, RLE-encoded), whereas lat and lon are the two largest columns at about 15 MB each. Pruning on `qt` costs almost nothing to read.
- **The spatial sort alone makes lat/lon zone maps prune** about as well as Overpass's `calc_ranges`. The `qt` ranges matter more for ways and relations (`calc_parents`) and for the tag/locator paths.

## Lance vs Parquet for id lookups (`bench_idlookup.py`, `idlookup_lat*.json`)

The test fetches `(id, lat, lon)` for an id list, with the cache cold, at 30 ms per request:

| ids | DuckLake locator→nodes | Lance `nodes_by_id` (BTree) | Lance `nodes_by_qt` (BTree on id) |
|---|---|---|---|
| 5 | 0.42 s · 17 req · 1.7 MiB | 0.31 s · 47 req · 0.5 MiB | 0.30 s · 43 req · 0.6 MiB |
| 208 (a street) | 1.28 s · 166 req · 6.8 MiB | 0.65 s · 391 req · 6.9 MiB | **0.51 s** · 197 req · 4.6 MiB |
| 8,957 (subway routes) | 3.68 s · 841 req · 41 MiB | 1.18 s · 996 req · 65 MiB | **1.02 s** · 601 req · 53 MiB |

Bbox scan on the same tile-sorted Lance table, cold Lance against Parquet with footers warm, at 30 ms:

| Area | Lance | Parquet |
|---|---|---|
| tiny | 0.30 s | 0.18 s |
| Mitte (67 K nodes) | 0.35 s | 0.27 s |

Reading:
- **Lance wins id lookups by 1.4–3.6×**, mainly through much higher request concurrency and a single hop.
- **Lance stays competitive on spatial scans**, so one tile-sorted Lance table with a BTree on `id` could replace both `nodes` and `elem_locator`.
- **Costs**:
  - Lance files are about 2× larger than zstd Parquet with default encodings (306 MB against about 140 MB for nodes).
  - The indexes need maintenance after appends.
  - DuckDB's `lance` extension didn't take part. The benchmark uses the Lance Python API directly, as the design doc anticipated.

## Follow-up: Rust extractor, ohsome-planet, Lance element store

### Rust extractor (`rust/osm-extract`)
- **What it is:** about 450 lines (osmpbf + arrow-rs + rayon). PBF blocks are decoded in parallel in chunks. Node blocks are built first and fill an id-sorted location index, then way and relation blocks are built in parallel against it. The Overpass keys (`qt` for nodes, ways and relations, including compound levels) are computed in the same pass, in `src/quadtile.rs`.
- **Speed:** Berlin takes **12.1–12.9 s against 301.6 s** for `src/extract.py`, about **23–25×** faster on 4 threads. That includes the keys, which the Python path computes later in `build_lake.py`.
- **Correctness:**
  - Output is identical to the Python extractor, compared with `EXCEPT ALL` on every column of all 12.9 M nodes, 2.27 M ways and 30 K relations.
  - All 15.2 M `qt` values match the lake build, which the fuzz test verified against the C++ header.
- **Planet notes (not measured):**
  - Linear scaling gives roughly 1.7 h on 4 cores for the 95 GB planet PBF.
  - The in-memory location index (16 B per node) has to become a dense mmap array keyed by id: about 8 B × 13 G ids ≈ 105 GB on disk.

### ohsome-planet: read, not run
The build was blocked by the session's auto-mode policy, so this is a source review only (commit `dd4431c`).

Strengths:
- Full-history Parquet with major and *minor* versions, including the cascade into relations from way node moves.
- XZ-order spatial key.
- RocksDB replication with node→way, node→relation and way→relation back-references.

**Not a drop-in for an Overpass backend:**
- **Untagged elements are excluded from the Parquet output** (`hasNoTags` in `TransformerNodes.java`/`TransformerWays.java`, confirmed). Overpass recursion and per-ref coordinates need untagged way vertices.
- Way geometry silently drops unresolved nodes, so vertices aren't reliably 1:1 with `refs`.
- Non-multipolygon relations are stored only as bboxes.
- Replication emits new versions only, with **no closing rows** (`valid_to` of the prior version is never updated) and no changeset, uid or user in its stored state.
- It is single-node with local RocksDB, has no atomic commit or catalog, and is **GPL-3.0**.

Best use: a design reference for minor-version derivation and the replication cascade, or a source of tagged-feature history. Not the primary ingest.

### Lance as the element store (`corpus_lance_lat30.json`, `corpus_hybrid_lat30.json`)
- **Setup:** Lance tables mirror the lake layout (`nodes_tagged`, `nodes_untagged`, `ways`), tile-sorted, with BTree indexes on `id` and on `qt` / `qt_key31`. Relations, postings and reverse indexes stay in DuckLake.
- **Engine change:** every element scan now separates *pushable* predicates (ranges, ids, bbox; pruned by the storage layer) from *local* ones (tags, exact geometry; always in DuckDB). The same plan runs on either backend.
- **Correctness:** 24/24 queries are element-identical on all three backends.

Results at 30 ms per request, worker scenario:

| | median | p90 | max | sum | geomean vs DuckLake |
|---|---|---|---|---|---|
| DuckLake only | 1.21 s | 3.60 s | 7.10 s | 42.4 s | 1.00× |
| Lance for all node/way scans | 1.48 s | 2.36 s | 7.20 s | 41.7 s | 0.88× |
| **Hybrid**: Lance for id-driven fetches (recursion, id queries, global tag postings); DuckLake for spatial scans and bbox+tag | **1.09 s** | **2.24 s** | **4.94 s** | **32.6 s** | **1.18×** |

- **Lance wins id-driven work by up to 2.2×:** `subway_routes_down` 5.29 → 2.48 s, `route_master_downrel` 3.92 → 2.07 s, global tag queries about 1.7×.
- **Lance loses bbox scans by up to 4.5×:** `name_regex_icase` 0.30 → 1.38 s. It makes 5–10× more requests than Parquet zone-map reads.
- **The hybrid keeps both wins.** It needs two copies of nodes and ways (Lance is about 2× Parquet size), so a production layout should pick one primary format per access path, or accept the duplication.

### Harness fixes made along the way
- The range server's listen backlog was 5. Concurrent S3 clients (Lance) saw refused connections and about 30 s retry stalls. It's raised to 1024, and **all 30 ms corpus numbers above were re-run on the fixed server.**
- Earlier, a Nagle/delayed-ACK stall added about 40 ms per request. It's fixed with `TCP_NODELAY`. Request and byte counts were unaffected.

## Index overhead
Of the 490 MB lake:
- `tag_postings` is 71 MB.
- `elem_locator` is 29 MB.
- `node_parent_ways` plus `member_parent_rels` is 42 MB.

Altogether about 29%, which is similar in spirit to Overpass's `*_tags_*` and `.map` files. Extrapolating linearly by PBF size (95.5 GB planet / 0.183 GB), the planet lake would be roughly 250 GB of current data. That is an extrapolation, not a measurement.

## Blocked / not done
- **Reference Overpass**:
  - Building Overpass from source in this container was blocked by the session's auto-mode policy (running external build scripts).
  - The public `overpass-api.de` is unreachable through the sandbox proxy, as is `download.geofabrik.de`. That's why the extract comes from BBBike.
  - Either route would close the semantic-validation gap, which is the next item.
- **Not covered**: areas, history/attic, `around`/`poly`, output formats (`out geom/meta`, JSON/XML), and the QL parser. The corpus calls the engine API directly.
- **Simulated network**: the range server mimics latency and counts I/O, but it isn't S3. Real S3 has different concurrency limits, throughput, and first-byte variance.
- **Single-region scale**: Berlin is about 0.2% of the planet. Planet-scale catalog size, footer caching, and the frequency of compound-level (`qt_level` ≥ 4) objects are untested.

## Suggested next steps (superseded in part by the follow-up above)
1. **Reference Overpass**: build Overpass locally, or allow egress to an Overpass instance. Replay the corpus with `[date:"2026-10-24T00:00:00Z"]` and diff element sets.
2. **Lance as the element store**: replace `nodes_*` and `elem_locator` with a tile-sorted Lance table (BTree on id), and keep DuckLake for `tag_postings`, meta, and history. Re-run the corpus.
3. **Sets carry payload**: carry element payload (nds, geometry) in the set tables so recursion stops re-fetching ways. This is the largest remaining source of requests.
4. **Wider coverage**: add `out geom` / JSON output and a QL parser front end. Then grow the corpus to about 200 queries drawn from real overpass-turbo traffic patterns.
