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
                          │ read-only; reopen when the catalog's ETag changes
                ┌─────────▼──────────┐
                │  src/server.py     │  /api/interpreter  ← Overpass Turbo, scripts
                │  ql → engine → out │  any number, stateless, can scale to zero
                └────────────────────┘
```

| Component | Role |
|---|---|
| **Updater** | The only writer, like Overpass's `update_database`. Each replication diff is one DuckLake transaction. Run as `updater.py job` on a schedule, e.g. every minute: take the writer lease, download the catalog, apply what's pending, update areas incrementally, compact when due, upload with a conditional PUT. |
| **Catalog** | A DuckDB-format DuckLake file stored in the bucket (`src/objcatalog.py`), so no database server is needed. The conditional PUT (If-Match on the downloaded ETag) means overlapping runs can't fork it: the later one is refused and discarded, and the Parquet files it wrote are orphans that compaction deletes after a grace period. SQLite (single host) and Postgres catalogs still work for always-on deployments. |
| **Lance id store** | Lance copies of `nodes_tagged`, `nodes_untagged` and `ways` (BTree on `id`) for id-driven fetches: recursion, id queries, global tag lookups (up to 2.2× faster than DuckLake key ranges). Spatial scans stay on DuckLake. The updater deletes and re-appends touched ids and records the Lance versions in `lake.state` in the same transaction; readers open Lance at those versions, so both stores always show the same diff. Compaction merges Lance fragments and updates the index. Cost: about 1.25× the size of the whole lake's Parquet again. |
| **Server** | Read-only and stateless. Attaches the catalog object directly and reopens it when its ETag changes (`SUNCHASER_CATALOG_REFRESH`). One DuckDB cursor per request; `[timeout:N]` interrupts the query; responses stream. |

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

## Updates

The updater, per replication diff:
1. **Parse:** read the osmChange and keep the newest version per element.
2. **Regional filter** (only for extracts): keep existing elements, nodes in the region, ways using kept nodes, and relations with kept members (a fixpoint, so relation-of-relation works).
3. **Cascade:** moved nodes → ways using them (`node_parent_ways`) → relations containing either (`member_parent_rels`). As in Overpass, only real geometry changes cascade: a moved or deleted node, or a way whose coordinates changed; tags-only edits don't. Cascaded rows are logged as `geometry` changes, which `(changed)` counts, as Overpass does. Coordinates, bboxes and Overpass keys are recomputed with the build's macros.
4. **Rewrite:** delete and re-insert every affected row in all tables. Deletes are pruned by the rows' old keys. Then advance `state`, all in one transaction.

Catch-up uses hourly diffs (a planet day diff is about 100 MB, too much for one batch), then minutely.

**Maintenance** runs between batches:
- **`build_areas.py`:** Overpass's area rules. Areas lag the data, as in Overpass.
- **`compact`:** flush inlined rows to sorted Parquet, merge small files, expire snapshots.
- **`publish.py`:** the frozen catalog.

## Deployment on R2 / S3 (scale to zero)

1. Create a bucket and an API token (R2: Object Read & Write).
2. Set the environment variables. See `deploy/local.env` for the local stand-in. For R2:
   - `SUNCHASER_S3_ENDPOINT=<account>.r2.cloudflarestorage.com`
   - `SUNCHASER_S3_REGION=auto`
   - `SUNCHASER_S3_USE_SSL=true`
3. Build once into a local catalog file with the data on S3, then upload the catalog:

   ```bash
   rust/osm-extract/target/release/osm-extract planet.osm.pbf raw
   ```

   ```bash
   python src/build_lake.py --raw raw --catalog catalog.ducklake --data-path s3://bucket/lake/data/ --lance s3://bucket/lake/lance --osm-base <pbf timestamp>
   ```

   ```bash
   SUNCHASER_CATALOG=catalog.ducklake python src/build_areas.py
   ```

   ```bash
   python src/objcatalog.py put catalog.ducklake s3://bucket/lake/catalog.ducklake
   ```

4. Schedule the updater, for example every minute (cron, EventBridge, Cloud Scheduler). A run that finds nothing new exits without writing:

   ```bash
   python src/updater.py job --catalog-url s3://bucket/lake/catalog.ducklake --max-seconds 50
   ```

5. Run read-only API servers anywhere, with `SUNCHASER_CATALOG=s3://bucket/lake/catalog.ducklake`.

`docs/aws.md` has the full AWS walkthrough for a planet build with full history.

## Validation

| What | Result |
|---|---|
| Differential test (`tools/compare_overpass.py`): 58 requests vs a real Overpass on the same extract | 57/58 element-identical. The exception is Overpass's `out bb` for relations with members missing from an extract |
| Overpass Turbo (`tools/turbo_check.js`) | loads and renders results from the service |
| Index math (`tests/test_quadtile.py`) | bit-identical to Overpass's C++ |
| Engine regression (`tests/test_corpus.py`) | the indexed lake equals a brute-force evaluator on 24 queries |
| After 5 days of replayed diffs | 66/69 non-area and 9/10 area-related queries identical; the differences come from the reference's state |
| Fixed 24-hour window (both sides given the lake's edits) | 68/69 current-data queries; 13/13 history queries (`[date:]` incl. `out meta`/`geom`, `newer`, `changed`) vs Overpass with `--keep-attic` |
| Object-storage catalog, updated only by scheduled jobs (24 h window) | 81/82 queries incl. 13/13 history; two overlapping jobs: one commits, the other is refused (412) and discarded |
| Lance id store kept by jobs through 24 diffs and a compaction | pinned versions equal the lake tables row for row; 81/82 queries incl. 13/13 history with id fetches on Lance; a second concurrent job exits on the lease |
| Bucketed vs flat build | identical tables, files in key order (`osm-extract --bucket-bits 12`, 16 groups) |

## Known gaps

- **Not implemented:**
  - `[diff]`, `[adiff]`, `timeline`, `retro`.
  - `make`, `convert`, `for`, `complete`, `compare` and `if` blocks.
  - Antimeridian-crossing bboxes.
  - Ways as areas: this Overpass version's rules build areas from relations only.
- **Performance:**
  - Single queries take 0.3–3 s on a 4-core box, against 0.1–2.4 s for local-disk Overpass.
  - Large outputs: JSON nodes and ways are serialised in SQL (about 32 MB/s); other formats and modes in Python (about 10 MB/s).
- **Areas** are rebuilt in full, not incrementally.
- **Planet scale:** see `docs/planet.md` for the build and update plan, and what is measured vs extrapolated.
