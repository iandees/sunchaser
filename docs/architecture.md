# sunchaser architecture (as built)

`docs/design.md` is the original design exploration. This document describes what is implemented
now and why.

## Components

```
            OSMF replication (minute/hour diffs)
                          │
                ┌─────────▼──────────┐   single writer
                │  src/updater.py    │── areas rebuild, compaction, frozen-catalog publish
                └─────────┬──────────┘
          catalog commits │ Parquet PUTs
   ┌──────────────────────▼──────────────────────────────────┐
   │ DuckLake                                                │
   │  catalog: SQLite (WAL) / Postgres   data: S3 / R2       │
   │  frozen catalog: s3://…/catalog.ducklake (published)    │
   └──────────────────────┬──────────────────────────────────┘
                          │ read-only (live catalog or frozen file)
                ┌─────────▼──────────┐
                │  src/server.py     │  /api/interpreter  ← Overpass Turbo, scripts
                │  ql → engine → out │
                └────────────────────┘
```

| Component | Role |
|---|---|
| **Updater** | The only writer, like Overpass's `update_database`. Each replication diff is one DuckLake transaction. Readers see whole diffs or nothing. |
| **Live catalog** | SQLite (WAL) on the writer's host, or Postgres for multi-host readers. Small updates stay inlined in the catalog until a flush. |
| **Frozen catalog** | A DuckDB-format `.ducklake` the updater publishes to the bucket every few minutes. Servers anywhere can read the lake with nothing but object storage, and reload when the published object changes. |
| **Server** | Stateless. One DuckDB cursor per request. `[timeout:N]` interrupts the query. |

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

## Deployment on R2 / S3

1. Create a bucket and an API token (R2: Object Read & Write).
2. Set the environment variables. See `deploy/local.env` for the local stand-in. For R2:
   - `SUNCHASER_S3_ENDPOINT=<account>.r2.cloudflarestorage.com`
   - `SUNCHASER_S3_REGION=auto`
   - `SUNCHASER_S3_USE_SSL=true`
3. Build once from a planet or extract PBF:

   ```bash
   rust/osm-extract/target/release/osm-extract planet.osm.pbf raw
   ```

   ```bash
   python src/build_lake.py --raw raw --osm-base <pbf timestamp>
   ```

   ```bash
   python src/build_areas.py
   ```

4. Run the updater (one host):

   ```bash
   python src/updater.py run --publish-to s3://bucket/lake/catalog.ducklake
   ```

5. Run any number of API servers:
   - Same host: point them at the live SQLite catalog for minutely freshness.
   - Elsewhere: use `SUNCHASER_CATALOG=s3://bucket/lake/catalog.ducklake` for freshness of a few minutes and no catalog database.
   - For minutely freshness across hosts, use a Postgres catalog.

## Validation

| What | Result |
|---|---|
| Differential test (`tools/compare_overpass.py`): 58 requests vs a real Overpass on the same extract | 57/58 element-identical. The exception is Overpass's `out bb` for relations with members missing from an extract |
| Overpass Turbo (`tools/turbo_check.js`) | loads and renders results from the service |
| Index math (`tests/test_quadtile.py`) | bit-identical to Overpass's C++ |
| Engine regression (`tests/test_corpus.py`) | the indexed lake equals a brute-force evaluator on 24 queries |
| After 5 days of replayed diffs | 66/69 non-area and 9/10 area-related queries identical; the differences come from the reference's state |
| Fixed 24-hour window (both sides given the lake's edits) | 68/69 current-data queries; 13/13 history queries (`[date:]` incl. `out meta`/`geom`, `newer`, `changed`) vs Overpass with `--keep-attic` |
| Bucketed vs flat build | identical tables, files in key order (`osm-extract --bucket-bits 12`, 16 groups) |

## Known gaps

- **Not implemented:**
  - `[diff]`, `[adiff]`, `timeline`, `retro`.
  - `make`, `convert`, `for`, `complete`, `compare` and `if` blocks.
  - Antimeridian-crossing bboxes.
  - Ways as areas: this Overpass version's rules build areas from relations only.
- **Performance:**
  - Single queries take 0.3–3 s on a 4-core box, against 0.1–2.4 s for local-disk Overpass.
  - Large outputs are bound by Python serialisation, at about 10 MB/s.
  - Lance as an id-lookup store (`results/NOTES.md`) is not wired into the live path.
- **Areas** are rebuilt in full, not incrementally.
- **Planet scale:** see `docs/planet.md` for the build and update plan, and what is measured vs extrapolated.
