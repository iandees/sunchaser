# A cloud-native Overpass: design notes

> Historical: the design exploration this project started from. What was built, and where it differs, is in [`architecture.md`](architecture.md).

*Source reviewed: `drolbr/Overpass-API` master (shallow clone, 2026-10-29). External facts checked on the same date.*

---

## 1. How Overpass indexes OSM today

Overpass is one hand-built storage engine (`src/template_db/`) plus about 25 OSM-specific files (`src/overpass_api/core/settings.cc:125-302`). Its design boils down to a few key choices.

### 1.1 Storage engine (`template_db`)
- **Block_Backend**: each file is a sorted sequence of `(Index → [objects])` stored in fixed-size blocks of 128 KB to 2 MB. Blocks are LZ4 or zlib compressed, at a compression factor of 8.
- **`.idx` files**: a small index of block → index-range for each file. This is effectively a **zone map**.
- **Random_File (`.map`)**: a dense array of `id → current spatial index`, used for id lookups (`random_file_index.h`).
- **Transactions**: shadow-copy writes, with a **single writer** and many readers coordinated by the `dispatcher` over shared memory. Readers see a consistent snapshot.

### 1.2 Spatial key: the quadtile
- Coordinates are fixed-point at 1e-7°, with latitude offset by +91° (`index_computations.h:1257`).
- The upper 16 bits of lat and lon are **bit-interleaved (Morton / Z-order)** into a 32-bit `ll_upper` (`index_computations.h:58`). One tile is about 0.0066°, roughly 730 m north-south.
- **Nodes**: the key is `Uint32_Index = ll_upper`. The skeleton stores `(id, ll_lower)`, where `ll_lower` holds the low 16+16 bits.
- **Ways and relations**: the key is `Uint31_Index`.
  - If the element's bbox fits in one tile, the key is that tile.
  - Otherwise the high bit is set and the low bits encode a compound tile. There are 7 levels, flagged `0x01, 0x02, 0x04 … 0x40`. The extent grows by 4× per axis per level: 4, 16, 64, 256, 1024, 4096, 16384 tiles per axis (about 0.026° at level 1, up to about 107° at level 7). Each compound tile is anchored on an aligned corner, with the anchor's low 1, 3, 5 … 13 bits per axis cleared (2, 8, 32 … 8192 tile alignment). Flag `0x80` (`0x80000080`) means planet-wide and is always probed (`calc_index`, `index_computations.h:118`).
  - This is a **multi-level Z-order key**, much like an S2 or H3 cell-level hierarchy.
- **Bbox queries**: `calc_ranges` turns a bbox into a set of Morton ranges. Ways and relations are also probed at every coarser parent level (`calc_parents`).

### 1.3 Element payloads
- **`Way_Skeleton`**: `nds` (node ids), plus optional `Quad_Coord` geometry so `out geom` doesn't need node lookups.
- **`Relation_Skeleton`**: `members`, plus cached `node_idxs` and `way_idxs`, which are the spatial keys of the members. Recursing into members becomes a range scan, not an id lookup.

### 1.4 Tag indexes (two of them)
| File | Key | Value | Serves |
|---|---|---|---|
| `*_tags_local` | `(tile & 0x7fffff00, key, value)` | ids | tag filter **inside** a known spatial area |
| `*_tags_global` | `(key, value, idx-prefix)` | `(id, idx)` | tag filter **without** a bbox (inverted index) |
| `*_frequent_tags` | key | value counts | decides how to split huge posting lists |

- **Frequent-tag splitting** (`tags_global_writer.h`): a `key=value` posting list longer than 8 K, 512 K, or 32 M entries is **sub-partitioned by 8, 16, or 24 bits of the spatial index**.
- So `building=yes` becomes many spatially-clustered posting lists. This is in effect a composite sort key `(key, value, quadtile)`.

### 1.5 Metadata and history ("attic")
- **`*_meta`**: holds `(version, timestamp, changeset, uid)`, keyed by the same spatial index. Users are stored separately in `user_data` and `user_indices`.
- **Attic files**:
  - Every current file has a `*_attic` twin with the same layout. Each object is wrapped in `Attic<T>`, which carries the **timestamp at which that version stopped being valid** (`node_updater.cc:117`).
  - Ways are stored as `Way_Delta` against the current version.
  - Supporting files:
    - `*_attic_undeleted`: deletion and undeletion markers.
    - `*_attic_indexes`: id → every spatial key the element ever had.
    - `*_changelog`: `timestamp → (old_idx, new_idx, id)`. This drives `changed`, `diff`, and `adiff`.
- **Minor versions**: when a node moves, its parent ways and relations get new geometry and possibly a new spatial index **without a new OSM version**. The updater propagates this cascade (node → way → relation), and history records it too.
- **History starts at 2012-09-12**, the date of the ODbL licence change.

### 1.6 Areas
- `area_blocks` store polygon segments bucketed by tile. They are built by a **batch job** (`osm3s_query --rules` with `rules/areas.osm3s`), not by the minutely updater.
- So even upstream Overpass treats areas as eventually consistent.

### 1.7 What this means for a redesign
Overpass is **a clustered, Z-order-sorted columnar-ish store with zone maps, two inverted tag indexes, an id→location map, and SCD-style history split into "current" and "attic"**. Each of those maps onto lakehouse primitives:

| Overpass concept | Lakehouse equivalent |
|---|---|
| Block + `.idx` zone map | Parquet row-group min/max stats + DuckLake file-level stats |
| Quadtile Morton sort | `ORDER BY qt` (DuckLake 1.0 sorted tables) |
| Multi-level way/rel index | `qt` + `qt_level` columns (or bbox columns) |
| `tags_local` | tag filter evaluated after spatial pruning |
| `tags_global` + frequent split | `tag_postings` table sorted by `(key, value, qt)` |
| `.map` id → idx | id-sorted lookup table, or a Lance BTree plus `take` |
| current vs attic | `*_current` vs `*_history` tables (valid_from / valid_to) |
| changelog | `changes` table sorted by timestamp |
| dispatcher snapshot isolation | DuckLake snapshots / Lance versions |
| single-writer updater | single ingest service committing to DuckLake |

The rest of this document fills in that table.

---

## 2. The data sources, as they actually are

I checked the public buckets on 2026-10-29. **`s3://osm-pds` is still weekly.**

- **`osm-pds` (us-east-1, managed by OSMF and Pacific Atlas)**:
  - `planet/planet-latest.orc` (130 GB) and `planet-history/history-latest.orc` (222 GB).
  - `changesets-latest.orc`.
  - Dated weekly copies under `2026/` (41 weeks each of planet, history, and changesets, in both PBF and ORC).
  - **No Parquet, and no daily or hourly products.**
- **Where "minutely/hourly/daily" comes from**: the registry text refers to OSMF's **OsmChange replication** in `s3://osm-planet-eu-central-1/planet/replication/{minute,hour,day}/`. That prefix exists and is mirrored to us-west-2, with SNS notifications.
- **Implication**: use osm-pds **only for bootstrapping** (weekly history ORC/PBF). Then apply OSMF replication diffs at whatever cadence you choose. Minutely data is available today; you don't need to wait for osm-pds.
- **Ingest engine option worth evaluating**: [ohsome-planet](https://heigit.org/new-release-of-ohsome-planet-analyse-osm-data-at-the-pace-it-is-produced-with-minutely-updates/) (HeiGIT, Jan 2026).
  - It turns history PBF plus minutely `.osc` into **full-history Parquet with reconstructed geometries**.
  - It keeps a RocksDB "latest state" store, which is exactly the state the node→way→relation cascade needs.
  - Adopting or forking it could replace most of §5.1.

---

## 3. Technology roles

### 3.1 DuckDB: the query engine
- **Execution model**: Overpass QL is a sequence of statements over named sets (`._`, `.a`, …). That maps directly onto **DuckDB temp tables per set**, executed in order.
- **Features that fit**:
  - `ASOF JOIN` reconstructs "way geometry at time *t*" from node history.
  - The spatial extension (`ST_Contains`, `ST_Intersects`, and the `GEOMETRY` type, which DuckLake 1.0 supports) handles `poly:`, `around`, and area queries.
  - `regexp_matches` covers `~` filters.
  - `list_contains` and `unnest` handle `nds` and members.
- **Deployment**: stateless query workers (containers or Lambda) attached to the lake, with a local NVMe cache for hot Parquet ranges.

### 3.2 DuckLake: the table format and catalog
- **Status**: DuckLake 1.0 shipped in April 2026 with DuckDB 1.5.2. Relevant features:
  - **Data inlining**, on by default: small inserts, updates, and deletes live in the catalog DB until they are flushed. This solves the "a Parquet file per minute" problem.
  - **Sorted tables**: data is sorted on flush and compaction, so the spatial clustering stays in place.
  - **Bucket partitioning**, `GEOMETRY`, and snapshots / time travel.
- **Two deployments**:
  - **Live DuckLake** (Postgres catalog plus an S3 data path): the single ingest writer commits each minutely diff as one snapshot. Query workers attach it and get snapshot isolation, which replaces the dispatcher.
  - **Frozen DuckLake**: a read-only `.ducklake` catalog file published to object storage or a CDN. Clients `ATTACH 'ducklake:https://…/osm-2026-10-29.ducklake'` with no server at all.
    - Publish one hourly or daily for:
      - (a) serverless and edge readers;
      - (b) public redistribution ("Overpass-in-a-file");
      - (c) a cheap, reproducible base layer for the API.
    - Older catalog files stay valid as long as the Parquet they reference isn't garbage-collected. Keep daily freezes for 30+ days.
- **Two kinds of time**: use DuckLake snapshots for **operational** consistency, not for **OSM** time travel.
  - OSM time (`[date:]`, `[diff:]`) must be modelled explicitly with `valid_from` / `valid_to`.
  - Lake snapshots only begin when you start ingesting.
  - Keeping every minutely snapshot forever would also block compaction from reclaiming files.

### 3.3 Lance: random access, and where Parquet is weak
Overpass gets a lot of speed from cheap random access (the `.map` id lookup and member fetches), and Parquet on S3 is poor at that. A point lookup costs at least one row group, about 1 MB.

Lance fits these roles:
- **Id-keyed stores** (`nodes_by_id`, `ways_by_id`, `relations_by_id`): a BTree scalar index on `id` plus `take()` fetches a few thousand scattered rows in small reads. This serves `node(123)`, `way(id:…)`, member recursion without a cached spatial key, and `<` back-references.
- **The ingest-side "current state" store**: `merge_insert` upserts with versioned manifests. It can replace RocksDB from ohsome-planet if you'd rather keep everything in one format on object storage.
- **Tag indexes**:
  - A `LabelList` index on a `kv: list<string>` column (`"amenity=cafe"`) behaves like `tags_global`.
  - `Bitmap` on `key` presence.
  - `NGram` or inverted indexes on `name` speed up common `name~"…"` searches.

Caveats:
- The DuckDB `lance` extension is a core extension. Pushdown of scalar filters and `take` was still described as work in progress in the [DuckDB May 2026 post](https://duckdb.org/2026/05/21/test-driving-lance). Plan to call Lance's own Rust or Python API for id lookups and pass results into DuckDB as Arrow, rather than relying on SQL pushdown.
- Lance indexes go stale on append, so you need `optimize_indices` and compaction jobs. That's fine on an hourly cadence and awkward on a minutely one.

**Recommendation**: start with **Parquet / DuckLake everywhere**. Benchmark id lookups and global tag lookups. Bring in Lance for those two access paths **only if** the id-sorted Parquet tables (§4) don't meet the latency target. Keep the canonical history in DuckLake either way.

---

## 4. Proposed schema

All coordinates are stored as int32 at 1e-7°, like Overpass. `qt` is the Overpass-compatible 32-bit Morton key, which lets you port `calc_ranges` and `calc_index` and cross-check results bit-for-bit against a real Overpass.

```sql
-- CURRENT (hot path; ~ Overpass nodes / ways / relations + _meta)
nodes_current(qt UINT32, id BIGINT, lat INT, lon INT, tags MAP(VARCHAR,VARCHAR),
              version INT, ts TIMESTAMP, changeset BIGINT, uid INT)
  -- SORTED BY (qt, id)

ways_current(qt UINT32, qt_level UTINYINT,  -- 0 = single tile, 1..7 = Overpass levels, 8 = "global"
             id BIGINT, min_lat INT, min_lon INT, max_lat INT, max_lon INT,
             nds BIGINT[], geom GEOMETRY,     -- denormalised like Way_Skeleton.geometry
             tags MAP(VARCHAR,VARCHAR), version INT, ts TIMESTAMP, changeset BIGINT, uid INT)
  -- SORTED BY (qt_level, qt, id)

relations_current(qt, qt_level, id, bbox…, members STRUCT(type, ref, role)[],
                  member_qt UINT32[],         -- like Relation_Skeleton.node_idxs/way_idxs
                  tags, meta…)

-- ID LOOKUP (~ the .map files); tiny rows, sorted by id → tight min/max pruning
elem_locator(type UTINYINT, id BIGINT, qt UINT32, qt_level UTINYINT)  SORTED BY (type, id)

-- GLOBAL TAG INDEX (~ tags_global + frequent split; one row per (elem, tag))
tag_postings(type, key, value, qt, id)  SORTED BY (type, key, value, qt)

-- REVERSE MEMBERSHIP (for "<" and for the update cascade)
node_parent_ways(node_id, way_id)       SORTED BY node_id
member_parent_rels(mtype, mid, rel_id)  SORTED BY (mtype, mid)

-- HISTORY (~ *_attic + *_attic_undeleted); one row per (major OR minor) version
nodes_history(qt, id, version, valid_from, valid_to, visible, lat, lon, tags, meta…)
ways_history(qt, qt_level, id, version, minor, valid_from, valid_to, visible, nds, geom, tags, meta…)
relations_history(…)
  -- SORTED BY (qt_level, qt, valid_from)

-- CHANGELOG (~ *_changelog), drives changed / diff / adiff / augmented diffs
changes(ts, type, id, old_version, new_version, old_qt, new_qt)  SORTED BY ts

-- AREAS (~ area_blocks), rebuilt hourly/daily like upstream
areas(area_id BIGINT, qt, qt_level, bbox…, geom GEOMETRY, tags)
```

### Why these choices
- **Sorting by `(qt_level, qt)`**: each Parquet row group covers a contiguous Morton range, so a bbox query reads only the row groups whose `[min(qt), max(qt)]` overlaps a range from `calc_ranges`. This reproduces Overpass's `.idx` behaviour. The bbox columns add a second pruning filter that is cheaper and exact.
- **`tag_postings` sorted by `(key, value, qt)`**: this is Overpass's frequent-tag split done for free by the sort order.
  - `nwr[amenity=cafe]` globally reads only the `amenity/cafe` row groups.
  - Adding a bbox prunes further on `qt`.
  - `[key~"^addr:"]` uses the key range.
  - A value regex scans one key's segment.
- **Coarse partitioning**: partition by the top 4–6 Morton bits (16–64 world cells) for file-level pruning and parallel compaction. Don't partition finer than that, or you get too many small files.
- **Planet-spanning objects** (`qt_level` 8, e.g. coastline relations): they go in their own small partition and are always scanned, just as Overpass always probes `0x80000080`.

### Spatial shortcuts that avoid id lookups
- `way → nodes (>)`: the nodes lie inside the way's bbox. Scan `nodes_current` over the way's `qt` ranges, then semi-join on `id ∈ nds`. No id index needed.
- `node → ways (<)`: probe `ways_current` at the node's tile and every ancestor level (`calc_parents`), then filter with `list_contains(nds, id)`. Use `node_parent_ways` only for large node sets.
- `out geom` for ways needs no join, because the geometry is denormalised.

---

## 5. Ingest and freshness

### 5.1 Pipeline
```
osm-pds weekly history ORC/PBF ──► bootstrap (Spark/DuckDB/ohsome-planet) ──► DuckLake (sorted)
                                                                              ▲
OSMF replication minute/*.osc.gz ──► single ingest service ─────────────────┘ one snapshot per diff
   (SNS-triggered)                    • state store (RocksDB / Lance / DuckLake current tables)
                                      • cascade: node move → ways (node_parent_ways) → relations
                                      • emits: close old rows (valid_to), insert new rows, changes rows
                                                     │
                     hourly: flush inlined → sorted Parquet; refresh tag_postings, areas
                     daily : compact + publish FROZEN DuckLake catalog (+ optional Lance indices)
```

### 5.2 Notes
- **Bootstrap input**: DuckDB has no ORC reader. Either convert with PyArrow (`pyarrow.orc` → Parquet) or start from `history-latest.osm.pbf` (164 GB) with osmium or ohsome-planet. The ORC schema has no way geometry, so you build geometry in a batch job: join ways to nodes, then ASOF-join for history.
- **Minutely writes**: each minute is a small upsert (hundreds to a few thousand elements). DuckLake data inlining keeps those in the Postgres catalog until the hourly flush, so the minute-level cost is a few SQL transactions.
- **Current tables**: an update is "delete old row + insert new row". Inlined deletes, and deletion vectors after flush, handle that. Compact hourly or daily so sort order and pruning stay tight.
- **Areas, `tag_postings`, and the frozen catalog** can lag by up to one hour or one day. Upstream areas already lag, so this fallback is cheap.
- **Write ownership**: run exactly one writer, as Overpass does. Many query workers read either the live catalog (minutely freshness) or the latest frozen one (daily freshness, fully serverless).

---

## 6. Executing Overpass QL

Don't try to reuse the C++ statement classes. Each one calls `Block_Backend` / `Resource_Manager` directly (`src/overpass_api/statements/`, about 60 statement types). Instead:

1. **Parse** QL (and the XML form) into a statement tree. Port the grammar from `src/overpass_api/frontend/` or reuse an existing JS/TS parser.
2. **Plan** each statement as SQL over the set tables. A query like `node[amenity=cafe](bbox)` compiles to:
   ```sql
   CREATE TEMP TABLE _ AS
   SELECT n.* FROM nodes_current n
   WHERE (n.qt BETWEEN ? AND ? OR …)          -- calc_ranges(bbox)
     AND n.lat BETWEEN ? AND ? AND n.lon BETWEEN ? AND ?
     AND n.tags['amenity'] = 'cafe';
   ```
   The planner picks spatial-first or `tag_postings`-first from cardinality estimates. Overpass makes the same choice in `query.cc`.
3. **Swap tables for historical queries**: `[date:T]` uses `*_history WHERE valid_from <= T AND T < valid_to`. `[diff:T1,T2]` and `[adiff:…]` read `changes` between the two timestamps.
4. **Output** XML, JSON, CSV, or `out geom/center/bb/meta/ids/tags/count` from the final set.

### Coverage tiers
| Tier | QL features | Data needed |
|---|---|---|
| **1. Core** | `node/way/rel/nwr`, tag filters (`=`, `!=`, `~`, `!~`, exists, `i`), `(bbox)`, `(id:)`, `around`, `poly`, `>`, `>>`, `<`, `<<`, `(w)`, `(r)`, `(bn)`, union, difference, named sets, `out *`, `[out:json/csv]`, `[bbox]`, `[timeout/maxsize]` | current tables only |
| **2. Areas** | `area[...]`, `(area)`, `map_to_area`, `is_in`, `pivot` | `areas` (hourly/daily) |
| **3. History** | `[date:]`, `[diff:]`, `[adiff:]`, `changed`, `newer`, `user`, `uid`, `timeline`, `retro` | history + changes |
| **4. Programmable** | evaluators (`if:`, `t[]`, `count()`, `length()`, `is_closed()`, …), `make`, `convert`, `for`, `foreach`, `complete`, `if/else`, `compare`, `local` | engine work, no new data |

Tier 4 is engine work only. DuckDB expressions plus small macros cover almost all evaluators. `complete` and `foreach` need a host-language loop around SQL.

---

## 7. Fallbacks, ordered from least to most ambitious

| Ambition | Fallback | Cost of fallback |
|---|---|---|
| Minutely | Hourly diffs and hourly frozen catalogs; fully serverless, no Postgres | Small: most Overpass users tolerate minutes of lag |
| Full history since 2005 | History from 2012-09-12 (Overpass parity), or from the start of ingest only | Matches the reference implementation |
| Minor versions (way geometry history) | Major versions only; build geometry at query time with an ASOF join | Slower `[date:]` geometry queries |
| Full QL | Tiers 1–2 (most public Overpass traffic, e.g. overpass-turbo, iD, JOSM downloads) | Loses retro, timeline, and make/convert |
| Live areas | Daily areas | Same as upstream practice |

---

## 8. Suggested phases

1. **Spike (2–3 weeks)**:
   - Load one region (Geofabrik extract) into `nodes/ways/relations_current` and `tag_postings`, sorted by Overpass `qt`.
   - Port `calc_ranges` and `calc_index`.
   - Hand-translate 20 representative QL queries to SQL.
   - Measure cold and warm latency from S3 versus a local Overpass.
   - Prototype Lance `take` for id lookups as a direct comparison.
2. **Planet, current, daily**: bootstrap from osm-pds, apply daily diffs, and publish a frozen DuckLake each day. Build the Tier 1 translator and run differential tests against a reference Overpass instance on a query corpus (overpass-turbo examples, plus `osm-3s_testing/`).
3. **Hourly and areas**: hourly diffs, the `areas` build, and Tier 2.
4. **Minutely and history**: a live DuckLake with a Postgres catalog, the single-writer cascade, `*_history`, `changes`, and Tier 3.
5. **Full QL**: Tier 4, plus `adiff` output fidelity.

### Main risks
- **Small-query latency from object storage** compared with Overpass's local mmap. Mitigate with a worker-local NVMe cache and by keeping catalog and Parquet footers in memory.
- **The minutely cascade at planet scale**: big relations touch many members. This is where ohsome-planet's existing implementation is worth adopting.
- **Matching Overpass output exactly**: ordering, `out` modes, and area id offsets (2400000000 for ways, 3600000000 for relations).

---

### Sources
- Overpass source: `src/template_db/*`, `src/overpass_api/core/{settings.cc,index_computations.h,type_*.h}`, `src/overpass_api/osm-backend/{tags_global_writer.h,node_updater.cc}`
- [OSM on AWS registry](https://registry.opendata.aws/osm/) · [open-data-docs/osm-pds](https://github.com/awslabs/open-data-docs/tree/main/docs/osm-pds) · bucket listing of `s3://osm-pds` and `s3://osm-planet-eu-central-1/planet/replication/` (2026-10-29)
- [Frozen DuckLakes](https://ducklake.select/2025/10/24/frozen-ducklake/) · [DuckLake 1.0](https://ducklake.select/2026/04/13/ducklake-10/) · [DuckLake sorted tables](https://www.ducklake.select/docs/stable/duckdb/advanced_features/sorted_tables)
- [DuckDB lance extension](https://duckdb.org/docs/current/core_extensions/lance) · [Test-driving Lance in DuckDB](https://duckdb.org/2026/05/21/test-driving-lance) · [Lance scalar indexes](https://lancedb.com/documentation/guides/indexing/scalar-index.html)
- [ohsome-planet minutely release](https://heigit.org/new-release-of-ohsome-planet-analyse-osm-data-at-the-pace-it-is-produced-with-minutely-updates/)
