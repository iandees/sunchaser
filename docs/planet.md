# Running sunchaser for the planet

This is the plan for a planet-scale deployment. It is extrapolated from the Berlin measurements below. Nothing here has run at planet scale yet: the development box has 4 cores, 15 GB RAM and 250 GB of disk.

## Measured on the Berlin extract (4 cores)

| Step | Input | Time | Output |
|---|---|---|---|
| `osm-extract` (bucketed, in-RAM index) | 183 MB PBF; 12.9 M nodes, 2.27 M ways, 30 k relations | 20–24 s | 600 MB raw Parquet |
| `osm-extract --dense-index` | same | 65 s (sparse ids: 3.6 GB of index pages for a city) | same |
| `build_lake.py` (bucketed) | raw Parquet | 35 s (flat input: 71 s) | 540 MB lake |
| Updater, regional (24 hourly diffs) | about 80 k changes per hour, planet-wide | 320 s for the day (5–14 s per hour) | |
| Updater, **no region filter** (every change applied) | one planet hour: 67–75 k changes, 54–60 k nodes, 5–7 k ways | 11–22 s per hour | |

The bucketed build was checked row by row against the flat build: identical tables, and every table's files are in key order.

## Planet sizes (OSM, late 2026, approximate)

About 10 B nodes, 1.1 B ways and 13 M relations; the PBF is about 85 GB, roughly 465× Berlin's. Scaled from Berlin per table, where each table follows the element count it is made of:

| Table | Berlin | Planet estimate |
|---|---|---|
| `nodes_untagged` | 98 MB | 75 GB |
| `nodes_tagged` | 32 MB | 15 GB |
| `ways` (with geometry) | 183 MB | 90 GB |
| `nodes_meta` | 58 MB | 45 GB |
| `tag_postings` | 71 MB | 35 GB |
| `node_parent_ways` | 40 MB | 30 GB |
| `elem_locator` | 29 MB | 22 GB |
| `relations`, `*_meta`, `member_parent_rels` | 26 MB | 15 GB |
| **Total** | 540 MB | **about 330 GB** of Parquet on R2/S3 |

Berlin has more tags per element than the planet average, so tag-heavy tables are probably over-estimated.

## Initial build

One large machine with local NVMe, for example 32–64 vCPU, 128–256 GB RAM and 2 TB NVMe (AWS `i4i.8xlarge`, or `c7gd`/`r7gd`). Run it in the same region as the source PBF (osm-pds publishes the weekly planet PBF in `us-east-1`) or next to the R2 bucket.

1. **Get the PBF.** Note its replication timestamp; it becomes `--osm-base`, and the updater finds its replication sequence from it.
2. **Extract:**

   ```bash
   osm-extract planet.osm.pbf /nvme/raw --bucket-bits 12 --dense-index /nvme/idx
   ```

   - The dense index is about 110 GB for node locations plus 35 GB for the way index. Both are sparse mmap files, written once, sequentially by id, then read randomly while ways and relations are resolved. Keep them on NVMe; page cache does the rest.
   - With 12 bucket bits, nodes, ways and relations each split into up to 4,096 quadtile-range files, and `node_parent_ways` into 4,096 id-range files. That is about 16 k open files; the soft `ulimit -n` must allow it.
   - Expected time is 1–2 h, bound by decompression and random index reads. The Berlin rate scaled by PBF size and 8× the cores gives about 25 min of CPU, so the index I/O decides it.
   - Output is about 350 GB of raw Parquet on NVMe.
3. **Build the lake** straight into the bucket:

   ```bash
   SUNCHASER_CATALOG=postgres:... SUNCHASER_DATA_PATH=s3://bucket/lake/ \
     python src/build_lake.py --raw /nvme/raw --group-rows 200000000
   ```

   - Each element table is filled one bucket group at a time: sort about 200 M rows, then append. Memory and DuckDB temp space stay bounded by the group, not the planet.
   - The only global sort is `tag_postings` by `key=value`, done per element type out of core (DuckDB spills to `temp_directory`, which should point at NVMe).
   - `elem_locator` needs no sort, because the PBF is already in id order.
   - Expected time is 2–4 h, mostly Parquet encoding, zstd and upload; Berlin's 35 s scaled by about 600× rows and 8× cores gives about 45 min of CPU.
4. **Areas:** `python src/build_areas.py`, about 1.2 M multipolygon and boundary relations, through DuckDB spatial (`ST_BuildArea`). This is a full rebuild today. At planet scale it should run per bucket of relations, or incrementally from `changes`; see below.
5. **Publish** a frozen catalog for stateless readers: `python src/updater.py publish s3://bucket/lake/catalog.ducklake`.

## Updates

- **Updater host:** one small, always-on host: 4–8 vCPU, 16–32 GB RAM, near the bucket.
- **Catalog:** use Postgres, for example RDS or Neon, not SQLite, so API servers on other hosts see minutely commits.
- **Command:**

  ```bash
  python src/updater.py run --publish-to s3://bucket/lake/catalog.ducklake
  ```

  Without `--region`, every change is applied.
- **Throughput:** a planet hour took 11–22 s to apply on 4 cores, against a Berlin-sized lake with no compaction between hours. That is 0.2–0.4 s per minutely diff, plus about 1 s of fixed overhead per commit.
  - Against the real planet tables, the deletes have to locate old rows. Lookups go through `elem_locator`, pruned by id zone maps, and deletes are pruned by key, so cost should grow with the number of row groups touched, not with table size. This is the part to measure first at scale.
- **What a commit writes:**
  - DuckLake 1.0 keeps both new rows (`data_inlining_row_limit`) and deletes (`ducklake_inlined_delete_*`) in the catalog. A minutely commit is a Postgres transaction, not a scatter of small Parquet and delete files.
  - Compaction (`updater.py compact`, hourly) flushes them into sorted Parquet and merges small files.
  - The planet touches every region every hour, so compaction rewrites the files a diff touched. Budget about 1–5 GB of rewrites per hour, and run it on a separate schedule from minutely applies; it is already separate in `Maintenance`.
- **Cascade:** moved nodes cause their ways and relations to be rewritten (Overpass does the same). The parent indexes (`node_parent_ways`, `member_parent_rels`) make this a lookup, not a scan.
- **History:** every replaced version moves to `*_history` with `valid_from`/`valid_to`, so attic queries work from the build date on. History grows by roughly the volume of changes: about 3–5 M element versions a day, so tens of GB a year. It is sorted like the current tables, so `[date:]` queries prune the same way.

## Serving

- **API servers** are stateless: `src/server.py` behind a load balancer, autoscaled on CPU. Each one needs only object storage plus either the Postgres catalog (minutely freshness) or the frozen catalog file (freshness of a few minutes, no database).
- **Cache:** the hot set is the Parquet footers and row groups of popular areas. DuckDB's external file cache, and a local NVMe cache in front of R2/S3, cut first-byte latency. R2 has no egress fees, which suits a public API.
- **Streaming:** responses stream as they are produced, so large results never sit in memory twice, and a slow client pauses its query. `SUNCHASER_MAX_QUERIES`, `SUNCHASER_MAX_OUTPUT` and `SUNCHASER_MAX_TIMEOUT` bound one server.
- **Output speed:** JSON for nodes and ways (without `meta` or clipping) is built in DuckDB SQL, at about 32 MB/s per query on 4 cores. A whole-city `out geom` (250 MB) takes 8 s, against 24 s in Python. XML, CSV, `meta`, relations and clipped geometry still serialise in Python, at about 10 MB/s.

## Open items for planet scale

- **Areas:** incremental rebuild, only for relations that changed or whose members moved (the updater's `changes` log has them). Today it is a full rebuild.
- **Update cost against planet-sized tables:** delete lookups, the compaction cadence and the size of the catalog's inlined data all need measuring.
- **Serialisation:** move XML, `out meta` and relation geometry into SQL as well.
- **Antimeridian-crossing boxes** in the query engine.
