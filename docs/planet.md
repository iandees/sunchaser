# Running sunchaser for the planet

What a full-history planet deployment needs, from a measured sizing run, plus how it runs. The step-by-step runbooks are `docs/hetzner-r2.md` (Hetzner + Cloudflare R2, recommended) and `docs/aws.md`.

## How the numbers were measured

`deploy/hetzner/sizing.sh` on a 4-core, 15 GB container (October 2026), in two parts:

1. **One streamed pass over the real history planet** (`history-261019.osm.pbf`, 164 GB). The file goes from OSMF's bucket straight into `osm-extract --cut` and is never stored. The pass:
   - counts the whole planet exactly (`planet_stats.json`);
   - cuts full-history extracts of three regions with different mixes.
2. **Per region:** the lake (with Lance) and its areas are built locally under `tools/measure.py`. Then `tools/size_planet.py` scales each table by the planet total it depends on (node, way or relation versions).

The range in each estimate is the spread across the three regions. Raw results are in `results/sizing/`.

| Region | Box | Versions | Share of the planet's versions |
|---|---|---|---|
| Luxembourg | 49.44–50.19 N, 5.73–6.53 E | 17.1 M | 0.09% |
| Berlin | 52.33–52.68 N, 13.08–13.77 E | 27.9 M | 0.15% |
| Swiss plateau | 46.9–47.6 N, 7.0–9.0 E | 55.1 M | 0.29% |

## The planet, exactly (history file of 2026-10-19)

| | Versions | Deleted versions | Ids ever used | Max id |
|---|---|---|---|---|
| Nodes | 16,657,902,975 | 1,522,378,172 | 12,453,707,759 | 14,298,154,526 |
| Ways | 2,471,108,843 | 142,010,722 | 1,366,233,306 | 1,569,574,897 |
| Relations | 67,784,271 | 3,819,482 | 18,740,003 | 21,713,761 |

There are 15.9 B node-position records (first positions, moves and deletions) and 36.5 B way node references across all way versions.

Measured on the cut pass itself (4 cores): it took 84 min and peaked at 11.6 GB RSS. The download averaged about 33 MB/s over 8 range connections. The extractor set the pace, not the network; the download alone reached 80 MB/s.

## Estimate: disk and storage

| Planet, full history | Estimate |
|---|---|
| Raw extraction (local disk) | 800–850 GB |
| Node position timeline (`--dense-index`): 8 B per id + 16 B per position record | 370 GB |
| Way geometry timeline | 115–120 GB |
| **Peak local disk during the extract** (the PBF is streamed, not stored) | **1.28–1.34 TB** |
| Lake in object storage (Parquet), of which: | **610–620 GB** |
| – history and change log (`*_history`, `changes`) | 350–380 GB |
| – current tables, indexes, metadata | 240–265 GB |
| Lance id store (current nodes and ways) | 490–550 GB |

- **Timelines:** they are deleted after the extract. The build then needs only the raw buckets, and `--consume` frees them as it goes, plus DuckDB sort spill. The regional builds' local work directories stayed under 2× their lake size.
- **Lance:** about 2× the size of the current Parquet tables it copies. On R2 that is about $8 a month, against $9 for the whole Parquet lake. Lance made queries 1.54× faster (geometric mean over the test corpus; see `results/NOTES.md`).
- **Box sizing:** a 2 × 1 TB RAID0 box (about 1.9 TB usable) has about 30% headroom at the extract's peak. A 1 TB laptop does not fit a full-history extract. It does fit a current-only planet.

## Estimate: time and memory

| Step | Measured on the regions (4 cores) | Planet, 4 cores | Planet, 6-core i7-8700 (≈) |
|---|---|---|---|
| Extract: history processing | 136–316 s; 126–175 k versions/s | 31–43 h | 20–30 h |
| Lake build (with Lance) | 89–246 s; peak RSS 7–12 GB (DuckDB takes what it's allowed) | 24–39 h | 15–25 h |
| Areas (full) | 2–10 s | under 1 h | under 1 h |

- **Scaling assumptions:** times are scaled linearly by versions. The core-count column assumes near-linear scaling with cores. Both are assumptions until the first planet build records real numbers (`grep MEASURE /data/logs/*.log`).
- **Extract:** decoding is not the bottleneck; the cut pass decoded the whole planet in 84 min on 4 cores. History processing is. It runs per id group, in parallel, and is CPU-bound.
- **Build:** mostly sorting and Parquet encoding. The two relation tables are sorted single-threaded. A parallel sorted insert into DuckLake ran out of memory on Berlin's relation history (490 k versions carrying 303 M member entries), while one thread spills to disk and finishes. Relation history is small in rows but large in members: about 8.1 B member entries across the planet's relation versions.
- **Memory:** the extractor stays around 3–12 GB, with mimalloc and bounded writer buffers. DuckDB uses up to its memory limit (80% of RAM by default) and spills the rest to local disk. 64 GB is enough; 128 GB leaves page cache for the timelines.

Total for a 6-core box: about 2 days of compute, extract plus build. That is roughly $12 at $0.24/h for a Hetzner auction box.

## Running costs (R2 + Hetzner)

| | Per month |
|---|---|
| R2 storage: 610–620 GB of Parquet + 490–550 GB of Lance | ≈ $17 (≈ $9 without Lance) |
| R2 requests: a catalog upload per batch, plus compaction | a few dollars |
| Egress | $0 (R2) |
| Updater + API host | the Hetzner box, or a small VPS once the build is done |

## Updates

- **Shape:** one updater, `updater.py job --loop`, is the only writer. It holds a lease object next to the catalog and keeps the catalog file locally. After each batch it uploads it with `If-Match`, so a second writer can't overwrite it. API servers are read-only and reload when the catalog's ETag changes. Nothing has to stay on: the same job can run on a schedule (`updater.py job`) instead of `--loop`.
- **Throughput:** a planet hour applied in 11–22 s on 4 cores against a small lake. The cost against planet-sized tables still needs measuring on the real lake; deletes are located through `elem_locator` and pruned by key.
- **What a commit writes:**
  - New rows and deletes are inlined in the catalog file (DuckLake 1.0).
  - Compaction runs inside the job on its own cadence: it flushes inlined data, merges small files, expires snapshots and removes orphaned files.
  - Lance is updated in the same job, and its versions are pinned in `lake.state` in the same transaction.
- **Areas:** each batch rebuilds only the areas whose relations changed, including member way moves (the `changes` log's `batch` column).
- **History:** replaced versions move to `*_history`, so attic queries cover everything since 2005 and keep growing with the edits.

## Serving

- **API servers** (`src/server.py`) are stateless. They need only the bucket: the catalog file and the Parquet and Lance data.
- **Cache:** the hot set is Parquet footers and the row groups of popular areas. DuckDB's external file cache, and a local NVMe cache in front of R2, cut first-byte latency.
- **Streaming:** responses stream as they are produced. `SUNCHASER_MAX_QUERIES`, `SUNCHASER_MAX_OUTPUT` and `SUNCHASER_MAX_TIMEOUT` bound one server.

## Open items for planet scale

- **Extract throughput:** history processing is about 1.5 days on 4–6 cores. A box with more cores, or profiling the node group step (16.7 B versions), shortens it most.
- **Update cost against the real planet lake:** delete lookups, compaction time and the size of the catalog file.
- **Query latency against real R2:** the 1.54× Lance gain was measured with a local stand-in at 30 ms per request.
- **Antimeridian-crossing boxes** in the query engine.
