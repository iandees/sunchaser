# Runbook: planet build and operation on Cloudflare R2

The lake lives in **Cloudflare R2**, which has no egress fees. Compute is a **Hetzner dedicated
server** with unmetered traffic, or your own machine. The same commands work against AWS S3; see
"The same on S3" at the end.

**Ground rules**
- Ask before anything that erases disks (`installimage`), costs money beyond the rented box, exposes the API publicly, or deletes data in R2.
- R2 credentials live only in `/etc/sunchaser.env` on the box (mode 600). Never commit them or paste them into logs.
- Run anything longer than a few minutes inside `tmux`. The build script is resumable by stage.

## Status

Every command here has run on a Berlin extract against an S3-compatible stand-in, and Minnesota
and Germany have been built end to end on the target box (below). Results matched a real Overpass
with attic data. The planet build itself has not run.

Planet sizes come from a measured sizing run (appendix, `results/sizing/`): exact planet totals, plus
three regional full-history lakes scaled up.

**Rehearsals on the box** (Hetzner auction server, 12 threads, 125 GB RAM, 2 x 954 GB NVMe as RAID0, Ubuntu 24.04), local disk, no `--dense-index`, `--group-rows 100000000`:

| | Minnesota | Germany | Ratio |
|---|---|---|---|
| History PBF (Geofabrik) | 0.49 GB | 12.4 GB | 25× |
| `osm-extract --history` | 1 min 43 s | 41 min 52 s | 24× |
| `build_lake.py --consume` | 1 min 46 s | 40 min 56 s | 23× |
| `build_areas.py --full` | 5 s | 21 s | n/a |
| Lake on disk (Parquet + Lance) | 3.3 GB | 57 GB (37 + 20) | 17× |
| Peak extra disk during the run | not measured | 64 GB (94 GB used, 30 GB before) | n/a |
| Peak RSS, extract / build | not measured | 67.5 GB / 109.8 GB | n/a |

Times scale linearly with the input size: the planet's 164 GB history PBF is 13× Germany's, so
**the extract and the build are each about 9 h on this box's 12 threads** (the sizing run's
20-30 h and 15-25 h were scaled from 4 cores and are superseded). Not measured: whether build RAM
stays bounded as the input grows, and how a planet-wide updater keeps up (a planet lake applies
every edit, about 5,000 changes per minute diff worldwide, where Minnesota's regional filter
applies a few dozen). Run the updater on Germany before trusting it on the planet.

**Build memory is DuckDB's buffer cache, not the sort size.** Germany's build peaked at 109 GB of
the box's 125 GB with either `--group-rows 100000000` or `50000000` (109.4 GB, 42:45 for the
latter): the cache grows across `nodes_untagged`, the second `tag_postings` step and
`ways_history` and is not released between steps. `--memory-limit 40GB` held the build to 42.2 GB in
45:42 (about 7% slower) with identical row counts and areas. So cap it with `--memory-limit`;
lowering `--group-rows` is not the fix. (On Minnesota a lower `--group-rows` did lower the peak,
6.6 GB at 100 M against 3.8 GB at 2 M, at the cost of more sorts, but that is not the effect at
Germany's scale.)

**Extractor memory and the dense index.** Germany's extract with `--dense-index` took 44:05 (41:03
without) and peaked at 84-98 GB RSS, of which only about 11 GB was anonymous memory; the rest is
the memory-mapped index, which the kernel can evict. Germany's index was 102 GB on disk and 253 GB
of disk was in use at the peak.

**Open risk: the planet extract may be far slower than 9 h.** When the index does not fit in the
page cache, the ways phase slows about 25× from random index reads (measured by running Germany in a
30 GB cgroup). A sorted-batch-lookup experiment made that 3× better, not enough, and is parked on
the branch `extract-sorted-lookups`. The planet's dense index is about 490 GB (the sizing run's
figure; see Disk), and this box has 125 GB of RAM, so the planet extract will run in the slow
regime unless the index is made smaller, the lookups are made sequential, or the box gets much more
RAM. The linear 9 h estimate does not account for this. Germany's measured 102 GB for a 12.4 GB PBF
does not scale linearly to 490 GB (13× would be 1.3 TB); the two have not been reconciled, and the
planet's index size is the least certain number here.

**Updater on R2** (Minnesota, a regional lake, copied to R2 in 4 min 39 s): a batch of 2-3 minutely
diffs commits in 3-14 s; the hourly Lance compaction steps take 19-38 s each; the catalog upload is
about 13 MB (compacted on push).

## What goes where

```
 OSMF history planet (S3, Frankfurt) --curl--> osm-extract (stdin) --> build --> R2 bucket
                                                                                 lake/catalog.ducklake
 OSMF replication diffs --> updater (job --loop on the box)  --conditional PUT--> lake/data/, lake/lance/
 Overpass Turbo / clients --HTTPS--> API (server.py, read-only) <-- range reads --+
```

| | Cost |
|---|---|
| R2 storage | $0.015/GB-month (Standard): about $17/month for the full-history lake with Lance (610-620 GB Parquet + 490-550 GB Lance), about $9 without Lance. No egress fees; reads $0.36 per million, writes $4.50 per million |
| Hetzner auction box | Billed hourly (e.g. $0.24/h for an i7-8700 with 128 GB and 2 x 1 TB NVMe), unmetered 1 Gbit. Build for about 2 days (about $12), then keep it, or move the updater and API to a small VPS |
| Downloading the history planet | Free (OSMF publishes it) |

## Disk: will the history build fit?

Full-history planet, peak local disk (from the sizing run):

| | Plain | Streamed input + `--consume` |
|---|---|---|
| History PBF | 164 GB | 0 (piped from the download) |
| Node position timeline (`--dense-index`) | 370 GB: 115 GB id-indexed part (8 B per id up to the max id, 14.3 G) + 255 GB position records (16 B x 15.9 B records) | the same, deleted before the build |
| Way geometry timeline | 115-120 GB | the same, deleted before the build |
| Raw buckets (current + history + changes) | 800-850 GB | the same, shrinking as the build consumes them |
| DuckDB sort spill | small (regional builds stayed under 2× their lake size) | the same |
| **Peak** | **about 1.45-1.5 TB** | **1.28-1.34 TB**, during the extract |

The timeline row is the whole dense index: about 485-490 GB (370 + 115-120). Other docs and notes
quoted "110 GB" (the id-indexed part only), "370 GB" (nodes) and "490 GB" (everything); these are
parts of the same total.

- **2 x 1 TB, as RAID0 (about 1.9 TB):** fits, with about 30% headroom when streaming.
- **A 1 TB laptop:** does not fit full history. A current-only planet (`osm-extract --no-history`) fits.
- **Decision point:** if the extract's peak is above about 90% of the free space on `/data`, stop. Options are a box with more NVMe, a current-only planet, or `--bucket-bits 10` (which does not lower the peak).

## 1. R2

1. **Bucket:** in the Cloudflare dashboard, R2, Create bucket `osm-lake`. Set the location hint to **Eastern Europe (EEUR)** for a Hetzner box in Helsinki, or to wherever the compute runs.
2. **Token:** R2, Manage API tokens, create a token with **Object Read & Write** on that bucket only. Note the access key id, the secret and the account id.
3. **Lifecycle rule:** abort incomplete multipart uploads after 1 day.

R2 supports the conditional PUTs (`If-Match`, `If-None-Match`) that the catalog and the writer lease rely on.

Put the endpoint, keys and `LAKE=s3://<bucket>/lake` into `/etc/sunchaser.env`; the template is
[`deploy/hetzner/sunchaser.env.example`](../deploy/hetzner/sunchaser.env.example), and `setup.sh`
installs it as `/etc/sunchaser.env` with mode 600 the first time. Smoke-test the credentials and
conditional writes:

```bash
cd /data/sunchaser && set -a && . /etc/sunchaser.env && set +a && uv run python -I -c "
import sys; sys.path.insert(0, 'src'); import objcatalog
b, k = objcatalog._split('$LAKE/smoke-test')
s3 = objcatalog._client()
s3.put_object(Bucket=b, Key=k, Body=b'ok', IfNoneMatch='*')
print(s3.get_object(Bucket=b, Key=k)['Body'].read())
s3.delete_object(Bucket=b, Key=k)"
```

It should print `b'ok'`. A 412 means the key existed already: delete it and re-run.

## 2. The Hetzner box

Box facts to check first (report them before going on):

```bash
ssh root@<box> 'nproc; free -g; lsblk -d -o NAME,SIZE,MODEL; df -h; head -2 /etc/os-release; ulimit -Hn'
```

The plan assumes Ubuntu 24.04, about 2 TB of RAID0 NVMe mounted at `/data`, 64-128 GB of RAM and 1 Gbit unmetered.

1. **Order it** from the server auction, which bills hourly. At least 64 GB RAM (more is better: the dense index benefits from page cache, see the open risk above), NVMe disks, a recent CPU (core count is the build's speed). Order it with the rescue system.
2. **Install Ubuntu 24.04 with RAID0** (only if it is not installed that way; this erases both disks). Hetzner's `installimage` defaults to RAID1, which halves the space. Copy [`deploy/hetzner/installimage.conf`](../deploy/hetzner/installimage.conf) to the rescue system as `/root/sunchaser.conf` (not `/autosetup`: a failed run deletes it) and edit `DRIVE1`/`DRIVE2` (`lsblk`) and `IMAGE` (`ls /root/.oldroot/nfs/images/`; the file may be `.tar.zst` or `.tar.gz`). `installimage` isn't on the rescue system's `PATH`; run it by its full path, then reboot:

   ```bash
   TERM=xterm /root/.oldroot/nfs/install/installimage -a -c /root/sunchaser.conf
   ```

   The installed system has new SSH host keys: run `ssh-keygen -R <ip>` on your side before reconnecting. `/boot` becomes RAID1 whatever `SWRAIDLEVEL` says; that's fine. The build doesn't need redundancy: if a disk dies, rebuild. `df -h /data` should show about 1.9 TB.

3. **Software**:

   ```bash
   ssh root@<box> 'git clone https://github.com/iandees/sunchaser.git /data/sunchaser && /data/sunchaser/deploy/hetzner/setup.sh'
   ```

   [`setup.sh`](../deploy/hetzner/setup.sh) is idempotent: it installs build tools, uv and Rust, builds the extractor, runs `uv sync --locked`, raises the open-files limit (log in again so it applies), and creates `/etc/sunchaser.env` from the template. It does not install `osmium-tool` or `rclone`; add them with `apt-get install -y osmium-tool rclone` for the regional rehearsal and the laptop variant. DuckDB's `ducklake`, `httpfs` and `spatial` extensions are installed on first use, so the first build or API start downloads them.

## 3. Regional rehearsal (optional)

Geofabrik offers full-history country extracts (`.osh.pbf`) to logged-in OSM users, at `https://osm-internal.download.geofabrik.de/<region>-internal.osh.pbf` (for example `north-america/us/minnesota-internal.osh.pbf`). Log in at that site in a browser and copy the `gf_download_oauth` cookie value; pass it to `curl -H "Cookie: gf_download_oauth=\"...\""`. Treat it as a credential: keep it in a mode-600 file, not in the repo or a shell history.

The header of these files has no replication timestamp, and the build stops with an error unless you pass `--osm-base` (without it the updater would have nowhere to start). Take the newest object's time (`osmium fileinfo -e -g data.timestamp.last FILE`) and round up to the next hour boundary (Geofabrik cuts its extracts at 00:00 UTC); both rehearsals used `--osm-base 2026-10-19T00:00:00Z` style values.

Run steps 4-6 on a country without `--dense-index` to time the box itself, and compare with the rehearsal table. Pass `--region s,w,n,e` to the updater (section 6).

## 4. Planet build, the production path

[`deploy/hetzner/build-planet.sh`](../deploy/hetzner/build-planet.sh) runs the stages below in order, each under `tools/measure.py`:

```bash
tmux new -s build 'cd /data/sunchaser && deploy/hetzner/build-planet.sh 2>&1 | tee -a /data/logs/build-planet.log'
```

Stages (each resumable; a finished stage leaves `/data/state/<stage>.done` and is skipped):
1. `extract`: the history planet is streamed into `osm-extract --history --bucket-bits 12 --writer-mem-mb N --dense-index DIR --input-size S`, checked against the published md5, and the timelines are deleted afterwards.
2. `build`: `build_lake.py ... --group-rows N --memory-limit 70GB --consume [--lance]`. The lake goes straight to R2, with Lance, and raw buckets are deleted as they are used.
3. `areas`: `build_areas.py --full`.
4. `publish`: `objcatalog.py put`, after which the lake is live.

Settings are environment variables read by the script (names and defaults are in its header):
`PLANET_URL`, `THREADS` (the downloader's connections only; the extractor has no thread flag), `BUCKET_BITS`, `GROUP_ROWS`, `WRITER_MEM_MB`, `MEMORY_LIMIT`, `LANCE=0` to skip Lance, `CONSUME=0` to keep the raw buckets, `DATA`.

Watching it: `tail -f /data/logs/{extract,build}.log`; `df -h /data`, `free -g`; `grep MEASURE /data/logs/*.log` gives each stage's time, peak RSS and peak disk.

### Extract by hand

The 164 GB history file never touches the disk: it is piped from OSMF's bucket and its checksum is computed on the way.

```bash
F=planet-full-history/pbf/2026/history-261019.osm.pbf; B=https://osm-planet-eu-central-1.s3.dualstack.eu-central-1.amazonaws.com
```

```bash
curl -sSf $B/$F.md5
```

```bash
SIZE=$(curl -sSfI $B/$F | awk 'tolower($1) == "content-length:" {print $2 + 0}')
```

```bash
cd /data/sunchaser && uv run python -I tools/fetch_parallel.py $B/$F --threads 8 | tee >(md5sum > /data/pbf.md5) | rust/osm-extract/target/release/osm-extract - /data/raw --history --bucket-bits 12 --writer-mem-mb 256 --dense-index /data/idx --input-size $SIZE 2>&1 | tee /data/extract.log
```

- **Progress:** every 30 s the extractor prints the input read, its share and ETA (`--input-size`; a file's size is known without it), the rate, resident, anonymous and peak memory, and the counts so far.
- **Checksum:** compare `/data/pbf.md5` with the published `.md5`. If they differ, or the download breaks, start the extract again.
- **Replication timestamp:** read from the PBF header into `/data/raw/manifest.json` as `osm_base`. Check it's there; if not, pass `--osm-base` (the file's cut-off, 00:00 UTC of the date in its name) to the build.

Then free the timelines: `rm -rf /data/idx`.

### Build by hand

```bash
uv run python src/build_lake.py --raw /data/raw --lake /data/build --catalog /data/catalog.ducklake --data-path $LAKE/data/ --lance $LAKE/lance --group-rows 100000000 --memory-limit 70GB --consume 2>&1 | tee /data/build.log
```

- **Upload:** Parquet goes straight to R2. 1 Gbit is about 400 GB an hour, faster than the build produces it.
- **`--consume`:** deletes each raw bucket once the last table that needs it is built; `build.log` reports the GB freed.
- **Progress:** each table's line ends with the process's peak memory so far, and a table sorted in many groups prints a line at most every 30 s (`lake.X: group 12/40 done`).
- **`--memory-limit`:** DuckDB's cap (default 80% of RAM; RSS ran about 10% above it). The script sets `MEMORY_LIMIT=70GB`.
- **`--group-rows`:** how many rows each sort holds. It did not change Germany's peak memory; the limit does.
- **`--lance`:** optional. It adds 490-550 GB in R2 for queries 1.32× faster (geometric mean) on real R2 with Minnesota, 1.54× in the Berlin spike on a local stand-in; the gain is on id-driven queries (recursion, global tags, `around`), and plain bbox scans gain nothing. Storage ratios: `docs/architecture.md`, Measurements.

Then the areas, and the publish:

```bash
SUNCHASER_CATALOG=/data/catalog.ducklake uv run python src/build_areas.py --full
```

```bash
uv run python src/objcatalog.py put /data/catalog.ducklake $LAKE/catalog.ducklake
```

## 5. Failure handling

| Symptom | What to do |
|---|---|
| `md5 mismatch` | Re-run the script; the extract restarts. |
| Killed (OOM) in `extract` | Re-run with `WRITER_MEM_MB=128` (writer buffers, 8 tables, 256 MB each by default). Anonymous memory was about 11 GB on Germany, and the mapped index is evictable, so an OOM kill is not expected from RSS alone. `THREADS` does not help: it feeds only the downloader. |
| `extract` far slower than the ETA, ways phase | The dense index is not in the page cache (about 25× slower when it does not fit). See the open risk in Status. |
| Killed (OOM) in `build` | Lower `MEMORY_LIMIT` (default `70GB`; Germany at `40GB` peaked at 42.2 GB). Lowering `GROUP_ROWS` does not help at this scale. |
| `build` fails with `CONSUME=1` | The raw buckets are partly gone. The script refuses to continue and says what to remove; the extract must run again. If disk allows, use `CONSUME=0` from the start, so a failed build can be re-run alone. |
| Open-files error | `ulimit -Hn` must allow `(7 << BUCKET_BITS) + 1280`. Raise it in `/etc/security/limits.d/` and log in again, or set `BUCKET_BITS=11`. |
| R2 403 or 400 | Check the env file, and the token's bucket scope. |
| Updater exits "another run holds the lease" | See "If the updater dies" below. |

## 6. Updater

[`deploy/hetzner/sunchaser-updater.service`](../deploy/hetzner/sunchaser-updater.service) runs `updater.py job --loop --catalog-url ${LAKE}/catalog.ducklake --workdir /data/updater --max-seconds 300`. Edit its paths if the repo is not in `/data/sunchaser`, and add `--region s,w,n,e` (the extract's bounding box, from `osmium fileinfo`) for a regional lake, or it applies the whole planet's edits. Install and start:

```bash
cp /data/sunchaser/deploy/hetzner/sunchaser-*.service /etc/systemd/system/ && mkdir -p /data/updater && systemctl daemon-reload
```

```bash
systemctl enable --now sunchaser-updater && journalctl -u sunchaser-updater -f
```

The updater keeps the catalog on local disk and uploads it, with `If-Match`, at most every 120 s (`--upload-every`, the default); it holds the writer lease while running; it compacts every 3600 s (`--compact-every`, the default). It starts by catching up from the history file's date, hourly diffs then minutely; each committed batch logs `job --loop: N diffs to <timestamp>`. A week of backlog takes hours.

- **Not `uv run`:** the unit runs the venv's Python directly. Under `uv run`, SIGTERM stops `uv` and the Python child is killed before its cleanup, so the writer lease is never released and a restart waits out its TTL (measured). The API unit can keep `uv run`: it holds no lease.
- **`HOME`:** systemd units have none, and DuckDB needs it to find its extensions; the units set `Environment=HOME=/root`.
- **Catalog uploads:** each upload is compacted first (`objcatalog.compact`): DuckDB files never shrink, and the updater's churn left Minnesota's at 121 MB with 14 MB live, so the upload is about 13 MB. A catalog straight from `build_areas.py --full` can hold the areas inlined (Germany's: 435 MB, 422 MB of it area geometry, 6 MB once flushed to Parquet); the first hourly compaction flushes them.
- **Compaction in steps:** the hourly compaction (flush, merge, rewrite, expire, clean up, one Lance table at a time) runs one step per cycle, so diffs keep flowing. A one-shot `job` (no `--loop`) runs all the steps at once.
- **Stopping:** SIGTERM makes the updater finish the diff in hand, upload what is pending, release the lease and exit, so `systemctl restart` loses nothing. A long step (a Lance merge) delays that; `TimeoutStopSec=600` is the backstop.
- **Fixed-window tests:** `--until T` stops before the first diff ending after T.

**If the updater dies:** the lease expires after `--max-seconds` + 600 s and the restart takes it over. A normal `systemctl restart` or `stop` (SIGTERM) releases the lease, so the new process takes over at once (about 15 s measured). If it is killed instead (SIGKILL, power loss, or a stop that outlasts `TimeoutStopSec`), the new one exits ("another run holds the lease") and systemd retries every 30 s until the lease expires, up to about 15 minutes; the lake just falls behind for that time. If the local catalog is ever lost, the next start downloads the committed one.

What the updater does each batch, checked on Minnesota: it copies the outgoing version of every modified or deleted element into `nodes_history` / `ways_history` (sampled: 2,000 of 2,000 modified nodes and ways, and all 2,000 deleted nodes), then commits. Hourly it flushes inlined rows to Parquet, merges small files, expires snapshots and deletes unreferenced files after a 1-hour grace period (every file in R2 was either live, superseded, or scheduled for deletion; none orphaned).

## 7. API

[`deploy/hetzner/sunchaser-api.service`](../deploy/hetzner/sunchaser-api.service) runs the read-only server on `127.0.0.1:8080` with `SUNCHASER_CATALOG=$LAKE/catalog.ducklake`. The server downloads the catalog to a local file and attaches that, re-fetching when the object's ETag changes (`SUNCHASER_CATALOG_REFRESH` seconds, default 30). Run it on the same box, or on a small VPS; any host can read R2 without egress charges.

```bash
systemctl enable --now sunchaser-api
```

Expose it only after asking, with Caddy for TLS (`apt-get install caddy`; [`deploy/hetzner/Caddyfile.example`](../deploy/hetzner/Caddyfile.example), with a DNS name pointing at the box) or an SSH tunnel for testing. Point Overpass Turbo's server setting at `https://<host>/api/`.

**Smoke tests**, through the API (`curl -s localhost:8080/api/interpreter --data-urlencode 'data=...'`). These use Berlin; on a regional lake other than Berlin, use a bbox inside the region, since a Berlin box returns nothing there.

```
[out:json];node[amenity=cafe](52.51,13.38,52.52,13.40);out count;
[out:json][date:"2015-01-01T00:00:00Z"];node[amenity=cafe](52.51,13.38,52.52,13.40);out count;
[out:json];area[name="Berlin"][admin_level=4];node(area)[amenity=townhall];out ids;
```

Also check that `curl -s localhost:8080/api/timestamp` advances as the updater commits.

**Slow queries:** set `SUNCHASER_LOG_QUERIES=1` in the unit's `Environment=` for one line per query (total time, setup time, KiB sent, outcome, the query), or `2` to add each SQL statement's time and the time spent fetching results and writing output. On Minnesota (lake in R2, box in Helsinki) a 25-element library query took 2.8 s warm and 11 s cold, almost all of it in fetching results: setup was 0.02 s and the SQL statements under 1 s. `SUNCHASER_MAX_QUERIES`, `SUNCHASER_MAX_OUTPUT`, `SUNCHASER_MAX_TIMEOUT` bound one server, and `SUNCHASER_MAX_READ_MB` refuses a query whose estimated read is too large.

**Moving off the big box after the build:** a Hetzner Cloud VPS is enough for the updater and a modest API. Run both services there with the same `/etc/sunchaser.env` (fresh `--workdir`), then cancel the auction server.

## Laptop variant

The build runs the same on a laptop: macOS or Linux, 48 GB of RAM is enough. The differences:
- **Disk:** about 1.5 TB free for full history (the extract peaks at 1.28-1.34 TB, and the local lake grows to 1.1-1.2 TB while `--consume` frees the raw buckets), so a 2 TB external NVMe SSD over USB 3.2 or Thunderbolt. The built-in 1 TB is enough only for the current planet.
- **Open files:** raise the limit before extracting: `ulimit -n 65536` (on macOS also `sudo launchctl limit maxfiles 65536 200000`). Use `--bucket-bits 10` if the limit can't go that high.
- **Sleep:** keep the machine awake: `caffeinate -i` on macOS, `systemd-inhibit` on Linux.
- **Memory:** pass `--memory-limit` a few GB under the RAM you can spare (for example `32GB` on 48 GB; not measured on a laptop). The same open risk about the dense index applies, more so.
- **Build to local disk, then upload:** home uplinks are slow, so don't write to R2 during the build. Build with local paths, copy with rclone, then point the catalog at R2:

  ```bash
  uv run python src/build_lake.py --raw /ssd/raw --lake /ssd/build --catalog /ssd/catalog.ducklake --data-path /ssd/lake/data/ --lance /ssd/lake/lance --group-rows 100000000 --memory-limit 32GB --consume
  ```

  ```bash
  rclone copy /ssd/lake r2:osm-lake/lake --transfers 32 --s3-upload-concurrency 8 --progress
  ```

  ```bash
  uv run python tools/relocate_lake.py --catalog /ssd/catalog.ducklake --data-path $LAKE/data/ --lance $LAKE/lance
  ```

  Then run `build_areas.py --full` with `SUNCHASER_CATALOG=/ssd/catalog.ducklake`, and upload the catalog with `objcatalog.py put` as in section 4. Uploading 1.1-1.2 TB (with Lance) takes about 25-27 h at 100 Mbit/s and about 3 h at 1 Gbit/s.
- **Updater and API:** run them on an always-on host (sections 6-7 on a VPS), not on the laptop.

## The same on S3

The lake works unchanged on AWS S3 (the OSMF planet and replication buckets are in eu-central-1, so use that region). Differences from R2:
- **Credentials:** use the AWS credential chain instead of keys: `SUNCHASER_S3_CREDENTIAL_CHAIN=true` (an instance or task role needs read/write on the bucket and read on `osm-planet-eu-central-1`).
- **Region:** `SUNCHASER_S3_REGION=eu-central-1`, and leave `SUNCHASER_S3_ENDPOINT` unset.
- **URL style:** R2 needs `path` (`SUNCHASER_S3_URL_STYLE=path` in the env file). With the credential chain `SUNCHASER_S3_URL_STYLE` is not read and DuckDB's default, virtual-hosted style, applies, which is what S3 wants.

S3 supports the conditional PUTs the catalog needs. S3 egress is charged (about $0.09/GB), unlike R2.

## Appendix: planet sizing

How the numbers were measured: a one-off pass in October 2026 on a 4-core, 15 GB container, with scripts that have since been deleted (`sizing.sh`, `size_planet.py`, the extractor's `--cut`); the results stay under `results/sizing/`, and `results/sizing/estimate.md` is the generated estimate and the source of the tables below.

1. **One streamed pass over the real history planet** (`history-261019.osm.pbf`, 164 GB), from OSMF's bucket into the extractor, never stored. It counted the whole planet exactly (`planet_stats.json`) and cut full-history extracts of three regions with different mixes.
2. **Per region:** the lake (with Lance) and its areas were built locally under `tools/measure.py`, and each table was scaled by the planet total it depends on (node, way or relation versions). The range in each estimate is the spread across the three regions.

| Region | Box | Versions | Share of the planet's versions |
|---|---|---|---|
| Luxembourg | 49.44-50.19 N, 5.73-6.53 E | 17.1 M | 0.09% |
| Berlin | 52.33-52.68 N, 13.08-13.77 E | 27.9 M | 0.15% |
| Swiss plateau | 46.9-47.6 N, 7.0-9.0 E | 55.1 M | 0.29% |

**The planet, exactly** (history file of 2026-10-19):

| | Versions | Deleted versions | Ids ever used | Max id |
|---|---|---|---|---|
| Nodes | 16,657,902,975 | 1,522,378,172 | 12,453,707,759 | 14,298,154,526 |
| Ways | 2,471,108,843 | 142,010,722 | 1,366,233,306 | 1,569,574,897 |
| Relations | 67,784,271 | 3,819,482 | 18,740,003 | 21,713,761 |

There are 15.9 B node-position records (first positions, moves and deletions) and 36.5 B way node references across all way versions. The pass took 84 min (the log records 5,042 s) and peaked at 11.6 GB RSS; the download averaged about 33 MB/s over 8 range connections, and the extractor set the pace.

**Disk and storage estimate:**

| Planet, full history | Estimate |
|---|---|
| Raw extraction (local disk) | 800-850 GB |
| Node position timeline (`--dense-index`): 8 B per id + 16 B per position record | 370 GB |
| Way geometry timeline | 115-120 GB |
| **Peak local disk during the extract** (the PBF is streamed, not stored) | **1.28-1.34 TB** |
| Lake in object storage (Parquet), of which: | **610-620 GB** |
| - history and change log (`*_history`, `changes`) | 350-380 GB |
| - current tables, indexes, metadata | 240-265 GB |
| Lance id store (current nodes and ways) | 490-550 GB |

Lance is about 2× the current Parquet tables it copies and 0.85× the whole full-history lake; the ratios are explained once in `docs/architecture.md`, Measurements. A 2 x 1 TB RAID0 box has about 30% headroom at the extract's peak.

**Time and memory estimate** (scaled from the regions on 4 cores; for the planet on this box, use the Germany rehearsal above, about 9 h each):

| Step | Measured on the regions (4 cores) | Planet, 4 cores | Planet, 6-core i7-8700 (est.) |
|---|---|---|---|
| Extract: history processing | 136-316 s; 126-175 k versions/s | 31-43 h | 20-30 h |
| Lake build (with Lance) | 89-246 s; peak RSS 7-12 GB | 24-39 h | 15-25 h |
| Areas (full) | 2-10 s | under 1 h | under 1 h |

- **Scaling assumptions:** linear in versions; the core-count column assumes near-linear scaling with cores.
- **Extract:** decoding is not the bottleneck (the cut pass decoded the whole planet in 84 min on 4 cores); history processing is. It runs per id group, in parallel, and is CPU-bound.
- **Build:** mostly sorting and Parquet encoding. The two relation tables are sorted single-threaded: a parallel sorted insert into DuckLake ran out of memory on Berlin's relation history (490 k versions carrying 303 M member entries), while one thread spills to disk and finishes. Relation history is small in rows but large in members: about 8.1 B member entries across the planet's relation versions.
- **Memory:** the extractor's writer buffers are bounded (mimalloc); DuckDB uses up to its memory limit and spills the rest to local disk. See Status for measured peaks.

**Updates at planet scale:** a planet hour applied in 11-22 s on 4 cores against a small lake. The cost against planet-sized tables still needs measuring on the real lake (delete lookups, compaction time, catalog size). Each batch rebuilds only the areas whose relations changed, and replaced versions move to `*_history`, so attic queries cover everything since 2005.

**Serving:** API servers are stateless and need only the bucket. The hot set is Parquet footers and the row groups of popular areas; DuckDB's external file cache, and a local NVMe cache in front of R2, cut first-byte latency.

**Open items for planet scale:** the extract's dense-index risk (Status); update cost against the real planet lake; query latency and the Lance gain against real R2 (in progress); antimeridian-crossing boxes.
