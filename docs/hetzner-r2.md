# Planet build on Hetzner (or a laptop) with Cloudflare R2

The same lake as `docs/aws.md`, but with storage on **Cloudflare R2**, which has no egress fees,
and compute on a **Hetzner dedicated server** with unmetered traffic, or your own machine. Query
results, updater reads and API reads then cost nothing per GB, wherever the compute runs.

**Status.** Every command here has run on the Berlin extract against an S3-compatible stand-in:
- streamed extract from stdin, `--consume` build, `relocate_lake.py`;
- `updater.py job --loop` with the lease and conditional uploads, and the Lance store.

Results matched a real Overpass with attic data.

Planet sizes come from a **measured sizing run** (`docs/planet.md`, `results/sizing/`): exact
planet totals, plus three regional full-history lakes scaled up. Times are still scaled from
4 cores (marked ≈); write the real ones back here after the first run.

**Rehearsals on the box** (Hetzner auction server, 12 threads, 125 GB RAM, 2 × 954 GB NVMe as RAID0, Ubuntu 24.04), local disk, no `--dense-index`, `--group-rows 100000000`:

| | Minnesota | Germany | Ratio |
|---|---|---|---|
| History PBF (Geofabrik) | 0.49 GB | 12.4 GB | 25× |
| `osm-extract --history` | 1 min 43 s | 41 min 52 s | 24× |
| `build_lake.py --consume` | 1 min 46 s | 40 min 56 s | 23× |
| `build_areas.py --full` | 5 s | 21 s | n/a |
| Lake on disk (Parquet + Lance) | 3.3 GB | 57 GB (37 + 20) | 17× |
| Peak extra disk during the run | not measured | 64 GB (94 GB used, 30 GB before) | n/a |
| Peak RSS, extract / build | not measured | 67.5 GB / 109.8 GB | n/a |

Times scale linearly with the input size. Germany's build peaked at 110 GB of the box's 125 GB RAM with `--group-rows 100000000`, which leaves little headroom. A lower `--group-rows` does reduce it: on Minnesota the build peaked at 6.6 GB with 100 M and 3.8 GB with 2 M (one group per table versus 3–5), at the cost of more sorts. Germany with a lower value has not been run.

Minnesota was then published to R2 (the lake copied in 4 min 39 s) and kept current by the updater service: one batch of 14–30 minutely diffs takes 340–570 s, about 12 s per diff with areas and Lance maintenance, which is faster than real time.

Cross-check against the sizing run: scaling Germany's 57 GB lake by the ratio of PBF sizes (13×) gives ≈ 750 GB, below the sizing run's 1.1–1.2 TB for the planet, so lake size apparently does not scale linearly with the history file; plan with the sizing run's figure. Times did scale linearly: ≈ 9 h for the extract and ≈ 9 h for the build on this box's 12 threads, against the sizing run's 20–30 h and 15–25 h scaled from 4 cores. Not measured on either side: whether build RAM stays bounded as the input grows (Germany used 110 GB of 125 GB), and how a planet-wide updater keeps up: a planet lake applies every edit (≈ 5,000 changes per minute diff worldwide), where Minnesota's regional filter applies a few dozen. Run the updater on Germany before trusting it on the planet.

## What goes where

```
 OSMF history planet (S3, Frankfurt) ──curl──▶ osm-extract (stdin) ──▶ build ──▶ R2 bucket
                                                                                 lake/catalog.ducklake
 OSMF replication diffs ──▶ updater (job --loop on the box)  ──conditional PUT──▶ lake/data/, lake/lance/
 Overpass Turbo / clients ──HTTPS──▶ API (server.py, read-only) ◀── range reads ──┘
```

| | Cost |
|---|---|
| R2 storage | $0.015/GB-month (Standard): about $17/month for the full-history lake with Lance (610–620 GB Parquet + 490–550 GB Lance), about $9 without Lance. No egress fees; reads $0.36 per million, writes $4.50 per million |
| Hetzner auction box | Billed hourly (e.g. $0.24/h for an i7-8700 with 128 GB and 2 × 1 TB NVMe), unmetered 1 Gbit. Build for about 2 days (≈ $12), then keep it, or move the updater and API to a small VPS |
| Downloading the history planet | Free (OSMF publishes it) |

## Disk: will the history build fit?

Full-history planet, peak local disk (from the sizing run, `docs/planet.md`):

| | Plain | Streamed input + `--consume` |
|---|---|---|
| History PBF | 164 GB | 0 (piped from the download) |
| Timelines (`--dense-index`) | 370 GB nodes + 115–120 GB ways | the same, deleted before the build |
| Raw buckets (current + history + changes) | 800–850 GB | the same, shrinking as the build consumes them |
| DuckDB sort spill | small (regional builds stayed under 2× their lake size) | the same |
| **Peak** | **≈ 1.45–1.5 TB** | **1.28–1.34 TB**, during the extract |

- **2 × 1 TB, as RAID0 (about 1.9 TB):** fits, with about 30% headroom when streaming.
- **A 1 TB laptop:** does not fit full history. A current-only planet (`osm-extract` without `--history`) fits.

## 1. R2

1. **Bucket:** in the Cloudflare dashboard, R2 → Create bucket `osm-lake`. Set the location hint to **Eastern Europe (EEUR)** for a Hetzner box in Helsinki, or to wherever the compute runs.
2. **Token:** R2 → Manage API tokens → create a token with **Object Read & Write** on that bucket only. Note the access key id, the secret, and the account id.
3. **Lifecycle rules:** abort incomplete multipart uploads after 1 day. (The updater's `--keep-versions N` prunes its own `lake/catalogs/` copies; see section 6.)

R2 supports the conditional PUTs (`If-Match`, `If-None-Match`) that the catalog and the writer lease rely on.

Environment file used by every step, `/etc/sunchaser.env`:

```
SUNCHASER_S3_ENDPOINT=<ACCOUNT_ID>.r2.cloudflarestorage.com
SUNCHASER_S3_KEY_ID=<access key id>
SUNCHASER_S3_SECRET=<secret>
SUNCHASER_S3_REGION=auto
SUNCHASER_S3_URL_STYLE=path
SUNCHASER_S3_USE_SSL=true
LAKE=s3://osm-lake/lake
```

## 2. The Hetzner box

1. **Order it** from the server auction, which bills hourly. Choose at least 64 GB RAM (128 GB is better: the node timeline index is ≈ 110 GB and benefits from page cache), NVMe disks, and a recent CPU if you can; core count is the build's speed. Order it with the rescue system.
2. **Install Ubuntu 24.04 with RAID0.** Hetzner's `installimage` defaults to RAID1, which halves the space. In the rescue system, write the config to `/root/sunchaser.conf` (not `/autosetup`: a failed run deletes it):

   ```
   DRIVE1 /dev/nvme0n1
   DRIVE2 /dev/nvme1n1
   SWRAID 1
   SWRAIDLEVEL 0
   HOSTNAME sunchaser
   BOOTLOADER grub
   PART /boot ext3 1G
   PART /     ext4 100G
   PART /data xfs  all
   IMAGE /root/.oldroot/nfs/images/Ubuntu-2404-noble-amd64-base.tar.zst
   ```

   `installimage` isn't on the rescue system's `PATH`, and the image is a `.tar.zst`. Run it by its full path, then reboot:

   ```bash
   TERM=xterm /root/.oldroot/nfs/install/installimage -a -c /root/sunchaser.conf
   ```

   The installed system has new SSH host keys: run `ssh-keygen -R <ip>` on your side before reconnecting. `/boot` becomes RAID1 whatever `SWRAIDLEVEL` says; that's fine.

   The build doesn't need redundancy: if a disk dies, rebuild. The lake lives in R2.

3. **Packages and tools**, as root after reboot:

   ```bash
   apt-get update && apt-get install -y build-essential git curl osmium-tool tmux rclone
   ```

   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh && . ~/.local/bin/env
   ```

   ```bash
   curl -sSf https://sh.rustup.rs | sh -s -- -y && . ~/.cargo/env
   ```

   ```bash
   cd /data && git clone https://github.com/iandees/sunchaser.git && cd sunchaser
   ```

   ```bash
   uv sync --locked
   ```

   DuckDB's `ducklake`, `httpfs` and `spatial` extensions are installed on first use (`lakeconn.load`), so the first build or API start on a fresh box downloads them.

   ```bash
   cargo build --release --manifest-path rust/osm-extract/Cargo.toml
   ```

   ```bash
   set -a && . /etc/sunchaser.env && set +a && ulimit -n 65536
   ```

Run the long steps inside `tmux` so an SSH drop doesn't kill them.

## 3. Sizing pass (optional, about 1.5 h)

`deploy/hetzner/sizing.sh /data/sizing` streams the planet once, and cuts and builds three regional history lakes under `tools/measure.py`. It then writes a planet estimate. A run from a 4-core container is committed in `results/sizing/`. Re-run it on the box to see its own speed: the times depend on the box, the sizes don't.

### Regional rehearsal (optional)

Geofabrik offers full-history country extracts (`.osh.pbf`) to logged-in OSM users, at `https://osm-internal.download.geofabrik.de/<region>-internal.osh.pbf` (for example `north-america/us/minnesota-internal.osh.pbf`). Log in at that site in a browser and copy the `gf_download_oauth` cookie value; pass it to `curl -H "Cookie: gf_download_oauth=\"...\""`. Treat it as a credential: keep it in a mode-600 file, not in the repo or a shell history.

The header of these files has no replication timestamp, and the build stops with an error unless you pass `--osm-base` (without it the updater would have nowhere to start). Take the newest object's time (`osmium fileinfo -e -g data.timestamp.last FILE`) and round up to the next hour boundary (Geofabrik cuts its extracts at 00:00 UTC); both rehearsals used `--osm-base 2026-10-19T00:00:00Z` style values.

Run steps 4–6 on a country without `--dense-index` to time the box itself, then compare the extract and build times, the peak disk use (`df` during the extract) and the lake size with the table at the top.

## 4. Extract, streaming the planet (≈ 20–30 h on 6 cores)

The 164 GB history file never touches the disk: it is piped from OSMF's bucket, and its checksum is computed on the way.

```bash
F=planet-full-history/pbf/2026/history-261019.osm.pbf
```

```bash
curl -sSf https://osm-planet-eu-central-1.s3.dualstack.eu-central-1.amazonaws.com/$F.md5
```

```bash
SIZE=$(curl -sSfI https://osm-planet-eu-central-1.s3.dualstack.eu-central-1.amazonaws.com/$F | awk 'tolower($1) == "content-length:" {print $2 + 0}')
```

```bash
curl -sSf https://osm-planet-eu-central-1.s3.dualstack.eu-central-1.amazonaws.com/$F | tee >(md5sum > /data/pbf.md5) | rust/osm-extract/target/release/osm-extract - /data/raw --history --bucket-bits 12 --dense-index /data/idx --input-size $SIZE 2>&1 | tee /data/extract.log
```

- **Progress:** every 30 s the extractor prints a line with the input read, its share and ETA (`--input-size`; a file's size is known without it), the rate, resident and peak memory, and the counts so far.

- **Checksum:** compare `/data/pbf.md5` with the published `.md5`. If they differ, or the download breaks, start the extract again.
- **Disk headroom:** if there is room, downloading first (`curl -o`) makes a restart cheaper.
- **Replication timestamp:** read from the PBF header into `/data/raw/manifest.json` as `osm_base`. Check it's there; if not, pass `--osm-base` (the file's cut-off, 00:00 UTC of the date in its name) to the build.

Then free the timelines:

```bash
rm -rf /data/idx
```

## 5. Build into R2 (≈ 15–25 h on 6 cores)

```bash
uv run python src/build_lake.py --raw /data/raw --lake /data/build --catalog /data/catalog.ducklake --data-path $LAKE/data/ --lance $LAKE/lance --group-rows 100000000 --consume 2>&1 | tee /data/build.log
```

- **Upload:** Parquet goes straight to R2. 1 Gbit is about 400 GB an hour, faster than the build produces it.
- **`--consume`:** deletes each raw bucket once the last table that needs it is built; `build.log` reports the GB freed.
- **Progress:** each table's line ends with the process's peak memory so far, and a table sorted in many groups prints a line at most every 30 s (`lake.X: group 12/40 done`). The log is line-buffered, so `| tee build.log` shows it live.
- **`--group-rows`:** sets how many rows each sort holds. 100 M suits 64–128 GB of RAM; use 50 M with 48 GB.
- **`--lance`:** optional. It adds 490–550 GB in R2 (about 2× the current tables it copies) for queries 1.54× faster, mostly recursion and id lookups.

Then build the areas in full:

```bash
SUNCHASER_CATALOG=/data/catalog.ducklake uv run python src/build_areas.py --full
```

## 6. Publish and start the updater

Upload the catalog. From this moment the lake is live:

```bash
uv run python src/objcatalog.py put /data/catalog.ducklake $LAKE/catalog.ducklake
```

Run the updater as a service. `--loop` keeps the catalog on local disk and uploads it, with `If-Match`, after each batch, so it never re-downloads it. It holds the writer lease while running, and starts by catching up from the history file's date, hourly then minutely.

`/etc/systemd/system/sunchaser-updater.service`:

```
[Unit]
Description=sunchaser updater (only writer)
After=network-online.target

[Service]
EnvironmentFile=/etc/sunchaser.env
Environment=HOME=/root
LimitNOFILE=65536
WorkingDirectory=/data/sunchaser
ExecStart=/data/sunchaser/.venv/bin/python src/updater.py job --loop --catalog-url ${LAKE}/catalog.ducklake --workdir /data/updater --max-seconds 300 --compact-every 3600 --upload-every 120
Restart=always
RestartSec=30
TimeoutStopSec=600

[Install]
WantedBy=multi-user.target
```

```bash
mkdir -p /data/updater && systemctl daemon-reload && systemctl enable --now sunchaser-updater
```

```bash
journalctl -u sunchaser-updater -f
```

- **Not `uv run`:** the unit runs the venv's Python directly. Under `uv run`, SIGTERM stops `uv` and the Python child is killed before its cleanup, so the writer lease is never released and a restart waits out its TTL (measured). The API unit can keep `uv run`: it holds no lease.
- **Catalog uploads:** each commit uploads the catalog once, compacted first (`objcatalog.compact`): DuckDB files never shrink, and the updater's churn left Minnesota's at 121 MB with 14 MB live, so the upload is 13.6 MB. At the live edge the updater commits once per minute-diff (≈ 20 per hour), so this is what keeps the uploads small. A catalog straight from `build_areas.py --full` can hold the areas inlined (Germany's: 435 MB, 422 MB of it area geometry, 6 MB once flushed to Parquet and rewritten); the updater's first hourly compaction flushes them, and its commits after that are small.
- **`HOME`:** systemd units have none, and DuckDB needs it to find its extensions; without `Environment=HOME=/root` the service exits at once with "Can't find the home directory".
- **`--upload-every N`:** upload the catalog at most every N seconds (default 0: after every batch). The updater keeps applying diffs in between; a crash loses only the batches since the last upload, and the next start applies them again. Readers see the lake at most N seconds (plus one poll) late. 120 here, with the 13.6 MB compacted catalog.
- **Compaction in steps:** the hourly compaction (flush, merge, rewrite, expire, clean up, one Lance table at a time) runs one step per cycle, so diffs keep flowing between steps instead of waiting for the whole thing. A one-shot `job` (no `--loop`) still runs all the steps at once.
- **Stopping:** SIGTERM makes the updater finish the diff in hand, upload what is pending, release the lease and exit, so `systemctl restart` loses nothing. A long step (a Lance merge) delays that; `TimeoutStopSec=600` is the backstop.
- **`--keep-versions [N]`:** copies the whole catalog to `lake/catalogs/` after every commit (≈ 117 MiB each for Minnesota, a copy every 6 minutes) and keeps only the newest N (default 24). Without a limit that was ≈ 28 GB a day, so keep N small, and larger lakes have larger catalogs. Off in the sample unit.
- **`--region`:** for a regional lake add `--region s,w,n,e` (the extract's bounding box, from `osmium fileinfo`), or the updater applies the whole planet's edits.

**If the updater dies:** the lease expires after `--max-seconds` + 600 s and the restart takes it over. A normal `systemctl restart` or `stop` (SIGTERM) releases the lease, so the new process takes over at once (≈ 15 s measured). If it is killed instead (SIGKILL, power loss, or a stop that outlasts `TimeoutStopSec`), the new one exits ("another run holds the lease") and systemd retries every 30 s until the lease expires, up to ≈ 15 minutes with the default; the lake just falls behind for that time. The unit sets `TimeoutStopSec=600` so a stop during a long DuckDB call (a compaction) isn't cut short. If the local catalog is ever lost, the next start downloads the committed one.

What the updater does each batch, checked on Minnesota: it copies the outgoing version of every modified or deleted element into `nodes_history` / `ways_history` (sampled: 2,000 of 2,000 modified nodes and ways, and all 2,000 deleted nodes), then commits. Hourly it flushes inlined rows to Parquet, merges small files, expires snapshots and deletes unreferenced files after a 1-hour grace period (every file in R2 was either live, superseded, or scheduled for deletion; none orphaned).

## Drift check (periodic, off the live box)

Nothing else reconciles a lake rolled forward by the updater with a fresh build of the same moment, and a short replay can't show bugs that only appear after weeks of cascades, compactions and restarts. `tools/drift_check.py` does: it attaches two catalogs read-only and compares them table by table (row counts, then keys only in one side, then keys whose row differs, with the first `--limit` keys printed).

1. Take a frozen copy of the live catalog from `catalogs/` (`--keep-versions`) or the published one, and read its `state.sequence`/`osm_base`. That is lake A.
2. Build lake B from the planet (or the region's extract) as in steps 4–5, into a scratch bucket prefix or local disk, then replay minutely diffs with `updater.py run --once --until <A's osm_base>` so B ends at A's moment. The check refuses (exit 2) if `osm_base`/`sequence` differ. Build B from a snapshot at least as old as the last check, so the replay is short. Replaying B with the same updater only tests the updater against its own history of cascades from the build onward; B must come from a source the updater did not touch, for the check to say anything about A's older batches.
3. `uv run python tools/drift_check.py A.ducklake B.ducklake --data-path-a ... --data-path-b ...` (or `s3://` catalogs with the `SUNCHASER_S3_*` variables). Exit 0: no drift; 1: drift; 2: not the same moment. Add `--history` only if B came from a `--history` extract.

Excluded because they legitimately differ (full list in the script's docstring): `valid_from` on current rows (version time in a snapshot build, change time after an update), the history tables and `changes` (a snapshot build has none), `state.batch`/`compacted_at`/`areas_batch`, the Lance store, file layout.

Cost: B is a full build plus replay (the 15–25 h build in step 5 plus the extract, on a box that is not the live one), and the comparison is a full scan of both lakes (hash-joins on the biggest tables: `tag_postings`, `nodes_meta`). At planet scale run it in slices (`--shard 0/16` ... `15/16`, rows with `hash(key) % 16 == I`) with `SUNCHASER_DRIFT_TEMP` on a large disk. Run it quarterly, or after any updater change that touches the cascade, `_rewrite`, or compaction; a regional lake (a country) makes it a cheap weekly job.

What it can't catch: bugs the build shares with the updater (both use the same SQL macros and `quadtile.py`, so a wrong index key is wrong on both sides); history tables, `changes` and `valid_from` on current rows (no snapshot reference); the Lance store; differences hidden by element versions that an extract and the replication feed disagree about (e.g. the extract is cut at a different instant than `osm_base`); and anything in a regional lake that the `--region` filter drops differently from a fresh extract. A clean run is evidence about the current-state tables, not about the lake as a whole. `tests/test_drift_check.py` shows the recipe on a five-element fixture: identical lakes pass, a corrupted row and a deleted index row fail.

## 7. API

A read-only server: it attaches `$LAKE/catalog.ducklake` and reloads it when its ETag changes. Run it on the same box, or on a small VPS. Any host can read R2 without egress charges.

`/etc/systemd/system/sunchaser-api.service`:

```
[Unit]
Description=sunchaser API (read-only)
After=network-online.target

[Service]
EnvironmentFile=/etc/sunchaser.env
Environment=HOME=/root SUNCHASER_CATALOG_REFRESH=30 SUNCHASER_PORT=8080 SUNCHASER_HOST=127.0.0.1
WorkingDirectory=/data/sunchaser
ExecStart=/bin/sh -c 'SUNCHASER_CATALOG=$LAKE/catalog.ducklake exec /root/.local/bin/uv run python src/server.py'
Restart=always

[Install]
WantedBy=multi-user.target
```

Put Caddy in front for TLS (`apt-get install caddy`). The whole `/etc/caddy/Caddyfile`:

```
overpass.example.org {
    reverse_proxy 127.0.0.1:8080
}
```

Point Overpass Turbo at `https://overpass.example.org/api/`.

**Slow queries:** set `SUNCHASER_LOG_QUERIES=1` in the unit's `Environment=` for one line per query (total time, setup time, KiB sent, outcome, the query), or `2` to add each SQL statement's time and the time spent fetching results and writing output. On Minnesota (lake in R2, box in Helsinki) a 25-element library query took 2.8 s warm and 11 s cold, almost all of it in that last bucket: setup was 0.02 s and the SQL statements under 1 s.

**Moving off the big box after the build:** a Hetzner Cloud VPS is enough for the updater and a modest API. Run both services there with the same `/etc/sunchaser.env` (fresh `--workdir`), then cancel the auction server.

## Laptop variant

The build runs the same on a laptop: macOS or Linux, 48 GB of RAM is enough. The differences:
- **Disk:** about 1.5 TB free for full history (the extract peaks at 1.28–1.34 TB, and the local lake grows to 1.1–1.2 TB while `--consume` frees the raw buckets), so a 2 TB external NVMe SSD over USB 3.2 or Thunderbolt. The built-in 1 TB is enough only for the current planet.
- **Open files:** raise the limit before extracting: `ulimit -n 65536` (on macOS also `sudo launchctl limit maxfiles 65536 200000`). Use `--bucket-bits 10` if the limit can't go that high.
- **Sleep:** keep the machine awake: `caffeinate -i` on macOS, `systemd-inhibit` on Linux.
- **Sort size:** `--group-rows 50000000`.
- **Build to local disk, then upload:** home uplinks are slow, so don't write to R2 during the build. Build with local paths, copy with rclone, then point the catalog at R2:

  ```bash
  uv run python src/build_lake.py --raw /ssd/raw --lake /ssd/build --catalog /ssd/catalog.ducklake --data-path /ssd/lake/data/ --lance /ssd/lake/lance --group-rows 50000000 --consume
  ```

  ```bash
  rclone copy /ssd/lake r2:osm-lake/lake --transfers 32 --s3-upload-concurrency 8 --progress
  ```

  ```bash
  uv run python tools/relocate_lake.py --catalog /ssd/catalog.ducklake --data-path $LAKE/data/ --lance $LAKE/lance
  ```

  Then run `build_areas.py --full` with `SUNCHASER_CATALOG=/ssd/catalog.ducklake`, and upload the catalog with `objcatalog.py put` as in step 6. Uploading 1.1–1.2 TB (with Lance) takes ≈ 25–27 h at 100 Mbit/s and ≈ 3 h at 1 Gbit/s.
- **Updater and API:** run them on an always-on host (step 6–7 on a VPS), not on the laptop.
