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
3. **Lifecycle rules:** expire `lake/catalogs/` after 30 days (per-commit catalog copies, kept for rollback), and abort incomplete multipart uploads after 1 day.

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
2. **Install Ubuntu 24.04 with RAID0.** Hetzner's `installimage` defaults to RAID1, which halves the space. In the rescue system run `installimage`, choose Ubuntu 24.04, and set:

   ```
   SWRAID 1
   SWRAIDLEVEL 0
   PART /boot ext3 1G
   PART /     ext4 100G
   PART /data xfs  all
   ```

   The build doesn't need redundancy: if a disk dies, rebuild. The lake lives in R2.

3. **Packages and tools**, as root after reboot:

   ```bash
   apt-get update && apt-get install -y build-essential git curl osmium-tool
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

   ```bash
   cargo build --release --manifest-path rust/osm-extract/Cargo.toml
   ```

   ```bash
   set -a && . /etc/sunchaser.env && set +a && ulimit -n 65536
   ```

Run the long steps inside `tmux` so an SSH drop doesn't kill them.

## 3. Sizing pass (optional, about 1.5 h)

`deploy/hetzner/sizing.sh /data/sizing` streams the planet once, and cuts and builds three regional history lakes under `tools/measure.py`. It then writes a planet estimate. A run from a 4-core container is committed in `results/sizing/`. Re-run it on the box to see its own speed: the times depend on the box, the sizes don't.

## 4. Extract, streaming the planet (≈ 20–30 h on 6 cores)

The 164 GB history file never touches the disk: it is piped from OSMF's bucket, and its checksum is computed on the way.

```bash
F=planet-full-history/pbf/2026/history-261019.osm.pbf
```

```bash
curl -sSf https://osm-planet-eu-central-1.s3.dualstack.eu-central-1.amazonaws.com/$F.md5
```

```bash
curl -sSf https://osm-planet-eu-central-1.s3.dualstack.eu-central-1.amazonaws.com/$F | tee >(md5sum > /data/pbf.md5) | rust/osm-extract/target/release/osm-extract - /data/raw --history --bucket-bits 12 --dense-index /data/idx 2>&1 | tee /data/extract.log
```

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
WorkingDirectory=/data/sunchaser
ExecStart=/root/.local/bin/uv run python src/updater.py job --loop --catalog-url ${LAKE}/catalog.ducklake --workdir /data/updater --max-seconds 300 --compact-every 3600 --keep-versions
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
```

```bash
mkdir -p /data/updater && systemctl daemon-reload && systemctl enable --now sunchaser-updater
```

```bash
journalctl -u sunchaser-updater -f
```

**If the updater dies:** the lease expires after `--max-seconds` + 600 s and the restart takes it over. If the local catalog is ever lost, the next start downloads the committed one.

## 7. API

A read-only server: it attaches `$LAKE/catalog.ducklake` and reloads it when its ETag changes. Run it on the same box, or on a small VPS. Any host can read R2 without egress charges.

`/etc/systemd/system/sunchaser-api.service`:

```
[Unit]
Description=sunchaser API (read-only)
After=network-online.target

[Service]
EnvironmentFile=/etc/sunchaser.env
Environment=SUNCHASER_CATALOG_REFRESH=30 SUNCHASER_PORT=8080 SUNCHASER_HOST=127.0.0.1
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
