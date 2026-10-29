# Planet build with full history on AWS

> Egress from AWS costs about $0.09/GB for query results and for any reads from outside AWS. For
> storage without egress fees and flat-rate compute, see [`hetzner-r2.md`](hetzner-r2.md).

This runbook builds a planet lake with full history (`[date:]`, `newer` and `changed` back to the first
OSM edits), keeps it current with minutely diffs, and serves it, with nothing running between
updater runs and requests.

**Status.** Every step below has run on a Berlin extract on a 4-core box, with the same commands
and code paths: the full-history extract and build, the object-storage catalog, scheduled updater
jobs, the Lance store and incremental areas. They matched a real Overpass with attic data on 81 of
82 queries, including 13 of 13 history queries. The planet run itself hasn't happened yet:
**sizes and times are extrapolations** (marked ≈), and the first real run should record the actual
numbers here.

## Overview

```
 s3://osm-planet-eu-central-1/planet-full-history/pbf/YYYY/history-YYMMDD.osm.pbf   (OSMF, weekly)
        │  one-off build on an i4i instance with local NVMe (≈ 10–20 h, then terminated)
        ▼
 s3://YOUR-BUCKET/lake/  catalog.ducklake   DuckLake catalog (DuckDB file): the only metadata
                         data/…              Parquet: current, *_history, changes, indexes, areas
                         lance/…             Lance id store (nodes, ways)
        ▲  every minute: updater job (Lambda or Fargate); conditional PUT of the catalog
        │  diffs from s3://osm-planet-eu-central-1/planet/replication/
        ▼
 read-only API: Lambda function URL (scales to zero) or Fargate service
```

Use **eu-central-1 (Frankfurt)** for everything. The OSMF bucket with the history planet and the
replication diffs is there, so reading it is in-region and free of transfer charges.

## 1. Bucket and roles

1. **Bucket.** Create `YOUR-BUCKET` in eu-central-1, with versioning off, Block Public Access on, and SSE-S3 encryption by default.
2. **Lifecycle rules:**
   - Expire `lake/catalogs/` after 30 days. These are per-commit catalog copies, kept only for rollback.
   - Abort incomplete multipart uploads after 1 day.
3. **IAM policy** `sunchaser-lake-rw`: `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` and `s3:ListBucket` on `YOUR-BUCKET` and `YOUR-BUCKET/lake/*`, plus `s3:GetObject` and `s3:ListBucket` on `osm-planet-eu-central-1`.
4. **Roles:**
   - `sunchaser-build`: an EC2 instance profile with `sunchaser-lake-rw` and `AmazonSSMManagedInstanceCore`, for Session Manager access without SSH.
   - `sunchaser-updater`: a Lambda execution or ECS task role with `sunchaser-lake-rw` and CloudWatch Logs.
   - `sunchaser-api`: `s3:GetObject` and `s3:ListBucket` on `YOUR-BUCKET/lake/*` only. Readers never write.

All components use the role's credentials. Set these environment variables everywhere:

```
SUNCHASER_S3_CREDENTIAL_CHAIN=true
SUNCHASER_S3_REGION=eu-central-1
SUNCHASER_S3_URL_STYLE=vhost
```

## 2. Build instance

| | Recommended | Minimum |
|---|---|---|
| Instance | `i4i.16xlarge`: 64 vCPU, 512 GiB, 4 × 3,750 GB NVMe | `i4i.8xlarge`: 32 vCPU, 256 GiB, 2 × 3,750 GB NVMe |
| AMI | Ubuntu 24.04 LTS (x86_64) | |
| Root volume | 50 GB gp3 | |
| Purchase | On-demand. The pipeline has no checkpoints, so a Spot interruption means starting over. | |

Local NVMe space needed (≈):

| What | Size |
|---|---|
| History PBF | 164 GB (October 2026) |
| Node position timeline | ≈ 110 GB index + ≈ 150–250 GB records |
| Way geometry timeline | ≈ 15 GB index + ≈ 100–150 GB records |
| Raw bucketed Parquet: current + history + changes | ≈ 1–1.5 TB |
| DuckDB build database and spill | ≈ 0.3–0.5 TB |
| **Total** | **≈ 2–2.5 TB**. Both instance types have more. |

Set it up, as root:

```bash
mdadm --create /dev/md0 --level=0 --raid-devices=$(ls /dev/nvme*n1 | grep -v nvme0 | wc -l) $(ls /dev/nvme*n1 | grep -v nvme0)
```

```bash
mkfs.xfs /dev/md0 && mkdir -p /nvme && mount -o noatime /dev/md0 /nvme
```

```bash
apt-get update && apt-get install -y build-essential git awscli osmium-tool
```

```bash
curl -sSf https://sh.rustup.rs | sh -s -- -y && . ~/.cargo/env
```

```bash
cd /nvme && git clone https://github.com/iandees/sunchaser.git && cd sunchaser
```

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh && . ~/.local/bin/env
```

```bash
uv sync --locked
```

```bash
cargo build --release --manifest-path rust/osm-extract/Cargo.toml
```

```bash
ulimit -n 65536
```

The `ulimit` is needed because, with 12 bucket bits, the extractor keeps up to 4,096 open files per bucketed table.

Then set the environment for the rest of the session:

```bash
export SUNCHASER_S3_CREDENTIAL_CHAIN=true SUNCHASER_S3_REGION=eu-central-1 SUNCHASER_S3_URL_STYLE=vhost
```

```bash
export LAKE=s3://YOUR-BUCKET/lake
```

## 3. Get the history planet

Pick the newest file under `planet-full-history/pbf/<year>/` and copy it in-region (≈ 10–20 min):

```bash
aws s3 ls s3://osm-planet-eu-central-1/planet-full-history/pbf/2026/ --no-sign-request | grep 'osm.pbf$' | tail -1
```

```bash
aws s3 cp s3://osm-planet-eu-central-1/planet-full-history/pbf/2026/history-261019.osm.pbf /nvme/ --no-sign-request
```

```bash
aws s3 cp s3://osm-planet-eu-central-1/planet-full-history/pbf/2026/history-261019.osm.pbf.md5 /nvme/ --no-sign-request
```

```bash
cd /nvme && md5sum -c history-261019.osm.pbf.md5
```

The file's replication timestamp becomes the lake's `osm_base`; the updater continues from there:

```bash
osmium fileinfo -g header.option.osmosis_replication_timestamp /nvme/history-261019.osm.pbf
```

If that prints nothing, use the dump's cut-off time (the date in the file name, 00:00 UTC) and pass it as `--osm-base` in step 5.

## 4. Extract (≈ 2–6 h)

```bash
cd /nvme/sunchaser && rust/osm-extract/target/release/osm-extract /nvme/history-261019.osm.pbf /nvme/raw --history --bucket-bits 12 --dense-index /nvme/idx 2>&1 | tee /nvme/extract.log
```

It reads every version once, in id order, keeping node position and way geometry timelines in
`/nvme/idx`, which is read randomly when ways and relations are sliced. It writes:
- Current rows, history rows (`valid_from`/`valid_to` with each version's metadata) and the `changes` log, bucketed by key range.
- The id locator, in id order, and the node → way index.

The final line gives row counts; `/nvme/raw/manifest.json` records them along with `osm_base`.

The time estimate scales Berlin by file size (15 M versions in 67 s on 4 threads, about 900× smaller), then adds headroom for the random timeline reads. Check `extract.log` for progress.

## 5. Build the lake (≈ 4–8 h)

```bash
uv run python src/build_lake.py --raw /nvme/raw --lake /nvme/build --catalog /nvme/catalog.ducklake --data-path $LAKE/data/ --lance $LAKE/lance --group-rows 200000000 2>&1 | tee /nvme/build.log
```

- **What it does:** sorts and writes each table one bucket group at a time, about 200 M rows per sort, straight to S3.
- **Global sorts:** `tag_postings` (by `key=value`) is the only one. `changes` is sorted one ≈ 48-day period at a time.
- **Spill:** DuckDB spills to `/nvme/build`.
- **Lance:** `--lance` also writes the Lance id store (≈ 1.25× the lake's Parquet size again). Leave it out to save space, at the cost of slower recursion and id lookups.

Then build the areas in full (≈ 1–2 h; DuckDB spatial over about 1.2 M pivot relations):

```bash
SUNCHASER_CATALOG=/nvme/catalog.ducklake uv run python src/build_areas.py --full
```

Expected lake size (≈): current tables 330 GB, history and changes 0.6–1.2 TB, Lance 350–400 GB. That is roughly $35–45 a month in S3 Standard; Intelligent-Tiering lowers it for the cold history files.

## 6. Publish the catalog and catch up

Upload the catalog. This makes the lake live for readers:

```bash
uv run python src/objcatalog.py put /nvme/catalog.ducklake $LAKE/catalog.ducklake
```

The history file is days old by now. Catch up from the build box with one long job, using hourly diffs, then minutely ones:

```bash
uv run python src/updater.py job --catalog-url $LAKE/catalog.ducklake --max-seconds 21600 --lease-seconds 25200 --keep-versions --workdir /nvme
```

A planet hour measured 11–22 s to apply on 4 cores against a small lake. Against the full tables, budget ≈ 1 min per hour of backlog, so a week of backlog takes a few hours. Re-run until the job reports `nothing new` or only a few diffs.

Check the result with a reader on the box:

```bash
SUNCHASER_CATALOG=$LAKE/catalog.ducklake uv run python src/server.py &
```

```bash
curl -s localhost:8080/api/interpreter --data-urlencode 'data=[out:json][date:"2015-01-01T00:00:00Z"];node[amenity=cafe](52.51,13.38,52.52,13.40);out count;'
```

Then terminate the build instance. Nothing on it is needed any more.

## 7. Scheduled updater (scale to zero)

The updater is a job, not a service. Each run:
1. Takes the writer lease.
2. Downloads the catalog.
3. Applies pending diffs within `--max-seconds`, updating Lance and areas incrementally.
4. Uploads the catalog with a conditional PUT.

Overlapping runs exit on the lease, and the conditional PUT makes a lost race harmless. Between runs nothing runs.

Image: build a container from the repo with `uv sync --locked --no-dev` and `src/`. The Rust extractor isn't needed here. Two ways to schedule it:

| | Lambda (recommended) | ECS Fargate scheduled task |
|---|---|---|
| Trigger | EventBridge Scheduler, `rate(1 minute)` | EventBridge Scheduler, `rate(5 minutes)` |
| Size | 6–10 GB memory; ephemeral `/tmp` 10 GB | 2 vCPU, 8–16 GB; ephemeral storage 50 GB |
| Command | `updater.py job --catalog-url $LAKE/catalog.ducklake --max-seconds 50 --compact-every 0 --workdir /tmp` | same, with `--max-seconds 240` |
| Notes | Starts in seconds; 15 min maximum per run | Task start-up takes 30–60 s, hence the longer cadence |

**Maintenance** goes in a second schedule, hourly, on Fargate, because it can outlast a Lambda:

```bash
updater.py job --catalog-url $LAKE/catalog.ducklake --max-seconds 0 --compact-every 1 --lease-seconds 3600
```

It flushes inlined rows, merges small files, compacts Lance, and deletes files unreferenced for more than an hour (`--file-grace`).

**Notes:**
- **Cost per run:** each run downloads the catalog. For the planet that is ≈ 50–200 MB, an in-region GET of 1–2 s. Measure it after the build: `aws s3 ls $LAKE/catalog.ducklake`.
- **Areas:** each run updates them for the relations its diffs touched (`build_areas.py` processes only the `changes` since the last run). A weekly `build_areas.py --full` is a cheap safety net.
- **Rollback:** with `--keep-versions`, every commit is also copied to `lake/catalogs/<osm_base>.ducklake`. To roll back, copy an older one over `catalog.ducklake` while no job is running.

## 8. Read-only API

Readers attach `$LAKE/catalog.ducklake` read-only and reopen it when its ETag changes, polling every `SUNCHASER_CATALOG_REFRESH` seconds. They need no other state.

| | Lambda function URL (scales to zero) | ECS Fargate service behind an ALB |
|---|---|---|
| Packaging | The container image plus the [AWS Lambda Web Adapter](https://github.com/awslabs/aws-lambda-web-adapter) layer (`AWS_LWA_INVOKE_MODE=response_stream`); the function URL in `RESPONSE_STREAM` mode | The same image running `src/server.py` |
| Size | 4–10 GB memory | 2–4 vCPU, 8–16 GB per task; autoscale on CPU, minimum 1 task |
| Limits | 15 min per request; streamed responses are capped (check the current Lambda limit). Large exports belong on the Fargate option. | `SUNCHASER_MAX_TIMEOUT`, `SUNCHASER_MAX_OUTPUT`, `SUNCHASER_MAX_QUERIES` |
| Cold start | Attaching the catalog over S3 takes a few seconds | None after the first task |

Environment for both: `SUNCHASER_CATALOG=$LAKE/catalog.ducklake`, `SUNCHASER_CATALOG_REFRESH=30`, plus the S3 variables from step 1. Point Overpass Turbo at `https://<function-url>/api/`. CORS is already handled.

## Costs (≈, eu-central-1, on-demand)

| Item | Estimate |
|---|---|
| Build: `i4i.16xlarge` for 10–20 h | ≈ $60–120, once |
| S3 storage: 1.3–2 TB | ≈ $30–45 a month |
| Updater: Lambda every minute, ≈ 20–40 s at 8 GB | ≈ $25–50 a month |
| Maintenance: Fargate hourly, ≈ 10 min | ≈ $5 a month |
| S3 requests: catalog GET/PUT per minute plus Parquet PUTs | ≈ $5–15 a month |
| API | Pay per request on Lambda; ≈ $70+ a month per always-on Fargate task |

## Before trusting the planet build

- Do a dress rehearsal on a country history file first: Geofabrik offers `.osh.pbf` extracts to logged-in OSM users. Use the same commands without `--dense-index` and with fewer bucket bits, and record the real times and sizes here.
- Run `tools/compare_overpass.py --history` against an Overpass loaded with `--keep-attic` from the same history file, as `results/overpass_diff_history_build.json` does for Berlin.
- Record the actual extract and build times, the lake size, the catalog size and the per-diff apply time in the tables above.
