# Handoff: run the planet build on the Hetzner box

**For:** a Claude Code session on Ian's machine with SSH access to the rented Hetzner box.
**From:** the cloud session that built and validated everything here; it can't SSH out.
**Goal:**
1. Build the full-history OpenStreetMap planet lake into Cloudflare R2.
2. Keep it current with minutely diffs.
3. Serve the Overpass API from the box.
4. Record the real resource numbers in the docs.

Read this whole file first, then `docs/hetzner-r2.md` (the runbook these steps follow).
`docs/architecture.md` explains the design if something surprises you.

## Ground rules

- **Ask Ian before:**
  - anything that erases disks (`installimage`);
  - anything that costs money beyond the rented box (a bigger box, extra storage);
  - exposing the API publicly (DNS, Caddy, firewall);
  - deleting data in R2.
- **Secrets:** R2 credentials live only in `/etc/sunchaser.env` on the box (mode 600). Never commit them, and never paste them into logs or docs.
- **Long-running work:** run anything longer than a few minutes inside `tmux`. Re-attach rather than restarting, because several stages take hours. The build script is resumable by stage.
- **Results:** commit them back to this repository (`main`): measurements under `results/sizing/`, and real numbers into `docs/hetzner-r2.md` and `docs/planet.md` where they say ≈. Keep the repository's style: tables for numbers, short sentences.

## State of the project (what you can rely on)

Everything below was validated on a Berlin extract in the cloud session. Details are in `results/NOTES.md` and `docs/architecture.md`.

- **Query correctness:** element-identical with a real Overpass (0.7.62) on 81 of 82 differential queries, including 13 of 13 history queries (`[date:]`, `newer`, `changed`). The one difference is Overpass's `out bb` placeholder for relation members missing from an extract.
- **Full-history build** (`osm-extract --history`, then `build_lake.py`): identical results to a lake updated incrementally through the same diffs.
- **Catalog on object storage:**
  - The DuckLake catalog is a single DuckDB file in the bucket.
  - The updater is the only writer: `updater.py job --loop` holds a lease, keeps the catalog locally, and uploads it with `If-Match` after each batch.
  - Readers are stateless and reload when the catalog's ETag changes.
  - R2 supports the conditional PUTs this needs.
- **Lance id store:** 1.54× faster queries (median 1.44 → 0.81 s at 30 ms per request), for 2× the object-store reads and about 1.25× extra storage. Kept in step with the lake by the updater.
- **Incremental areas:** each update rebuilds only the areas whose relations changed.
- **Not measured at planet scale:** time, disk and memory are extrapolations. The sizing pass (step 4) measures them.

## Box facts to check first

```bash
ssh root@<box> 'nproc; free -g; lsblk -d -o NAME,SIZE,MODEL; df -h; cat /etc/os-release | head -2; ulimit -Hn'
```

The plan assumes:
- Ubuntu 24.04;
- about 2 TB of RAID0 NVMe mounted at `/data`;
- 64–128 GB of RAM;
- 1 Gbit unmetered.

If the box is still in the rescue system, or has Hetzner's default RAID1 layout, see step 1. Report the facts to Ian before going on.

## Steps

### 1. Operating system (only if not installed as RAID0 with /data)

This erases both disks, so confirm with Ian first. In the Hetzner rescue system:
1. Check the drive names (`lsblk`) and the image name (`ls /root/.oldroot/nfs/images/`).
2. Copy `deploy/hetzner/installimage.conf` over (`scp`) and adjust it.
3. Run `installimage -a -c installimage.conf`, then `reboot`.

Afterwards `df -h /data` should show about 1.9 TB.

### 2. Software

```bash
ssh root@<box> 'git clone https://github.com/iandees/sunchaser.git /data/sunchaser && /data/sunchaser/deploy/hetzner/setup.sh'
```

`setup.sh` is idempotent. It installs build tools, uv and Rust, builds the extractor, runs `uv sync --locked`, raises the open-files limit, and creates `/etc/sunchaser.env` from the template. Log in again afterwards so the new limits apply.

### 3. R2 credentials

Ian creates them:
1. An R2 bucket (location hint Eastern Europe for a Helsinki box).
2. An API token with Object Read & Write on that bucket only.
3. A lifecycle rule expiring `lake/catalogs/` after 30 days.

Put the endpoint, keys and `LAKE=s3://<bucket>/lake` into `/etc/sunchaser.env` on the box (see `deploy/hetzner/sunchaser.env.example`). Then smoke-test it:

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

### 4. Sizing pass (recommended before the full build, ≈ 1–2 h)

```bash
tmux new -s sizing 'cd /data/sunchaser && deploy/hetzner/sizing.sh /data/sizing 2>&1 | tee /data/logs/sizing.log'
```

**What it does:**
1. Streams the 164 GB history planet once (8 parallel connections), cutting history extracts of Luxembourg, Berlin and the Switzerland box, and counting the whole planet exactly (`planet_stats.json`).
2. Builds each region's lake, with Lance and areas, locally under `tools/measure.py` (time, peak RAM, peak disk).
3. Writes `/data/sizing/estimate.md`, a planet extrapolation from the regions.

**Check:** the cloud session may already have committed a sizing run from its own 4-core container under `results/sizing/`. If so, compare. This box's core count changes the times, not the sizes.

**Decision point:** if the estimate's peak local disk for the extract is above about 90% of `df /data` free space, stop and tell Ian. Options:
- a box with more NVMe;
- a current-only planet (drop `--history` in `build-planet.sh`);
- `--bucket-bits 10`, which doesn't reduce the peak.

Copy the result back (`scp -r root@<box>:/data/sizing/estimate.md` and the `build-*/sizing.json` files into `results/sizing/`), commit, and show Ian the estimate.

### 5. Planet build (≈ 1–3 days on a 6-core box)

```bash
tmux new -s build 'cd /data/sunchaser && deploy/hetzner/build-planet.sh 2>&1 | tee -a /data/logs/build-planet.log'
```

**Stages** (each resumable; finished stages leave `/data/state/<stage>.done` and are skipped):
1. `extract`: the history planet is streamed into `osm-extract --history --dense-index`, checked against the published md5, and the timelines are deleted afterwards.
2. `build`: the lake goes straight to R2, with Lance. It deletes raw buckets as it goes (`CONSUME=1`).
3. `areas`: a full area build.
4. `publish`: the catalog is uploaded, and the lake is live.

**Watching it:**
- `tail -f /data/logs/{extract,build}.log`
- `df -h /data`, `free -g`
- `grep MEASURE /data/logs/*.log` gives each stage's time, peak RSS and peak disk.

**Failure handling:**

| Symptom | What to do |
|---|---|
| `md5 mismatch` | Re-run the script; the extract restarts. |
| Killed (OOM) in `extract` | Re-run with `WRITER_MEM_MB=128` (writer buffers, 8 tables) and/or `THREADS=4`. The extractor uses mimalloc and bounded writer buffers; at Berlin and planet-cut scale it stayed at 2–3 GB. |
| Killed (OOM) in `build` | Lower `GROUP_ROWS=50000000`. |
| `build` fails with `CONSUME=1` | The raw buckets are partly gone. The script refuses to continue and says what to remove; the extract must run again. If disk allows (see the sizing estimate), use `CONSUME=0` from the start, so a failed build can be re-run alone. |
| Open-files error | `ulimit -Hn` must allow `(7 << BUCKET_BITS) + 1280`. Raise it in `/etc/security/limits.d/` and log in again, or set `BUCKET_BITS=11`. |
| R2 403 or 400 | Check the env file, and the token's bucket scope. |

### 6. Updater and API

```bash
cp /data/sunchaser/deploy/hetzner/sunchaser-*.service /etc/systemd/system/ && systemctl daemon-reload
```

```bash
systemctl enable --now sunchaser-updater && journalctl -u sunchaser-updater -f
```

- **Catch-up:** the updater starts from the history file's timestamp, hourly diffs first, then minutely. A week of backlog takes hours; each committed batch logs `job --loop: N diffs to <timestamp>`.
- **API:** `systemctl enable --now sunchaser-api` serves on `127.0.0.1:8080`. Expose it only after asking Ian, with Caddy (`deploy/hetzner/Caddyfile.example`, a DNS name pointing at the box) or an SSH tunnel for testing.
- **Smoke tests**, through the API (`curl -s localhost:8080/api/interpreter --data-urlencode 'data=...'`):

  ```
  [out:json];node[amenity=cafe](52.51,13.38,52.52,13.40);out count;
  [out:json][date:"2015-01-01T00:00:00Z"];node[amenity=cafe](52.51,13.38,52.52,13.40);out count;
  [out:json];area[name="Berlin"][admin_level=4];node(area)[amenity=townhall];out ids;
  ```

  Also check that `curl -s localhost:8080/api/status` shows the data timestamp advancing.
- **Overpass Turbo:** settings → server `https://<host>/api/`.

### 7. Optional: still-open measurements

1. **Updater cost of Lance:** apply the same diffs with and without Lance, and compare the per-diff seconds in the updater log. On a copy of the catalog, run `--lance` off by removing `lance_base` from `lake.state`, or use a second lake.
2. **Query speed against real R2:** the cloud numbers used a local stand-in with 30 ms per request. Run `tools/compare_overpass.py`-style timings, or a set of real Turbo queries, against the live lake. Record median and p90.

## What to report back (and commit)

- `results/sizing/` (estimate and `sizing.json` files) and `/data/logs/*.log` `MEASURE` lines from the planet build: time, peak RSS and peak disk per stage.
- The final lake size in R2 (Cloudflare dashboard, or `rclone size`) and the catalog object's size.
- The updater's steady state: seconds per minutely diff, and compaction time.
- Anything that failed and how it was fixed. Update `docs/hetzner-r2.md` so the next run doesn't hit it.

## Where things are

| | |
|---|---|
| `deploy/hetzner/` | `setup.sh`, `sizing.sh`, `build-planet.sh`, systemd units, env template, `installimage.conf`, Caddyfile |
| `docs/hetzner-r2.md` | The runbook (also covers the laptop variant) |
| `rust/osm-extract` | Extractor: `--history`, `--dense-index`, `--cut`, stdin input, `--writer-mem-mb` |
| `src/build_lake.py` | Lake build (`--consume`, `--lance`, `--group-rows`) |
| `src/updater.py` | `job --loop` (always-on), `job` (scheduled), `compact` |
| `src/objcatalog.py` | Catalog in object storage: conditional PUT, writer lease |
| `tools/measure.py` | Wall time / peak RSS / peak disk wrapper |
| `tools/size_planet.py` | Planet estimate from a sizing run |
| `tools/relocate_lake.py` | Repoint a lake built on local disk after copying it to R2 |
