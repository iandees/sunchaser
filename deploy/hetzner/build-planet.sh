#!/usr/bin/env bash
# Planet build with full history into R2, resumable by stage (a finished stage leaves
# $DATA/state/<stage>.done and is skipped on re-run). Each stage runs under tools/measure.py, so
# its time, peak memory and peak disk land in $DATA/logs/<stage>.log ("MEASURE {...}").
#   extract  stream the history planet from OSMF into osm-extract (nothing stored but its output)
#   build    raw buckets -> lake in R2 (+ Lance), consuming the raw buckets as it goes
#   areas    full area build
#   publish  upload the catalog: the lake goes live
# Then start the updater: systemctl enable --now sunchaser-updater
# Usage: deploy/hetzner/build-planet.sh   (env from /etc/sunchaser.env; PLANET_URL, THREADS,
#        BUCKET_BITS, GROUP_ROWS, WRITER_MEM_MB, MEMORY_LIMIT, LANCE=0 to skip Lance, CONSUME=0 to keep the raw buckets, DATA=/data)
# LAKE set on the command line wins over the env file's, so a planet can be built into a new prefix
# (LAKE=s3://bucket/planet) while the env file's lake stays live for the updater and API.
# CONSUME=1 (default) deletes raw buckets as the build uses them, lowering peak disk, but a build
# that fails part-way then needs the extract again (rm $DATA/state/extract.done). With room to
# spare, CONSUME=0 makes the build stage re-runnable on its own.
set -euo pipefail
cd "$(dirname "$0")/../.."
lake_arg=${LAKE:-}
set -a; . /etc/sunchaser.env; set +a
LAKE=${lake_arg:-${LAKE:-}}
DATA=${DATA:-/data}
PLANET_URL=${PLANET_URL:-https://osm-planet-eu-central-1.s3.dualstack.eu-central-1.amazonaws.com/planet-full-history/pbf/2026/history-261026.osm.pbf}
THREADS=${THREADS:-8}
BUCKET_BITS=${BUCKET_BITS:-12}
GROUP_ROWS=${GROUP_ROWS:-100000000}
MEMORY_LIMIT=${MEMORY_LIMIT:-70GB}  # DuckDB's cap for the build: unset, Germany's build peaked at 109 GB, with 40GB at 42 GB
WRITER_MEM_MB=${WRITER_MEM_MB:-256}  # extractor buffers per bucketed table (8 tables)
LANCE=${LANCE:-1}
CONSUME=${CONSUME:-1}
X=rust/osm-extract/target/release/osm-extract
M="uv run python -I tools/measure.py"
ulimit -n 65536 2>/dev/null || ulimit -n "$(ulimit -Hn)"
need=$(( (7 << BUCKET_BITS) + 1024 + 256 ))  # history mode: 7 bucketed tables + 1024 change-log buckets
[ "$(ulimit -n)" -ge "$need" ] || { echo "open-files limit $(ulimit -n) < $need: raise it or lower BUCKET_BITS"; exit 1; }
mkdir -p "$DATA/state" "$DATA/logs"
case "$LAKE" in s3://*) ;; *) echo "LAKE must be s3://... (see /etc/sunchaser.env)"; exit 1;; esac

stage() { [ -f "$DATA/state/$1.done" ] && { echo "== $1: done already"; return 1; }; echo "== $1: $(date -u +%FT%TZ)"; }
done_() { touch "$DATA/state/$1.done"; }

if stage extract; then
  export MIMALLOC_PURGE_DELAY=-1  # keep freed memory: returning it to the OS and re-faulting zeroed pages was 7% of the main thread
  rm -rf "$DATA/raw" "$DATA/idx"
  size=$(curl -sSfI "$PLANET_URL" | awk 'tolower($1) == "content-length:" {print $2 + 0}')  # for % and ETA in the progress lines
  $M --label extract --watch "$DATA/raw" --watch "$DATA/idx" -- bash -c \
    "set -o pipefail; uv run python -I tools/fetch_parallel.py '$PLANET_URL' --threads $THREADS 2>'$DATA/logs/fetch.log' \
     | tee >(md5sum > '$DATA/logs/planet.md5') \
     | $X - '$DATA/raw' --history --bucket-bits $BUCKET_BITS --writer-mem-mb $WRITER_MEM_MB --dense-index '$DATA/idx' --input-size ${size:-0}" 2>&1 | tee "$DATA/logs/extract.log"
  expected=$(curl -sSf "$PLANET_URL.md5" | cut -d' ' -f1)
  got=$(cut -d' ' -f1 "$DATA/logs/planet.md5")
  [ "$expected" = "$got" ] || { echo "md5 mismatch ($got, expected $expected): re-run"; exit 1; }
  grep -q '"osm_base"' "$DATA/raw/manifest.json" || echo "WARNING: no replication timestamp in the header; pass --osm-base to the build"
  rm -rf "$DATA/idx"
  done_ extract
fi

if stage build; then
  if [ "$CONSUME" = 1 ] && [ -f "$DATA/state/build.started" ]; then
    echo "a consuming build was interrupted: the raw buckets are partly gone; re-run the extract"
    echo "(rm $DATA/state/extract.done $DATA/state/build.started) and clean up $LAKE/data/ and $LAKE/lance/"
    exit 1
  fi
  touch "$DATA/state/build.started"
  rm -rf "$DATA/build" "$DATA"/catalog.ducklake*  # a fresh catalog; files of a failed attempt in R2 are orphans
  lance=(); [ "$LANCE" = 1 ] && lance=(--lance "$LAKE/lance")
  consume=(); [ "$CONSUME" = 1 ] && consume=(--consume)
  $M --label build --watch "$DATA/raw" --watch "$DATA/build" -- uv run python -I src/build_lake.py \
    --raw "$DATA/raw" --lake "$DATA/build" --catalog "$DATA/catalog.ducklake" --data-path "$LAKE/data/" \
    "${lance[@]}" --group-rows "$GROUP_ROWS" --memory-limit "$MEMORY_LIMIT" "${consume[@]}" 2>&1 | tee "$DATA/logs/build.log"
  done_ build
fi

if stage areas; then
  SUNCHASER_CATALOG="$DATA/catalog.ducklake" $M --label areas -- uv run python -I src/build_areas.py --full \
    2>&1 | tee "$DATA/logs/areas.log"
  done_ areas
fi

if stage publish; then
  uv run python -I src/objcatalog.py put "$DATA/catalog.ducklake" "$LAKE/catalog.ducklake" | tee "$DATA/logs/publish.log"
  done_ publish
fi
grep -h '^MEASURE' "$DATA"/logs/*.log || true
echo "next: systemctl enable --now sunchaser-updater sunchaser-api (catches up from $(jq -r .osm_base "$DATA/raw/manifest.json" 2>/dev/null))"
