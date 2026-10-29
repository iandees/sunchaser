#!/usr/bin/env bash
# Measure what a full-history planet build needs, from regional cuts of the real planet.
#   1. One streamed pass over the history planet: regional history extractions + planet totals
#      (skipped if $OUT/planet_stats.json exists).
#   2. Per region: build the lake (with Lance) and its areas into local directories, recording wall
#      time, peak RSS and peak disk, plus raw and lake sizes.
#   3. tools/size_planet.py turns it into a planet estimate ($OUT/estimate.md).
# Usage: deploy/hetzner/sizing.sh [OUT]   (env: PLANET_URL, THREADS, REGIONS)
set -euo pipefail
cd "$(dirname "$0")/../.."
OUT=${1:-/data/sizing}
PLANET_URL=${PLANET_URL:-https://osm-planet-eu-central-1.s3.dualstack.eu-central-1.amazonaws.com/planet-full-history/pbf/2026/history-261019.osm.pbf}
THREADS=${THREADS:-8}
REGIONS=${REGIONS:-"luxembourg:49.44,5.73,50.19,6.53 berlin:52.33,13.08,52.68,13.77 switzerland:45.81,5.95,47.81,10.50"}
X=rust/osm-extract/target/release/osm-extract
ulimit -n 65536 2>/dev/null || ulimit -n "$(ulimit -Hn)"
mkdir -p "$OUT"

if [ ! -f "$OUT/planet_stats.json" ]; then
  cuts=(); for r in $REGIONS; do cuts+=(--cut "$r"); done
  uv run python -I tools/measure.py --label cut --watch "$OUT" -- bash -c \
    "uv run python -I tools/fetch_parallel.py '$PLANET_URL' --threads $THREADS 2>'$OUT/fetch.log' | $X - '$OUT' --history --writer-mem-mb 64 ${cuts[*]}" \
    2>&1 | tee "$OUT/cut.log"
fi

for r in $REGIONS; do
  name=${r%%:*}; raw="$OUT/$name"; work="$OUT/build-$name"
  [ -f "$raw/manifest.json" ] || { echo "missing $raw/manifest.json"; exit 1; }
  [ -f "$work/sizing.json" ] && continue
  rm -rf "$work"; mkdir -p "$work"
  du -sb "$raw"/*/ | awk '{n=$2; sub(".*/","",n); sub("/$","",n); printf "%s %s\n", n, $1}' > "$work/raw_sizes.txt"
  env -u SUNCHASER_CATALOG -u SUNCHASER_DATA_PATH uv run python -I tools/measure.py --label "build-$name" \
      --watch "$work" -- uv run python -I src/build_lake.py --raw "$raw" --lake "$work/tmp" \
      --catalog "$work/catalog.ducklake" --data-path "$work/lake/data/" --lance "$work/lake/lance" \
      --group-rows 50000000 2>&1 | tee "$work/build.log"
  SUNCHASER_CATALOG="$work/catalog.ducklake" uv run python -I tools/measure.py --label "areas-$name" \
      -- uv run python -I src/build_areas.py --full 2>&1 | tee "$work/areas.log"
  du -sb "$work/lake/data" "$work/lake/lance" | awk '{print $2, $1}' > "$work/lake_sizes.txt"
  python3 - "$work" "$raw" <<'PY'
import json, sys, pathlib
work, raw = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
measure = lambda f: [json.loads(l[8:]) for l in (work / f).read_text().splitlines() if l.startswith("MEASURE ")][-1]
pairs = lambda f: {k: int(v) for k, v in (l.split() for l in (work / f).read_text().splitlines())}
out = {"manifest": json.loads((raw / "manifest.json").read_text()), "raw_bytes": pairs("raw_sizes.txt"),
       "lake_bytes": pairs("lake_sizes.txt"), "build": measure("build.log"), "areas": measure("areas.log"),
       "build_log": [l for l in (work / "build.log").read_text().splitlines() if l.startswith(("lake.", "lance ")) or "files=" in l]}
(work / "sizing.json").write_text(json.dumps(out, indent=1))
print("wrote", work / "sizing.json")
PY
  rm -rf "$work/tmp"
done
uv run python -I tools/size_planet.py "$OUT" | tee "$OUT/estimate.md"
