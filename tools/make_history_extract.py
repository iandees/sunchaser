"""Synthesise a history file (.osh.pbf) for testing the full-history build on an extract.

Real full-history planets come from planet.openstreetmap.org (history-latest.osm.pbf); extract
providers rarely offer history. This merges an extract snapshot with the replication diffs a lake
applied afterwards (the updater log), keeping every version (deletions as visible=false), so a
history build from the result should answer like the lake that applied those diffs incrementally.

Only elements the lake's changes log has are taken from the diffs (the same filter as
tools/sync_reference.py --lake-changes), so a regional lake and the history file agree.

  python tools/make_history_extract.py --pbf Berlin.osm.pbf --log updater.log --from 2026-10-24T00:00:00Z \
      --lake-changes /path/lake.ducklake --out berlin.osh.pbf
"""
import argparse
import gzip
import os
import pathlib
import re
import sys

import osmium

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools"))
import updater  # noqa: E402
from sync_reference import filter_osc, lake_changed  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pbf", required=True, help="extract snapshot")
    ap.add_argument("--log", required=True, help="updater log listing the applied diffs")
    ap.add_argument("--from", dest="start", required=True, help="snapshot timestamp")
    ap.add_argument("--until", help="last diff end timestamp to include")
    ap.add_argument("--lake-changes", required=True, metavar="CATALOG")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    keep = lake_changed(args.lake_changes)
    batches = re.findall(r"^(hour|minute)/(\d+) -> (\S+)", pathlib.Path(args.log).read_text(), re.M)
    plan = []
    for kind, seq, ts in batches:
        if ts > args.start and (args.until is None or ts <= args.until) and (kind, seq) not in [p[:2] for p in plan]:
            plan.append((kind, seq, ts))
    reader = osmium.MergeInputReader()
    reader.add_file(args.pbf)
    for kind, seq, ts in plan:
        reader.add_buffer(filter_osc(gzip.decompress(updater.osc(kind, int(seq))), keep), "osc")
    if os.path.exists(args.out):
        os.remove(args.out)
    header = osmium.io.Header()
    header.has_multiple_object_versions = True
    header.set("osmosis_replication_timestamp", plan[-1][2] if plan else args.start)
    writer = osmium.SimpleWriter(args.out, header=header)
    reader.apply(writer, simplify=False)  # every version, sorted by type, id, version
    writer.close()
    print(f"{args.out}: snapshot + {len(plan)} diffs ({len(keep)} changed elements), "
          f"{os.path.getsize(args.out) / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
