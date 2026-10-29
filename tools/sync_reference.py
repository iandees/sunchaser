"""Bring a reference Overpass database to the same replication state as the lake.

Reads the updater log for the batches the lake applied and replays the same diffs into Overpass with
update_database. The reference is assumed to be at --from (e.g. after day diffs). Hour diffs are
applied after that timestamp, then the same minute diffs as the lake.

  python tools/sync_reference.py --log updater.log --db-dir /path/to/overpass-db --from 2026-10-29T00:00:00Z
"""
import argparse
import pathlib
import re
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import updater  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", required=True)
    ap.add_argument("--db-dir", required=True)
    ap.add_argument("--from", dest="start", required=True, help="timestamp the reference is at now")
    ap.add_argument("--bin", default="/home/user/overpass-ref/bin/update_database")
    ap.add_argument("--keep-attic", action="store_true", help="the reference was loaded with --keep-attic")
    args = ap.parse_args()
    batches = re.findall(r"^(hour|minute)/(\d+) -> (\S+)", pathlib.Path(args.log).read_text(), re.M)
    todo = [(k, int(s), ts) for k, s, ts in batches if ts > args.start]
    seen, plan = set(), []
    for b in todo:  # a restarted updater may log a batch twice
        if b not in seen:
            seen.add(b)
            plan.append(b)
    print(f"{len(plan)} diffs to apply: {plan[0][:2] if plan else None} .. {plan[-1] if plan else None}")
    for kind, seq, ts in plan:
        t = time.time()
        data = updater.osc(kind, seq)
        r = subprocess.run([args.bin, f"--db-dir={args.db_dir}", "--keep-attic" if args.keep_attic else "--meta",
                            "--map-compression-method=gz"],
                           input=subprocess.run(["gunzip", "-c"], input=data, capture_output=True).stdout,
                           capture_output=True)
        if r.returncode:
            sys.exit(f"{kind}/{seq} failed: {r.stderr[-500:]!r}")
        print(f"{kind}/{seq} -> {ts} ({time.time() - t:.1f}s)", flush=True)


if __name__ == "__main__":
    main()
