"""Run a command and report wall time, peak memory (RSS of the whole process tree) and the peak
size of some directories, sampled every second. For sizing builds:

  python tools/measure.py --watch /data/raw --watch /data/build -- osm-extract ...

Prints one JSON line at the end (and the command's own output as it runs).
"""
import argparse
import json
import os
import subprocess
import sys
import time


def tree_rss(pid):
    """Resident memory of pid and its descendants, in bytes (Linux /proc)."""
    children = {}
    for p in os.listdir("/proc"):
        if not p.isdigit():
            continue
        try:
            with open(f"/proc/{p}/stat") as f:
                ppid = int(f.read().rsplit(")", 1)[1].split()[1])
            children.setdefault(ppid, []).append(int(p))
        except (OSError, IndexError, ValueError):
            pass
    total, stack = 0, [pid]
    while stack:
        q = stack.pop()
        try:
            with open(f"/proc/{q}/statm") as f:
                total += int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
        except OSError:
            pass
        stack.extend(children.get(q, []))
    return total


def du(path):
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += os.lstat(os.path.join(root, name)).st_blocks * 512  # allocated, sparse-aware
            except OSError:
                pass
    return total


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watch", action="append", default=[], help="directory whose peak size to record")
    ap.add_argument("--label", default="")
    ap.add_argument("cmd", nargs=argparse.REMAINDER)
    args = ap.parse_args()
    cmd = args.cmd[1:] if args.cmd and args.cmd[0] == "--" else args.cmd
    t0 = time.time()
    proc = subprocess.Popen(cmd)
    peak_rss, peak_disk = 0, {w: 0 for w in args.watch}
    while proc.poll() is None:
        peak_rss = max(peak_rss, tree_rss(proc.pid))
        for w in args.watch:
            peak_disk[w] = max(peak_disk[w], du(w))
        time.sleep(1)
    for w in args.watch:
        peak_disk[w] = max(peak_disk[w], du(w))
    out = {"label": args.label, "seconds": round(time.time() - t0, 1), "exit": proc.returncode,
           "peak_rss_gb": round(peak_rss / 1e9, 2), "peak_disk_gb": {w: round(v / 1e9, 2) for w, v in peak_disk.items()}}
    print("MEASURE " + json.dumps(out), flush=True)
    sys.exit(proc.returncode)


if __name__ == "__main__":
    main()
