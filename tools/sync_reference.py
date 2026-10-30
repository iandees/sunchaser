"""Bring a reference Overpass database to the same replication state as the lake.

Reads the updater log for the batches the lake applied and replays the same diffs into Overpass with
update_database. The reference is assumed to be at --from (e.g. after day diffs). Hour diffs are
applied after that timestamp, then the same minute diffs as the lake.

  python tools/sync_reference.py --log updater.log --db-dir /path/to/overpass-db --from 2026-10-29T00:00:00Z
"""
import argparse
import gzip
import pathlib
import re
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import updater  # noqa: E402


def lake_changed(catalog):
    """(type, id) of every element the lake created, modified or deleted (its changes log)."""
    import duckdb
    con = duckdb.connect()
    con.sql("LOAD ducklake")
    con.sql(f"ATTACH 'ducklake:{catalog}' AS lake (READ_ONLY)")
    rows = con.sql("SELECT DISTINCT type, id FROM lake.changes WHERE action <> 'geometry'").fetchall()
    return {("node" if t == "n" else "way" if t == "w" else "relation", i) for t, i in rows}


def filter_osc(xml, keep):
    """Keep only the osmChange elements in `keep`: the reference then receives exactly what the
    regional lake applied (an attic Overpass rejects far-away ways whose nodes it never had)."""
    import xml.etree.ElementTree as ET
    root = ET.fromstring(xml)
    for action in root:
        for el in list(action):
            if (el.tag, int(el.get("id"))) not in keep:
                action.remove(el)
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", required=True)
    ap.add_argument("--db-dir", required=True)
    ap.add_argument("--from", dest="start", required=True, help="timestamp the reference is at now")
    ap.add_argument("--bin", default="/home/user/overpass-ref/bin/update_database")
    ap.add_argument("--keep-attic", action="store_true", help="the reference was loaded with --keep-attic")
    ap.add_argument("--lake-changes", metavar="CATALOG",
                    help="apply only the elements this lake's changes log has (a .ducklake file)")
    args = ap.parse_args()
    keep = lake_changed(args.lake_changes) if args.lake_changes else None
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
        xml = gzip.decompress(updater.osc(kind, seq))
        if keep is not None:
            xml = filter_osc(xml, keep)
        r = subprocess.run([args.bin, f"--db-dir={args.db_dir}", "--keep-attic" if args.keep_attic else "--meta",
                            "--map-compression-method=gz"], input=xml, capture_output=True)
        if r.returncode:
            sys.exit(f"{kind}/{seq} failed: {r.stderr[-500:]!r}")
        print(f"{kind}/{seq} -> {ts} ({time.time() - t:.1f}s)", flush=True)


if __name__ == "__main__":
    main()
