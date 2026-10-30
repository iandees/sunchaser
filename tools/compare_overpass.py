"""Differential test: run each request through a reference Overpass (osm3s_query) and through our QL
executor, and compare the outputs element by element (order and formatting ignored).

  python tools/compare_overpass.py --db-dir /path/to/overpass-db [--only name ...]

JSON and XML outputs are parsed into normalised element dicts and compared as multisets; CSV is
compared as a multiset of rows (header compared exactly).
"""
import argparse
import collections
import json
import pathlib
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "queries")]
import duckdb  # noqa: E402
import lakeconn  # noqa: E402
import ql  # noqa: E402
from engine import Engine  # noqa: E402
from ql_corpus import QL, QL_HISTORY  # noqa: E402


def norm(v):
    if isinstance(v, float):
        return round(v, 7)
    if isinstance(v, dict):
        return {k: norm(x) for k, x in sorted(v.items())}
    if isinstance(v, list):
        return [norm(x) for x in v]
    return v


def xml_elements(text):
    root = ET.fromstring(text)
    out = []
    for el in root:
        if el.tag in ("note", "meta", "remark"):
            continue
        d = {"type": el.tag, **{k: (float(v) if k in ("lat", "lon") else v) for k, v in el.attrib.items()}}
        for c in el:
            if c.tag == "tag":
                d.setdefault("tags", {})[c.get("k")] = c.get("v")
            elif c.tag == "nd":
                d.setdefault("nd", []).append({k: (float(v) if k != "ref" else v) for k, v in c.attrib.items()})
            elif c.tag == "member":
                m = dict(c.attrib)
                pts = [{k: float(v) for k, v in p.attrib.items()} for p in c]
                if pts:
                    m["geometry"] = pts
                d.setdefault("members", []).append(m)
            else:
                d[c.tag] = {k: float(v) for k, v in c.attrib.items()}
        out.append(d)
    return out


def elements(fmt, text):
    if fmt == "csv":
        lines = text.rstrip("\n").split("\n")
        return [("header", lines[0])] + [("row", ln) for ln in lines[1:]]
    if fmt == "json":
        return json.loads(text)["elements"]
    return xml_elements(text)


def fmt_of(query):
    return "json" if "[out:json]" in query else "csv" if "[out:csv" in query else "xml"


def inject_bbox(query, bbox):
    """Add [bbox:...] to the settings block (after its last ']', quotes and nested brackets aware)."""
    setting = f"[bbox:{bbox}]"
    i, end = 0, None
    while True:
        while i < len(query) and query[i].isspace():
            i += 1
        if i >= len(query) or query[i] != "[":
            break
        depth, quote = 0, None
        while i < len(query):
            c = query[i]
            if quote:
                quote = None if c == quote else quote
            elif c in "\"'":
                quote = c
            elif c == "[":
                depth += 1
            elif c == "]":
                depth -= 1
                if depth == 0:
                    break
            i += 1
        i += 1
        end = i
    if end is None:
        return setting + ";" + query
    return query[:end] + setting + query[end:]


def overpass(db_dir, query):
    r = subprocess.run(["/home/user/overpass-ref/bin/osm3s_query", f"--db-dir={db_dir}"],
                       input=query, capture_output=True, text=True, timeout=600)
    return r.stdout


def multiset(items):
    return collections.Counter(json.dumps(norm(x), sort_keys=True, ensure_ascii=False) for x in items)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-dir", default="/home/user/overpass-db")
    ap.add_argument("--only", nargs="*")
    ap.add_argument("--out", default="results/overpass_diff.json")
    ap.add_argument("--no-lance", action="store_true", help="ignore the lake's Lance id store")
    ap.add_argument("--history", action="store_true",
                    help="also run the attic queries (reference loaded with --keep-attic, same diffs replayed)")
    ap.add_argument("--global-bbox", help="s,w,n,e: inject [bbox:...] into every request (a regional lake "
                                          "compared with a reference holding planet-wide updates)")
    args = ap.parse_args()
    con = duckdb.connect()
    lakeconn.attach(con, read_only=True)
    import lancestore
    base, versions = lancestore.state_versions(con)
    store = lancestore.Store(base, versions) if base and not args.no_lance else None
    print(f"Lance id store: {base} {versions}" if store else "no Lance id store", flush=True)
    rows, ok = [], 0
    for name, query in QL + (QL_HISTORY if args.history else []):
        if args.only and name not in args.only:
            continue
        if args.global_bbox and "[bbox:" not in query:
            query = inject_bbox(query, args.global_bbox)
        fmt = fmt_of(query)
        t = time.perf_counter()
        ref_text = overpass(args.db_dir, query)
        t_ref = time.perf_counter() - t
        t = time.perf_counter()
        try:
            _, ours_text = ql.Executor(Engine(con, "lake", lance=store),
                                       "").run(ql.parse(query)).render()
            err = None
        except Exception as e:  # report and continue
            ours_text, err = "", f"{type(e).__name__}: {e}"
        t_ours = time.perf_counter() - t
        try:
            ref = multiset(elements(fmt, ref_text))
        except Exception as e:
            print(f"{name:<30} reference output unparseable: {e}: {ref_text[:200]!r}")
            continue
        ours = multiset(elements(fmt, ours_text)) if err is None else collections.Counter()
        missing, extra = ref - ours, ours - ref
        match = not missing and not extra and err is None
        ok += match
        row = dict(name=name, fmt=fmt, n_ref=sum(ref.values()), n_ours=sum(ours.values()), match=match,
                   missing=len(missing), extra=len(extra), error=err, ref_s=round(t_ref, 2), ours_s=round(t_ours, 2),
                   sample_missing=[json.loads(k) if fmt != "csv" else k for k in list(missing)[:2]],
                   sample_extra=[json.loads(k) if fmt != "csv" else k for k in list(extra)[:2]])
        rows.append(row)
        print(f"{name:<30} {fmt:<4} ref={row['n_ref']:<6} ours={row['n_ours']:<6} {'OK ' if match else 'DIFF'} "
              f"missing={row['missing']} extra={row['extra']} ref={t_ref:.2f}s ours={t_ours:.2f}s"
              + (f" ERROR {err}" if err else ""), flush=True)
    (ROOT / args.out).write_text(json.dumps(rows, indent=1, ensure_ascii=False, default=str))
    print(f"{ok}/{len(rows)} element-level matches against reference Overpass -> {args.out}")


if __name__ == "__main__":
    main()
