"""Planet estimate from regional history cuts (deploy/hetzner/sizing.sh output).

Inputs in OUT: planet_stats.json (exact planet totals from the cut pass), <region>/ (raw history
extraction + manifest.json), build-<region>/sizing.json (build and areas measurements, raw and
lake sizes). Every size is scaled by the planet/region ratio of the quantity it depends on:
node, way or relation versions, or all versions; the spread across regions shows how far one
region's mix (urban, rural) carries.

  python tools/size_planet.py OUT [--cores N]   ->  markdown on stdout
"""
import argparse
import json
import pathlib
import re
import statistics

import duckdb

DRIVER = {  # raw sub-directory or lake table -> which planet total it scales with
    "nodes": "n", "nodes_history": "n", "nodes_tagged": "n", "nodes_untagged": "n", "nodes_meta": "n",
    "ways": "w", "ways_history": "w", "ways_meta": "w", "node_parent_ways": "w",
    "relations": "r", "relations_history": "r", "relations_meta": "r", "member_parent_rels": "r",
    "changes": "all", "locator": "all", "elem_locator": "all", "tag_postings": "all", "areas": "r",
}


def gb(x):
    return f"{x / 1e9:,.1f}"


def region_versions(raw):
    """Versions (create/modify/delete) and geometry slices per type, from the region's change log."""
    con = duckdb.connect()
    rows = con.sql(f"""SELECT type, count(*) FILTER (WHERE action <> 'geometry'),
                              count(*) FILTER (WHERE action = 'geometry')
                       FROM read_parquet('{raw}/changes/*.parquet') GROUP BY 1""").fetchall()
    return {t: v for t, v, _ in rows}, {t: g for t, _, g in rows}


def lake_tables(build_log):
    """table -> MB from build_lake's closing table listing."""
    out = {}
    for line in build_log:
        m = re.match(r"\s+(\w+)\s+files=\d+\s+([\d.]+) MB", line)
        if m:
            out[m.group(1)] = float(m.group(2)) * 1e6
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--cores", type=int, default=0, help="cores of the target box (default: same as measured)")
    args = ap.parse_args()
    out = pathlib.Path(args.out)
    planet = json.loads((out / "planet_stats.json").read_text())
    pv = {"n": planet["versions"][0], "w": planet["versions"][1], "r": planet["versions"][2]}
    pv["all"] = sum(planet["versions"])
    cut = [json.loads(l[8:]) for l in (out / "cut.log").read_text().splitlines() if l.startswith("MEASURE ")] \
        if (out / "cut.log").exists() else []

    regions = []
    for sz in sorted(out.glob("build-*/sizing.json")):
        name = sz.parent.name[len("build-"):]
        s = json.loads(sz.read_text())
        v, geo = region_versions(out / name)
        v["all"] = sum(v.values())
        regions.append({"name": name, "s": s, "v": v, "geo": geo})
    if not regions:
        raise SystemExit("no build-*/sizing.json yet")

    def scaled(region, key, size):
        d = DRIVER.get(key, "all")
        return size * pv[d] / max(region["v"].get(d, 0), 1)

    print("# Planet estimate from regional history cuts\n")
    print(f"Planet ({planet.get('osm_base', '?')}): {pv['n']:,} node versions, {pv['w']:,} way versions, "
          f"{pv['r']:,} relation versions; {planet['node_moves']:,} node position changes; "
          f"max ids n={planet['max_id'][0]:,} w={planet['max_id'][1]:,}.\n")
    if cut:
        c = cut[-1]
        print(f"Cut pass over the whole history planet: {c['seconds'] / 60:.0f} min, peak RSS {c['peak_rss_gb']} GB.\n")

    print("| Region | Node versions | Way versions | Rel. versions | Share of planet versions | Way geometry slices per way version |")
    print("|---|---|---|---|---|---|")
    for r in regions:
        print(f"| {r['name']} | {r['v'].get('n', 0):,} | {r['v'].get('w', 0):,} | {r['v'].get('r', 0):,} | "
              f"{100 * r['v']['all'] / pv['all']:.3f}% | {r['geo'].get('w', 0) / max(r['v'].get('w', 1), 1):.2f} |")

    # sizes: raw extraction (disk during the extract) and lake (R2), scaled per table
    def estimate(kind):
        per = []
        for r in regions:
            sizes = r["s"]["raw_bytes"] if kind == "raw" else lake_tables(r["s"]["build_log"])
            per.append(sum(scaled(r, k, b) for k, b in sizes.items()))
        return per

    raw, lake = estimate("raw"), estimate("lake")
    lance = [r["s"]["lake_bytes"].get(str(pathlib.Path(r["s"]["build"]["peak_disk_gb"] and "") or ""), 0) for r in regions]
    lance = []
    for r in regions:
        lb = r["s"]["lake_bytes"]
        lance_b = next((b for k, b in lb.items() if k.endswith("lance")), 0)
        cur = r["s"]["manifest"]
        lance.append(lance_b * (planet["ids"][0] + planet["ids"][1]) / max(cur["nodes"] + cur["ways"], 1))
    timeline = 8 * planet["max_id"][0] + 16 * planet["node_moves"]
    way_tl = [8 * planet["max_id"][1] + 32 * (r["s"]["manifest"]["ways"] + r["s"]["manifest"]["ways_history"])
              * pv["w"] / max(r["v"].get("w", 1), 1) for r in regions]
    rng = lambda xs: f"{gb(min(xs))}–{gb(max(xs))}" if len(xs) > 1 else gb(xs[0])
    print("\n| Planet, full history | GB (range across regions) |")
    print("|---|---|")
    print(f"| Raw extraction (local disk) | {rng(raw)} |")
    print(f"| Node position timeline (`--dense-index`) | {gb(timeline)} |")
    print(f"| Way geometry timeline | {rng(way_tl)} |")
    print(f"| **Peak local disk, extract** (raw + timelines; the PBF streamed) | **{rng([a + timeline + w for a, w in zip(raw, way_tl)])}** |")
    print(f"| Lake in R2 (Parquet) | {rng(lake)} |")
    print(f"| Lance id store in R2 | {rng(lance)} |")

    # time and memory
    print("\n| Step | Measured on the regions | Planet estimate |")
    print("|---|---|---|")
    for step in ("build", "areas"):
        per = []
        for r in regions:
            m = r["s"][step]
            d = "all" if step == "build" else "r"
            per.append(m["seconds"] * pv[d] / max(r["v"].get(d, 1), 1))
        measured = ", ".join(f"{r['name']} {r['s'][step]['seconds']:.0f} s / {r['s'][step]['peak_rss_gb']} GB" for r in regions)
        print(f"| {step} | {measured} | {rng([p / 3600 * 1e9 for p in per]).replace(',', '')} h at the measured core count |")
    proc = [r["s"]["manifest"].get("process_seconds", 0) * pv["all"] / max(r["v"]["all"], 1) for r in regions]
    print(f"| extract (history processing, excluding decode) | "
          f"{', '.join(str(r['s']['manifest'].get('process_seconds')) + ' s' for r in regions)} | "
          f"{rng([p / 3600 * 1e9 for p in proc]).replace(',', '')} h, plus the decode pass |")
    print("\nTimes scale linearly with data and roughly inversely with cores (DuckDB and the extractor's "
          "group processing are parallel); disk and lake sizes don't depend on the machine.")
    if args.cores:
        print(f"(Target: {args.cores} cores.)")


if __name__ == "__main__":
    main()
