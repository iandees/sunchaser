"""Differential test: src/quadtile.py vs. Overpass's C++ header (tools/qt_oracle)."""
import pathlib, random, subprocess, sys
import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import quadtile as qt

ORACLE = ROOT / "tools" / "qt_oracle"


def oracle(lines):
    out = subprocess.run([str(ORACLE)], input="\n".join(lines) + "\n", capture_output=True, text=True, check=True)
    return out.stdout.splitlines()


def pairs(line):
    v = list(map(int, line.split()))
    return list(zip(v[0::2], v[1::2]))


def main():
    rnd = random.Random(42)
    n = 3000
    # nodes: Berlin-ish plus global
    pts = [(rnd.uniform(52.2, 52.9), rnd.uniform(12.7, 14.0)) for _ in range(n)]
    pts += [(rnd.uniform(-89.9, 89.9), rnd.uniform(-179.9, 179.9)) for _ in range(n)]
    pts = [(round(a, 7), round(b, 7)) for a, b in pts]
    exp = list(map(int, oracle([f"N {a!r} {b!r}" for a, b in pts])))
    got = qt.node_index_np([round(a * 1e7) for a, _ in pts], [round(b * 1e7) for _, b in pts]).tolist()
    bad = sum(e != g for e, g in zip(exp, got))
    print(f"node index: {len(pts) - bad}/{len(pts)} match")

    # calc_index on clusters of nodes of varying spread, then on mixed way idxs (relations)
    cases = []
    for _ in range(3000):
        clat, clon = rnd.uniform(52.3, 52.7), rnd.uniform(13.0, 13.7)
        spread = 10 ** rnd.uniform(-4, 1.3)
        k = rnd.randint(1, 12)
        cases.append([int(v) for v in qt.node_index_np(
            [round((clat + rnd.uniform(-spread, spread)) * 1e7) for _ in range(k)],
            [round((clon + rnd.uniform(-spread, spread)) * 1e7) for _ in range(k)])])
    way_idx = list(map(int, oracle([f"W {len(c)} " + " ".join(map(str, c)) for c in cases])))
    got = [qt.calc_index(c) for c in cases]
    print(f"calc_index (ways): {sum(a == b for a, b in zip(way_idx, got))}/{len(cases)} match;",
          "levels seen:", sorted({qt.index_level(i) for i in way_idx}))
    rels = [sorted(set(rnd.sample(way_idx, rnd.randint(1, 6)) + rnd.choice(cases))) for _ in range(2000)]
    exp = list(map(int, oracle([f"W {len(c)} " + " ".join(map(str, c)) for c in rels])))
    got = [qt.calc_index(c) for c in rels]
    print(f"calc_index (rels): {sum(a == b for a, b in zip(exp, got))}/{len(rels)} match")

    # calc_ranges / calc_parents
    boxes = []
    for _ in range(400):
        s, w = rnd.uniform(52.3, 52.7), rnd.uniform(13.0, 13.7)
        d = 10 ** rnd.uniform(-3.5, 0)
        boxes.append((round(s, 6), round(s + d, 6), round(w, 6), round(w + d * rnd.uniform(0.5, 2), 6)))
    boxes += [(-10.0, 10.0, -5.0, 5.0), (40.0, 41.0, -74.5, -73.5)]
    exp_r = [pairs(l) for l in oracle([f"R {s!r} {n!r} {w!r} {e!r}" for s, n, w, e in boxes])]
    got_r = [qt.calc_ranges(*b) for b in boxes]
    print(f"calc_ranges: {sum(a == b for a, b in zip(exp_r, got_r))}/{len(boxes)} match")
    sub = exp_r[:150]
    exp_p = [pairs(l) for l in oracle([f"P {len(r)} " + " ".join(f"{a} {b}" for a, b in r) for r in sub])]
    got_p = [qt.calc_parents(r) for r in sub]
    # Uint31_Index prints/compares ignoring bit 31; restore it on the upper bound, then re-merge.
    exp_p = [qt.merge31(r) for r in exp_p]  # oracle prints raw Uint31 bounds; compare in key31 space
    print(f"calc_parents: {sum(a == b for a, b in zip(exp_p, got_p))}/{len(sub)} match")
    for a, b, box in zip(exp_r, got_r, boxes):
        if a != b:
            print("first ranges mismatch", box, a[:4], b[:4]); break
    for a, b in zip(exp_p, got_p):
        if a != b:
            print("first parents mismatch", a[:6], b[:6]); break


if __name__ == "__main__":
    main()
