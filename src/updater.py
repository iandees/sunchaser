"""Minutely updater: applies OSM replication diffs (osmChange) to the lake.

  python src/updater.py run [--region s,w,n,e] [--once]    follow replication (day -> hour -> minute catch-up)
  python src/updater.py apply FILE.osc.gz ...               apply local diffs
  python src/updater.py compact                             flush inlined data, merge files, expire snapshots

The updater is the lake's single writer (like Overpass's update_database). Each batch commits as one
DuckLake transaction, so API readers see either the whole diff or none of it. Per batch:

  1. parse the diffs, keeping the newest version of every element (deletes included)
  2. regional filter (only with --region): keep elements already in the lake, nodes inside the region,
     ways using a kept node, relations with a kept member (an extract's usual "complete ways" rule)
  3. cascade: moved nodes -> ways using them -> relations containing those nodes/ways; their
     coordinates, bboxes and Overpass index keys are recomputed with the build's SQL macros
  4. rewrite every affected row in nodes_*, ways, relations, *_meta, tag_postings, elem_locator,
     node_parent_ways, member_parent_rels, then advance state.osm_base / state.sequence
"""
import argparse
import gzip
import io
import pathlib
import sys
import time
import urllib.request

import duckdb
import osmium
import pyarrow as pa

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import lakeconn  # noqa: E402
import quadtile as qt  # noqa: E402
from build_lake import MACROS  # noqa: E402
from engine import Engine  # noqa: E402

REPLICATION = "https://osm-planet-eu-central-1.s3.dualstack.eu-central-1.amazonaws.com/planet/replication"
NO_COORD = -2147483648
TAGS_T = pa.map_(pa.string(), pa.string())
META_T = [("version", pa.int32()), ("ts", pa.timestamp("s", tz="UTC")), ("changeset", pa.int64()),
          ("uid", pa.int32()), ("user", pa.string())]
MEMBER_T = pa.struct([("type", pa.string()), ("ref", pa.int64()), ("role", pa.string())])


# ---- replication feed ----------------------------------------------------------------

def fetch(url, retries=4):
    for i in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                return r.read()
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(2 ** (i + 1))


def seq_path(seq):
    s = f"{seq:09d}"
    return f"{s[0:3]}/{s[3:6]}/{s[6:9]}"


def parse_state(text):
    kv = {}
    for line in text.decode().splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            kv[k] = v.replace("\\:", ":")
    return int(kv["sequenceNumber"]), kv["timestamp"]


def state_of(kind, seq=None):
    url = f"{REPLICATION}/{kind}/" + (f"{seq_path(seq)}.state.txt" if seq is not None else "state.txt")
    return parse_state(fetch(url))


def osc(kind, seq):
    return fetch(f"{REPLICATION}/{kind}/{seq_path(seq)}.osc.gz")


def find_seq(kind, ts):
    """Largest sequence of `kind` whose state timestamp is <= ts (ISO strings compare correctly)."""
    step = {"minute": 60, "hour": 3600, "day": 86400}[kind]
    cur, cur_ts = state_of(kind)
    if cur_ts <= ts:
        return cur, cur_ts
    import datetime as dt
    p = lambda s: dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
    seq = cur - int((p(cur_ts) - p(ts)).total_seconds() // step)
    while True:
        s, s_ts = state_of(kind, seq)
        if s_ts > ts:
            seq -= 1
        else:
            nxt_ts = state_of(kind, seq + 1)[1] if seq + 1 <= cur else "9999"
            if nxt_ts <= ts:
                seq += 1
            else:
                return seq, s_ts


# ---- diff parsing ----------------------------------------------------------------------

def read_osc(blobs):
    """Newest version of each element across the diffs: {(type, id): dict}."""
    latest = {}
    for blob in blobs:
        data = gzip.decompress(blob) if blob[:2] == b"\x1f\x8b" else blob
        reader = osmium.io.Reader(osmium.io.FileBuffer(data, "osc"))
        for o in osmium.OsmFileIterator(reader) if hasattr(osmium, "OsmFileIterator") else _iter(reader):
            t = "n" if o.is_node() else "w" if o.is_way() else "r"
            rec = {"id": o.id, "deleted": o.deleted, "version": o.version, "ts": o.timestamp,
                   "changeset": o.changeset, "uid": o.uid, "user": o.user,
                   "tags": [(tg.k, tg.v) for tg in o.tags]}
            if t == "n" and not o.deleted:
                rec["lat"], rec["lon"] = o.location.y, o.location.x
            elif t == "w":
                rec["nds"] = [n.ref for n in o.nodes]
            elif t == "r":
                rec["members"] = [{"type": m.type, "ref": m.ref, "role": m.role} for m in o.members]
            old = latest.get((t, o.id))
            if old is None or old["version"] <= rec["version"]:
                latest[(t, o.id)] = rec
        reader.close()
    return latest


def _iter(reader):
    buf = []

    class H(osmium.SimpleHandler):
        def node(self, n):
            buf.append(osmium.osm.mutable.Node(n))

        def way(self, w):
            buf.append(osmium.osm.mutable.Way(w))

        def relation(self, r):
            buf.append(osmium.osm.mutable.Relation(r))
    osmium.apply(reader, H())
    return buf


# ---- applying ----------------------------------------------------------------------------

class Updater:
    def __init__(self, con, region=None):
        self.con = con
        con.sql(MACROS)
        self.e = Engine(con, "lake")
        self.region = region  # (s, w, n, e) or None for a planet lake

    def sql(self, q):
        return self.con.sql(q)

    def ids_sql(self, ids):
        return ",".join(map(str, sorted(ids))) or "NULL"

    def existing(self, t, ids):
        """id -> current Overpass index for elements present in the lake."""
        if not ids:
            return {}
        rows = self.sql(f"SELECT id, qt FROM lake.elem_locator WHERE type = '{t}' AND {self.e._id_pred('id', sorted(ids))}").fetchall()
        return dict(rows)

    def in_region(self, lat, lon):
        if self.region is None:
            return True
        s, w, n, e = self.region
        return s * 1e7 <= lat <= n * 1e7 and w * 1e7 <= lon <= e * 1e7

    def apply(self, blobs, osm_base=None, sequence=None):
        t0 = time.time()
        ch = read_osc(blobs)
        by = {t: {i: r for (tt, i), r in ch.items() if tt == t} for t in "nwr"}
        ex = {t: self.existing(t, by[t].keys()) for t in "nwr"}

        # 1. regional filter (the lake keeps what it already has, plus new elements in the region)
        nodes = {i: r for i, r in by["n"].items()
                 if i in ex["n"] or (not r["deleted"] and self.in_region(r["lat"], r["lon"]))}
        node_kept = {i for i, r in nodes.items() if not r["deleted"]}
        way_refs = {i for r in by["w"].values() if not r["deleted"] for i in r["nds"]}
        ref_exist = set(self.existing("n", way_refs - set(by["n"]))) | node_kept
        ways = {i: r for i, r in by["w"].items()
                if i in ex["w"] or (not r["deleted"] and any(n in ref_exist for n in r["nds"]))}
        # nodes a newly kept way needs, that the diff touched but the filter dropped, come along too
        for r in ways.values():
            for n in r.get("nds", []):
                if n in by["n"] and n not in nodes and not by["n"][n]["deleted"]:
                    nodes[n] = by["n"][n]
                    node_kept.add(n)
        way_kept = {i for i, r in ways.items() if not r["deleted"]}
        mem_n = {m["ref"] for r in by["r"].values() if not r["deleted"] for m in r["members"] if m["type"] == "n"}
        mem_w = {m["ref"] for r in by["r"].values() if not r["deleted"] for m in r["members"] if m["type"] == "w"}
        mem_r = {m["ref"] for r in by["r"].values() if not r["deleted"] for m in r["members"] if m["type"] == "r"}
        present = {"n": set(self.existing("n", mem_n)) | node_kept, "w": set(self.existing("w", mem_w)) | way_kept,
                   "r": set(self.existing("r", mem_r)) | {i for i, r in by["r"].items() if not r["deleted"]}}
        rels = {i: r for i, r in by["r"].items()
                if i in ex["r"] or (not r["deleted"] and any(m["ref"] in present[m["type"]] for m in r["members"]))}

        # 2. cascade: ways using changed nodes, relations using changed nodes/ways
        moved = [i for i, r in nodes.items() if i in ex["n"]]
        cascade_w = set()
        if moved:
            cascade_w = {w for (w,) in self.sql(
                f"SELECT DISTINCT way_id FROM lake.node_parent_ways WHERE {self.e._id_pred('node_id', sorted(moved))}").fetchall()}
        cascade_w -= set(ways)
        geo_ways = set(ways) | cascade_w
        cascade_r = set()
        for mt, ids in (("n", [i for i in nodes if i in ex["n"]]), ("w", sorted(geo_ways))):
            if ids:
                cascade_r |= {r for (r,) in self.sql(
                    f"SELECT DISTINCT rel_id FROM lake.member_parent_rels WHERE mtype = '{mt}' AND {self.e._id_pred('mid', sorted(ids))}").fetchall()}
        cascade_r -= set(rels)

        # 3. stage new rows in build-compatible temp tables
        self._stage_nodes(nodes)
        self._stage_ways(ways, cascade_w)
        self._stage_relations(rels, cascade_r)

        # 4. one transaction rewrites everything touched
        touched = {"n": set(nodes), "w": set(ways) | cascade_w, "r": set(rels) | cascade_r}
        self.sql("BEGIN TRANSACTION")
        try:
            self._rewrite(touched, ex, ways, rels)
            if osm_base:
                self.sql(f"UPDATE lake.state SET value = '{osm_base}' WHERE key = 'osm_base'")
            if sequence is not None:
                self.sql(f"UPDATE lake.state SET value = '{sequence}' WHERE key = 'sequence'")
            self.sql("COMMIT")
        except Exception:
            self.sql("ROLLBACK")
            raise
        self.e._locator_memo.clear()
        return {"changes": len(ch), "nodes": len(nodes), "ways": len(ways), "rels": len(rels),
                "cascade_ways": len(cascade_w), "cascade_rels": len(cascade_r), "secs": round(time.time() - t0, 2)}

    # -- staging (same column layout as extract.py output, then the build's key computation)

    def _register(self, name, cols, schema):
        self.con.register(f"{name}_arrow", pa.table(cols, schema=schema))
        self.sql(f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT * FROM {name}_arrow")
        self.con.unregister(f"{name}_arrow")

    def _stage_nodes(self, nodes):
        live = [r for r in nodes.values() if not r["deleted"]]
        schema = pa.schema([("id", pa.int64()), ("lat", pa.int32()), ("lon", pa.int32()), ("tags", TAGS_T), *META_T])
        self._register("upd_n_raw", {
            "id": [r["id"] for r in live], "lat": [r["lat"] for r in live], "lon": [r["lon"] for r in live],
            "tags": [r["tags"] for r in live], "version": [r["version"] for r in live], "ts": [r["ts"] for r in live],
            "changeset": [r["changeset"] for r in live], "uid": [r["uid"] for r in live],
            "user": [r["user"] for r in live]}, schema)
        self.sql('CREATE OR REPLACE TEMP TABLE upd_n AS SELECT ov_node_idx(lat, lon) AS qt, * FROM upd_n_raw')

    def _coords(self, refs):
        """node id -> (lat, lon) from the staged nodes, else from the lake."""
        out = dict(((i, (a, b)) for i, a, b in self.sql(
            f"SELECT id, lat, lon FROM upd_n WHERE id IN ({self.ids_sql(refs)})").fetchall()))
        rest = sorted(set(refs) - set(out))
        if rest:
            out.update((i, (a, b)) for i, a, b in self.e.elements_by_id("n", rest, ["id", "lat", "lon"]))
        return out

    def _stage_ways(self, ways, cascade):
        rows = [r for r in ways.values() if not r["deleted"]]
        if cascade:  # geometry-only changes keep tags/meta/refs from the lake
            meta = self.e.meta("w", sorted(cascade))
            for i, nds, tags in self.e.elements_by_id("w", sorted(cascade), ["id", "nds", "tags"]):
                version, ts, changeset, uid, user = meta.get(i, (0, None, 0, 0, ""))
                rows.append({"id": i, "nds": nds, "tags": list(tags.items()) if isinstance(tags, dict) else tags,
                             "version": version, "ts": ts, "changeset": changeset, "uid": uid, "user": user})
        xy = self._coords({n for r in rows for n in r["nds"]})
        schema = pa.schema([("id", pa.int64()), ("nds", pa.list_(pa.int64())), ("nd_lat", pa.list_(pa.int32())),
                            ("nd_lon", pa.list_(pa.int32())), ("tags", TAGS_T), *META_T])
        self._register("upd_w_raw", {
            "id": [r["id"] for r in rows], "nds": [r["nds"] for r in rows],
            "nd_lat": [[xy.get(n, (NO_COORD, NO_COORD))[0] for n in r["nds"]] for r in rows],
            "nd_lon": [[xy.get(n, (NO_COORD, NO_COORD))[1] for n in r["nds"]] for r in rows],
            "tags": [r["tags"] for r in rows], "version": [r["version"] for r in rows], "ts": [r["ts"] for r in rows],
            "changeset": [r["changeset"] for r in rows], "uid": [r["uid"] for r in rows],
            "user": [r["user"] for r in rows]}, schema)
        self.sql("""
            CREATE OR REPLACE TEMP TABLE upd_w AS
            WITH m AS (
              SELECT w.id, u.i AS seq, ov_node_idx(u.la, u.lo) AS nidx, u.la, u.lo
              FROM upd_w_raw w,
                   LATERAL (SELECT unnest(range(len(w.nds))) AS i, unnest(w.nd_lat) AS la, unnest(w.nd_lon) AS lo) u
              WHERE u.la <> -2147483648),
            agg AS (
              SELECT id, arg_min(nidx, seq) AS first_idx,
                     min(nidx & 715827882) AS lat_min, max(nidx & 715827882) AS lat_max,
                     min(nidx & 1431655765) AS lon_min, max(nidx & 1431655765) AS lon_max,
                     min(la) AS min_lat, max(la) AS max_lat, min(lo) AS min_lon, max(lo) AS max_lon
              FROM m GROUP BY id)
            SELECT ov_way_idx(a.first_idx, a.lat_min, a.lat_max, a.lon_min, a.lon_max) AS qt,
                   w.id, a.min_lat, a.max_lat, a.min_lon, a.max_lon, w.nds, w.nd_lat, w.nd_lon, w.tags,
                   w.version, w.ts, w.changeset, w.uid, w."user"
            FROM upd_w_raw w LEFT JOIN agg a USING (id)""")

    def _stage_relations(self, rels, cascade):
        rows = [r for r in rels.values() if not r["deleted"]]
        if cascade:
            meta = self.e.meta("r", sorted(cascade))
            for i, members, tags in self.e.elements_by_id("r", sorted(cascade), ["id", "members", "tags"]):
                version, ts, changeset, uid, user = meta.get(i, (0, None, 0, 0, ""))
                rows.append({"id": i, "members": members,
                             "tags": list(tags.items()) if isinstance(tags, dict) else tags,
                             "version": version, "ts": ts, "changeset": changeset, "uid": uid, "user": user})
        mn = sorted({m["ref"] for r in rows for m in r["members"] if m["type"] == "n"})
        mw = sorted({m["ref"] for r in rows for m in r["members"] if m["type"] == "w"})
        # member keys and extents: staged rows win over the lake
        n_info = {i: (q, a, b) for i, q, a, b in self.sql(
            f"SELECT id, qt, lat, lon FROM upd_n WHERE id IN ({self.ids_sql(mn)})").fetchall()}
        rest = [i for i in mn if i not in n_info]
        if rest:
            n_info.update((i, (q, a, b)) for i, q, a, b in self.e.elements_by_id("n", rest, ["id", "qt", "lat", "lon"]))
        w_info = {i: (q, a, b, c, d) for i, q, a, b, c, d in self.sql(
            f"SELECT id, qt, min_lat, max_lat, min_lon, max_lon FROM upd_w WHERE id IN ({self.ids_sql(mw)})").fetchall()}
        rest = [i for i in mw if i not in w_info]
        if rest:
            w_info.update((i, (q, a, b, c, d)) for i, q, a, b, c, d in self.e.elements_by_id(
                "w", rest, ["id", "qt", "min_lat", "max_lat", "min_lon", "max_lon"]))
        cols = {k: [] for k in ("qt", "id", "min_lat", "max_lat", "min_lon", "max_lon", "members", "tags",
                                "version", "ts", "changeset", "uid", "user")}
        for r in rows:
            idxs, lats, lons = set(), [], []
            for m in r["members"]:
                if m["type"] == "n" and m["ref"] in n_info:
                    q, a, b = n_info[m["ref"]]
                    idxs.add(q)
                    lats.append(a)
                    lons.append(b)
                elif m["type"] == "w" and m["ref"] in w_info:
                    q, a, b, c, d = w_info[m["ref"]]
                    idxs.add(q)
                    if a is not None:
                        lats += [a, b]
                        lons += [c, d]
            cols["qt"].append(qt.calc_index(sorted(idxs)))
            cols["id"].append(r["id"])
            cols["min_lat"].append(min(lats) if lats else None)
            cols["max_lat"].append(max(lats) if lats else None)
            cols["min_lon"].append(min(lons) if lons else None)
            cols["max_lon"].append(max(lons) if lons else None)
            for k in ("members", "tags", "version", "ts", "changeset", "uid", "user"):
                cols[k].append(r[k])
        schema = pa.schema([("qt", pa.uint32()), ("id", pa.int64()), ("min_lat", pa.int32()), ("max_lat", pa.int32()),
                            ("min_lon", pa.int32()), ("max_lon", pa.int32()), ("members", pa.list_(MEMBER_T)),
                            ("tags", TAGS_T), *META_T])
        self._register("upd_r", cols, schema)

    # -- rewrite

    def _del(self, table, t, ids, keycol, idcol="id", keys=None):
        """DELETE rows of ids, pruned by their known index keys (tables are sorted by key)."""
        if not ids:
            return
        pred = self.e._id_pred(idcol, sorted(ids))
        if keys:
            vals = [qt.key31(k) if keycol == "qt_key31" or (keycol == "k" and t != "n") else k for k in keys]
            pred = self.e._ranges_sql(keycol, self.e._value_ranges(vals)) + " AND " + pred
        self.sql(f"DELETE FROM lake.{table} WHERE {pred}")

    def _rewrite(self, touched, ex, ways, rels):
        old = {t: {i: ex[t][i] for i in touched[t] if i in ex[t]} for t in "nwr"}
        # cascade-only elements are in the lake too; look their keys up
        for t in "wr":
            missing = [i for i in touched[t] if i not in old[t]]
            old[t].update(self.existing(t, missing))
        old_way_refs = {}
        if old["w"]:
            for i, nds in self.e.elements_by_id("w", sorted(set(ways) & set(old["w"])), ["id", "nds"]):
                old_way_refs[i] = nds
        old_rel_members = {}
        if old["r"]:
            for i, members in self.e.elements_by_id("r", sorted(set(rels) & set(old["r"])), ["id", "members"]):
                old_rel_members[i] = members

        nk = list(old["n"].values())
        self._del("nodes_tagged", "n", old["n"], "qt", keys=nk)
        self._del("nodes_untagged", "n", old["n"], "qt", keys=nk)
        self._del("nodes_meta", "n", old["n"], "k", keys=nk)
        self.sql("INSERT INTO lake.nodes_tagged SELECT qt, id, lat, lon, tags FROM upd_n WHERE cardinality(tags) > 0")
        self.sql("INSERT INTO lake.nodes_untagged SELECT qt, id, lat, lon, tags FROM upd_n WHERE cardinality(tags) = 0")
        self.sql('INSERT INTO lake.nodes_meta SELECT qt, id, version, ts, changeset, uid, "user" FROM upd_n')

        wk = list(old["w"].values())
        self._del("ways", "w", old["w"], "qt_key31", keys=wk)
        self._del("ways_meta", "w", old["w"], "k", keys=wk)
        self.sql("""INSERT INTO lake.ways SELECT ov_key31(qt), ov_level(qt), qt, id, min_lat, max_lat, min_lon, max_lon,
                    tags, nds, nd_lat, nd_lon FROM upd_w""")
        self.sql('INSERT INTO lake.ways_meta SELECT ov_key31(qt), id, version, ts, changeset, uid, "user" FROM upd_w')

        rk = list(old["r"].values())
        self._del("relations", "r", old["r"], "qt_key31", keys=rk)
        self._del("relations_meta", "r", old["r"], "k", keys=rk)
        self.sql("""INSERT INTO lake.relations SELECT ov_key31(qt), ov_level(qt), qt, id, min_lat, max_lat, min_lon,
                    max_lon, tags, members FROM upd_r""")
        self.sql('INSERT INTO lake.relations_meta SELECT ov_key31(qt), id, version, ts, changeset, uid, "user" FROM upd_r')

        for t, src in (("n", "upd_n"), ("w", "upd_w"), ("r", "upd_r")):
            ids = old[t]
            if ids:
                self.sql(f"DELETE FROM lake.tag_postings WHERE type = '{t}' AND {self.e._id_pred('id', sorted(ids))}")
                self.sql(f"DELETE FROM lake.elem_locator WHERE type = '{t}' AND {self.e._id_pred('id', sorted(ids))}")
            self.sql(f"""INSERT INTO lake.tag_postings
                SELECT '{t}', k || '=' || v, k, v, ov_key31(qt), id FROM (
                  SELECT unnest(map_keys(tags)) AS k, unnest(map_values(tags)) AS v, qt, id FROM {src})""")
            self.sql(f"INSERT INTO lake.elem_locator SELECT '{t}', id, qt FROM {src}")

        # reverse indexes: only elements whose refs/members changed (cascade-only rows keep theirs)
        if old_way_refs:
            refs = sorted({n for nds in old_way_refs.values() for n in nds})
            self.sql(f"""DELETE FROM lake.node_parent_ways WHERE {self.e._id_pred('node_id', refs)}
                         AND way_id IN ({self.ids_sql(old_way_refs)})""")
        changed_ways = self.ids_sql({i for i, r in ways.items() if not r["deleted"]})
        self.sql(f"INSERT INTO lake.node_parent_ways SELECT DISTINCT unnest(nds), id FROM upd_w WHERE id IN ({changed_ways})")
        if old_rel_members:
            for mt in "nwr":
                mids = sorted({m["ref"] for ms in old_rel_members.values() for m in ms if m["type"] == mt})
                if mids:
                    self.sql(f"""DELETE FROM lake.member_parent_rels WHERE mtype = '{mt}'
                                 AND {self.e._id_pred('mid', mids)} AND rel_id IN ({self.ids_sql(old_rel_members)})""")
        changed_rels = self.ids_sql({i for i, r in rels.items() if not r["deleted"]})
        self.sql(f"""INSERT INTO lake.member_parent_rels
                     SELECT DISTINCT m.type, m.ref, m.id FROM (SELECT id, unnest(members, recursive := true) FROM upd_r
                     WHERE id IN ({changed_rels})) m""")


def compact(con):
    """Periodic maintenance: inlined rows -> sorted Parquet, small files merged, old snapshots dropped."""
    for q in ("CALL ducklake_flush_inlined_data('lake')", "CALL ducklake_merge_adjacent_files('lake')",
              "CALL ducklake_rewrite_data_files('lake')", "CALL ducklake_expire_snapshots('lake', older_than => now() - INTERVAL 1 HOUR)",
              "CALL ducklake_cleanup_old_files('lake', older_than => now() - INTERVAL 1 HOUR)"):
        t = time.time()
        try:
            con.sql(q)
            print(f"{q:<90} {time.time() - t:6.1f}s")
        except duckdb.Error as e:
            print(f"{q:<90} skipped: {str(e).splitlines()[0][:80]}")


def follow(up, once=False, poll=30):
    """Catch up with day, then hour, then minute diffs; then poll the minutely feed."""
    base = up.sql("SELECT value FROM lake.state WHERE key = 'osm_base'").fetchall()[0][0]
    for kind in ("day", "hour", "minute"):
        start, start_ts = find_seq(kind, base)
        if start_ts != base and kind != "minute":
            continue  # the lake isn't aligned to this granularity; try a finer one
        cur, _ = state_of(kind)
        if kind != "minute" and cur == start:
            continue
        seq = start + 1
        while seq <= cur:
            s, ts = state_of(kind, seq)
            stats = up.apply([osc(kind, seq)], osm_base=ts, sequence=f"{kind}/{seq}")
            print(f"{kind}/{seq} -> {ts} {stats}", flush=True)
            base = ts
            seq += 1
            if kind == "minute" and seq > cur and not once:
                while True:
                    cur, _ = state_of("minute")
                    if cur >= seq:
                        break
                    time.sleep(poll)
        if once and kind == "minute":
            return


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["run", "apply", "compact"])
    ap.add_argument("files", nargs="*")
    ap.add_argument("--region", help="s,w,n,e: keep a regional extract consistent (omit for a planet lake)")
    ap.add_argument("--once", action="store_true", help="catch up and exit instead of following")
    args = ap.parse_args()
    con = duckdb.connect()
    lakeconn.attach(con)
    if args.cmd == "compact":
        return compact(con)
    region = tuple(map(float, args.region.split(","))) if args.region else None
    up = Updater(con, region)
    if args.cmd == "apply":
        print(up.apply([pathlib.Path(f).read_bytes() for f in args.files]))
    else:
        follow(up, once=args.once)


if __name__ == "__main__":
    main()
