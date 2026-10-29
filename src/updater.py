"""Minutely updater: applies OSM replication diffs (osmChange) to the lake.

  python src/updater.py run [--region s,w,n,e] [--once]    follow replication (day -> hour -> minute catch-up)
  python src/updater.py apply FILE.osc.gz ...               apply local diffs
  python src/updater.py compact                             flush inlined data, merge files, expire snapshots
  python src/updater.py publish s3://bucket/lake/catalog.ducklake     publish a frozen catalog (src/publish.py)

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
import os
import pathlib
import sys
import threading
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


STOP = threading.Event()  # set by SIGTERM: finish the diff in hand, upload, release the lease, exit


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
    import tempfile
    latest = {}
    for blob in blobs:
        with tempfile.NamedTemporaryFile(suffix=".osc.gz" if blob[:2] == b"\x1f\x8b" else ".osc") as f:
            f.write(blob)
            f.flush()
            for o in osmium.FileProcessor(f.name):
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
    return latest


# ---- applying ----------------------------------------------------------------------------

def migrate(con):
    """Bring an older lake's schema up to date (idempotent)."""
    cols = {c for (c,) in con.sql("SELECT column_name FROM duckdb_columns() WHERE database_name = 'lake' "
                                  "AND table_name = 'changes'").fetchall()}
    if "batch" not in cols:  # monotonic per applied diff: lets the area build process only new changes
        con.sql("ALTER TABLE lake.changes ADD COLUMN batch BIGINT")
    if not con.sql("SELECT 1 FROM lake.state WHERE key = 'batch'").fetchall():
        con.sql("INSERT INTO lake.state VALUES ('batch', '0')")


class Updater:
    def __init__(self, con, region=None):
        self.con = con
        con.sql(MACROS)
        self.e = Engine(con, "lake")
        self.region = region  # (s, w, n, e) or None for a planet lake
        migrate(con)

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
                   "r": set(self.existing("r", mem_r))}
        # fixpoint: a new relation is kept if it has a kept member, including relations kept this round
        rels = {}
        while True:
            added = {i: r for i, r in by["r"].items() if i not in rels and
                     (i in ex["r"] or (not r["deleted"] and any(m["ref"] in present[m["type"]] for m in r["members"])))}
            if not added:
                break
            rels.update(added)
            present["r"] |= {i for i, r in added.items() if not r["deleted"]}

        # 2. cascade: ways using moved nodes, relations using moved nodes/changed ways. Like Overpass, a
        # tags-only node edit leaves its ways and relations alone.
        old_pos = {i: (la, lo) for i, la, lo in self.e.elements_by_id(
            "n", [i for i in nodes if i in ex["n"]], ["id", "lat", "lon"])} if ex["n"] else {}
        moved = [i for i, r in nodes.items() if i in old_pos and
                 (r["deleted"] or (r["lat"], r["lon"]) != old_pos[i])]
        cascade_w = set()
        if moved:
            pairs = self.sql(f"SELECT DISTINCT node_id, way_id FROM lake.node_parent_ways "
                             f"WHERE {self.e._id_pred('node_id', sorted(moved))}").fetchall()
            cascade_w = {w for _, w in pairs}
            way_moves = {}
            for n, w in pairs:
                way_moves.setdefault(w, []).append(nodes[n]["ts"])
        else:
            way_moves = {}
        cascade_w -= set(ways)
        # change time per element (valid_from of the new row, valid_to of the replaced one): its own version
        # timestamp, or for geometry-only changes the latest move that caused it
        ct = {("n", i): r["ts"] for i, r in nodes.items()}
        ct.update((("w", i), r["ts"]) for i, r in ways.items())
        ct.update((("r", i), r["ts"]) for i, r in rels.items())
        for w, times in way_moves.items():
            ct[("w", w)] = max(times + ([ct[("w", w)]] if ("w", w) in ct else []))
        # stage nodes and ways now: a relation cascades only from member ways whose coordinates changed
        # (a tags-only way edit leaves its relations alone, as in Overpass)
        self.batch = {"n": nodes}
        self._stage_nodes(nodes)
        self._stage_ways(ways, cascade_w)
        mod_w = sorted(i for i, r in ways.items() if i in ex["w"] and not r["deleted"])
        new_xy = {i: (la, lo) for i, la, lo in self.sql(
            f"SELECT id, nd_lat, nd_lon FROM upd_w WHERE id IN ({self.ids_sql(mod_w)})").fetchall()} if mod_w else {}
        old_xy = {i: (la, lo) for i, la, lo in self.e.elements_by_id("w", mod_w, ["id", "nd_lat", "nd_lon"])}
        geo_ways = (cascade_w | {i for i, r in ways.items() if i in ex["w"] and r["deleted"]}
                    | {i for i in mod_w if new_xy.get(i) != old_xy.get(i)})
        cascade_r, rel_moves = set(), {}
        for mt, ids in (("n", moved), ("w", sorted(geo_ways))):
            if ids:
                for mid, rel in self.sql(f"SELECT DISTINCT mid, rel_id FROM lake.member_parent_rels WHERE mtype = '{mt}' "
                                         f"AND {self.e._id_pred('mid', sorted(ids))}").fetchall():
                    cascade_r.add(rel)
                    if (mt, mid) in ct:
                        rel_moves.setdefault(rel, []).append(ct[(mt, mid)])
        cascade_r -= set(rels)
        for r_, times in rel_moves.items():
            ct[("r", r_)] = max(times + ([ct[("r", r_)]] if ("r", r_) in ct else []))
        self.change_time = ct

        # 3. stage the relations (nodes and ways are staged above)
        self._stage_relations(rels, cascade_r)

        # 4. one transaction rewrites everything touched
        touched = {"n": set(nodes), "w": set(ways) | cascade_w, "r": set(rels) | cascade_r}
        self.sql("BEGIN TRANSACTION")
        try:
            self.batch_no = int(self.sql("SELECT value FROM lake.state WHERE key = 'batch'").fetchall()[0][0]) + 1
            self.sql(f"UPDATE lake.state SET value = '{self.batch_no}' WHERE key = 'batch'")
            self._rewrite(touched, ex, ways, rels)
            self._update_lance(touched)
            if osm_base:
                self.sql(f"UPDATE lake.state SET value = '{osm_base}' WHERE key = 'osm_base'")
            if sequence is not None:
                self.sql(f"UPDATE lake.state SET value = '{sequence}' WHERE key = 'sequence'")
            self.sql("COMMIT")
        except Exception:
            try:
                self.sql("ROLLBACK")
            except duckdb.TransactionException:
                pass  # a failed COMMIT has already ended the transaction
            raise
        self.e._locator_memo.clear()
        return {"changes": len(ch), "nodes": len(nodes), "ways": len(ways), "rels": len(rels),
                "cascade_ways": len(cascade_w), "cascade_rels": len(cascade_r), "secs": round(time.time() - t0, 2)}

    def _update_lance(self, touched):
        """Keep the Lance id store (src/lancestore.py) in step; its new versions are recorded in
        lake.state inside this diff's transaction."""
        import lancestore
        base, versions = lancestore.state_versions(self.con)
        if not base:
            return
        rows = {
            "nodes_tagged": self.sql("SELECT qt, id, lat, lon, tags, ts AS valid_from FROM upd_n "
                                     "WHERE cardinality(tags) > 0").arrow().read_all(),
            "nodes_untagged": self.sql("SELECT qt, id, lat, lon, tags, ts AS valid_from FROM upd_n "
                                       "WHERE cardinality(tags) = 0").arrow().read_all(),
            "ways": self.sql("""SELECT ov_key31(w.qt) AS qt_key31, ov_level(w.qt) AS qt_level, w.qt, w.id, w.min_lat,
                                w.max_lat, w.min_lon, w.max_lon, w.tags, w.nds, w.nd_lat, w.nd_lon,
                                coalesce(x.t, w.ts) AS valid_from
                                FROM upd_w w LEFT JOIN upd_ct x ON x.type = 'w' AND x.id = w.id""").arrow().read_all(),
        }
        new = lancestore.apply(self.con, base, versions, {"n": touched["n"], "w": touched["w"]}, rows)
        lancestore._set_state(self.con, base, new)

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

    def _where(self, t, ids, keycol, idcol="id", keys=None):
        """Predicate selecting rows of ids, pruned by their known index keys (tables are sorted by key)."""
        pred = self.e._id_pred(idcol, sorted(ids))
        if keys:
            vals = [qt.key31(k) if keycol == "qt_key31" or (keycol == "k" and t != "n") else k for k in keys]
            pred = self.e._ranges_sql(keycol, self.e._value_ranges(vals)) + " AND " + pred
        return pred

    def _del(self, table, t, ids, keycol, idcol="id", keys=None):
        if ids:
            self.sql(f"DELETE FROM lake.{table} WHERE {self._where(t, ids, keycol, idcol, keys)}")

    def _archive(self, t, ids, keys):
        """Copy the outgoing versions of ids, with their metadata, into the history table."""
        if not ids:
            return
        meta = f"(SELECT * FROM lake.{ {'n': 'nodes', 'w': 'ways', 'r': 'relations'}[t]}_meta WHERE {self._where(t, ids, 'k', keys=keys)})"
        if t == "n":
            cur = (f"(SELECT * FROM lake.nodes_tagged WHERE {self._where('n', ids, 'qt', keys=keys)} UNION ALL "
                   f"SELECT * FROM lake.nodes_untagged WHERE {self._where('n', ids, 'qt', keys=keys)})")
            hist = "nodes_history"
        else:
            hist = {"w": "ways_history", "r": "relations_history"}[t]
            cur = f"(SELECT * FROM lake.{ {'w': 'ways', 'r': 'relations'}[t]} WHERE {self._where(t, ids, 'qt_key31', keys=keys)})"
        self.sql(f"""INSERT INTO lake.{hist}
            SELECT c.*, x.t AS valid_to, m.version, m.ts, m.changeset, m.uid, m."user"
            FROM {cur} c JOIN upd_ct x ON x.type = '{t}' AND x.id = c.id
            LEFT JOIN {meta} m ON m.id = c.id""")

    def _log_changes(self, touched, old, direct):
        """lake.changes: one row per element and batch (drives changed / [diff] / [adiff])."""
        rows = []
        for t in "nwr":
            old_meta = self.e.meta(t, sorted(old[t])) if old[t] else {}
            for i in touched[t]:
                r = direct[t].get(i)
                ov = old_meta.get(i, (None,))[0]
                if r is None:
                    action, nv = "geometry", ov
                elif r["deleted"]:
                    action, nv = "delete", None
                else:
                    action, nv = ("modify" if i in old[t] else "create"), r["version"]
                rows.append((self.change_time.get((t, i)), t, i, ov, nv, action))
        self.con.register("chg_arrow", pa.table({
            "ts": pa.array([r[0] for r in rows], pa.timestamp("s", tz="UTC")), "type": [r[1] for r in rows],
            "id": pa.array([r[2] for r in rows], pa.int64()), "old_version": pa.array([r[3] for r in rows], pa.int32()),
            "new_version": pa.array([r[4] for r in rows], pa.int32()), "action": [r[5] for r in rows],
            "batch": pa.array([self.batch_no] * len(rows), pa.int64())}))
        self.sql("INSERT INTO lake.changes BY NAME SELECT * FROM chg_arrow")
        self.con.unregister("chg_arrow")

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

        ct = self.change_time
        self.con.register("upd_ct_arrow", pa.table({
            "type": [k[0] for k in ct], "id": pa.array([k[1] for k in ct], pa.int64()),
            "t": pa.array(list(ct.values()), pa.timestamp("s", tz="UTC"))}))
        self.sql("CREATE OR REPLACE TEMP TABLE upd_ct AS SELECT * FROM upd_ct_arrow")
        self.con.unregister("upd_ct_arrow")
        self._log_changes(touched, old, {"n": self.batch["n"], "w": ways, "r": rels})
        for t in "nwr":
            self._archive(t, old[t], list(old[t].values()))

        nk = list(old["n"].values())
        self._del("nodes_tagged", "n", old["n"], "qt", keys=nk)
        self._del("nodes_untagged", "n", old["n"], "qt", keys=nk)
        self._del("nodes_meta", "n", old["n"], "k", keys=nk)
        self.sql("INSERT INTO lake.nodes_tagged SELECT qt, id, lat, lon, tags, ts FROM upd_n WHERE cardinality(tags) > 0")
        self.sql("INSERT INTO lake.nodes_untagged SELECT qt, id, lat, lon, tags, ts FROM upd_n WHERE cardinality(tags) = 0")
        self.sql('INSERT INTO lake.nodes_meta SELECT qt, id, version, ts, changeset, uid, "user" FROM upd_n')

        wk = list(old["w"].values())
        self._del("ways", "w", old["w"], "qt_key31", keys=wk)
        self._del("ways_meta", "w", old["w"], "k", keys=wk)
        self.sql("""INSERT INTO lake.ways SELECT ov_key31(w.qt), ov_level(w.qt), w.qt, w.id, w.min_lat, w.max_lat, w.min_lon,
                    w.max_lon, w.tags, w.nds, w.nd_lat, w.nd_lon, coalesce(x.t, w.ts)
                    FROM upd_w w LEFT JOIN upd_ct x ON x.type = 'w' AND x.id = w.id""")
        self.sql('INSERT INTO lake.ways_meta SELECT ov_key31(qt), id, version, ts, changeset, uid, "user" FROM upd_w')

        rk = list(old["r"].values())
        self._del("relations", "r", old["r"], "qt_key31", keys=rk)
        self._del("relations_meta", "r", old["r"], "k", keys=rk)
        self.sql("""INSERT INTO lake.relations SELECT ov_key31(r.qt), ov_level(r.qt), r.qt, r.id, r.min_lat, r.max_lat,
                    r.min_lon, r.max_lon, r.tags, r.members, coalesce(x.t, r.ts)
                    FROM upd_r r LEFT JOIN upd_ct x ON x.type = 'r' AND x.id = r.id""")
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


def compaction_steps(grace=3600):
    """The compaction as (label, run(con)) steps: inlined rows -> sorted Parquet, small files merged,
    old snapshots dropped, Lance fragments merged. Files are deleted only `grace` seconds after they
    stop being referenced, so readers still on an older catalog (or a frozen copy) keep working;
    orphans from discarded job runs go the same way."""
    sql = ("CALL ducklake_flush_inlined_data('lake')", "CALL ducklake_merge_adjacent_files('lake')",
           "CALL ducklake_rewrite_data_files('lake')",
           f"CALL ducklake_expire_snapshots('lake', older_than => now() - INTERVAL {int(grace)} SECOND)",
           f"CALL ducklake_cleanup_old_files('lake', older_than => now() - INTERVAL {int(grace)} SECOND)",
           f"CALL ducklake_delete_orphaned_files('lake', older_than => now() - INTERVAL {int(grace)} SECOND)")
    import lancestore
    return ([(q, lambda con, q=q: con.sql(q)) for q in sql]
            + [(f"lance maintain {name}", lambda con, name=name: lancestore.maintain(con, grace, [name]))
               for name in lancestore.TABLES])


def run_step(con, label, run):
    t = time.time()
    try:
        run(con)
        if not label.startswith("lance"):  # lance maintain prints its own line
            print(f"{label:<90} {time.time() - t:6.1f}s", flush=True)
    except duckdb.Error as e:
        print(f"{label:<90} skipped: {str(e).splitlines()[0][:80]}", flush=True)


def compact(con, grace=3600):
    """All the compaction steps in a row (the CLI and the `run` mode)."""
    for label, run in compaction_steps(grace):
        run_step(con, label, run)


COMPACTION = None  # job --loop: the steps still to run, one per cycle, so diffs flow between them


def compact_due(con, args, state):
    """job: run compaction if it is due; True if any step ran. With --loop one step runs per cycle:
    a flush or a Lance merge takes minutes on a big lake, and the updater is the only thing keeping the
    lake current, so it applies diffs between steps instead of stalling for the whole compaction. A
    one-shot job (a new process each run) runs all the steps."""
    global COMPACTION
    if COMPACTION is None:
        if not args.compact_every or time.time() - float(state.get("compacted_at", "0")) < args.compact_every:
            return False
        COMPACTION = compaction_steps(args.file_grace)
    for _ in range(1 if args.loop else len(COMPACTION)):
        label, run = COMPACTION.pop(0)
        run_step(con, label, run)
    if not COMPACTION:
        COMPACTION = None
        con.sql("DELETE FROM lake.state WHERE key = 'compacted_at'")
        con.sql(f"INSERT INTO lake.state VALUES ('compacted_at', '{time.time():.0f}')")
    return True


class Maintenance:
    """Periodic jobs between batches: areas (like Overpass's rules loop) and compaction."""

    def __init__(self, con, areas_every=3600, compact_every=3600, publish_to=None, publish_every=300):
        self.con, self.jobs, self.publish_to = con, [], publish_to
        if areas_every:
            self.jobs.append(["areas", areas_every, time.time()])
        if compact_every:
            self.jobs.append(["compact", compact_every, time.time()])
        if publish_to and publish_every:
            self.jobs.append(["publish", publish_every, 0])  # publish right after the first batch

    def tick(self):
        for job in self.jobs:
            name, every, last = job
            if time.time() - last < every:
                continue
            t = time.time()
            if name == "areas":
                import build_areas
                build_areas.build(self.con)
            elif name == "publish":
                import publish
                publish.publish(self.con, self.publish_to)
            else:
                compact(self.con)
            job[2] = time.time()
            print(f"maintenance {name} {time.time() - t:.1f}s", flush=True)


def apply_with_retry(up, blob, ts, sequence, attempts=5):
    """A batch is one transaction, so a failed commit (e.g. catalog lock contention) can be retried."""
    for i in range(attempts):
        try:
            return up.apply([blob], osm_base=ts, sequence=sequence)
        except duckdb.TransactionException as e:
            if i == attempts - 1:
                raise
            print(f"{sequence}: commit failed ({str(e).splitlines()[-1][:100]}), retrying", flush=True)
            time.sleep(2 ** i)


def follow(up, once=False, poll=30, kinds=("hour", "minute"), maintenance=None, until=None, deadline=None):
    """Catch up with coarse diffs (hourly by default: a planet day diff is ~100 MB, too much to hold
    in one batch), then minute diffs; then poll the minutely feed. Returns the number of diffs applied.
    `deadline` (epoch seconds) ends a scheduled job's run between diffs."""
    applied = 0
    base = up.sql("SELECT value FROM lake.state WHERE key = 'osm_base'").fetchall()[0][0]
    for kind in kinds:
        start, start_ts = find_seq(kind, base)
        if start_ts != base and kind != "minute":
            continue  # the lake isn't aligned to this granularity; try a finer one
        cur, _ = state_of(kind)
        if kind != "minute" and cur == start:
            continue
        seq = start + 1
        while seq <= cur:
            s, ts = state_of(kind, seq)
            if until is not None and ts > until:
                return applied  # reached the requested point (tests replay a fixed window, not up to now)
            if STOP.is_set() or (deadline is not None and time.time() > deadline):
                return applied  # the next job continues from state.osm_base
            stats = apply_with_retry(up, osc(kind, seq), ts, f"{kind}/{seq}")
            applied += 1
            print(f"{kind}/{seq} -> {ts} {stats}", flush=True)
            if maintenance is not None:
                maintenance.tick()
            base = ts
            seq += 1
            if kind == "minute" and seq > cur and not once:
                while True:
                    cur, _ = state_of("minute")
                    if cur >= seq or STOP.is_set():
                        break
                    time.sleep(poll)
        if once and kind == "minute":
            return applied
    return applied


def job(args):
    """Updater runs against a catalog on object storage (src/objcatalog.py), holding the writer lease.
    One run (default): download the catalog, apply pending diffs until caught up or out of time,
    update areas, compact when due, upload it conditionally, exit; scheduled, nothing runs between
    runs. --loop: keep running and keep the catalog on local disk, uploading after each batch of
    diffs (no download per run; for an always-on host)."""
    import objcatalog
    import signal
    t0 = time.time()
    ttl = args.lease_seconds or args.max_seconds + 600
    # systemd stops a service with SIGTERM: finish the diff in hand, upload, and release the lease
    # (the `finally`), so the restarted job doesn't wait out the old one's TTL
    signal.signal(signal.SIGTERM, lambda *_: STOP.set())
    lease = objcatalog.acquire_lease(args.catalog_url, ttl=ttl)
    if not lease:
        print("job: another run holds the lease; exiting", flush=True)
        return 3
    try:
        return _loop(args, lease, ttl) if args.loop else _job(args, t0)
    finally:
        objcatalog.release_lease(args.catalog_url)


def _cycle(local, args, deadline):
    """Attach the local catalog, apply what's pending, update areas, compact when due, detach.
    Returns (diffs applied, catalog changed, state)."""
    import build_areas
    con = duckdb.connect()
    lakeconn.attach(con, catalog=local, data_path="")  # the catalog knows its data path
    try:
        region = tuple(map(float, args.region.split(","))) if args.region else None
        up = Updater(con, region)
        n = follow(up, once=True, until=args.until, deadline=deadline)
        if n and args.areas_every:
            build_areas.build(con)
        state = dict(con.sql("SELECT key, value FROM lake.state").fetchall())
        compacted = compact_due(con, args, state)
        con.sql("DETACH lake")  # the file is complete and closed before it is uploaded
        return n, bool(n) or compacted, state
    finally:
        con.close()


def _commit(args, local, etag, state):
    """Conditional upload; returns the new ETag, or None if another writer committed first."""
    import objcatalog
    try:
        new = objcatalog.push(local, args.catalog_url, etag)
    except objcatalog.Conflict as e:
        print(f"job: {e}; discarding this run", flush=True)
        return None
    if args.keep_versions:
        prefix = args.catalog_url.rsplit("/", 1)[0] + "/catalogs"
        objcatalog.copy(args.catalog_url, prefix + f"/{state.get('osm_base', 'unknown').replace(':', '')}.ducklake")
        objcatalog.prune(prefix, args.keep_versions)
    return new


def _job(args, t0):
    import tempfile
    import objcatalog
    with tempfile.TemporaryDirectory(dir=args.workdir) as tmp:
        local = os.path.join(tmp, "catalog.ducklake")
        etag = objcatalog.fetch(args.catalog_url, local)
        n, changed, state = _cycle(local, args, t0 + args.max_seconds)
        if not changed:
            print(f"job: nothing new at {state.get('osm_base')}", flush=True)
            return 0
        if _commit(args, local, etag, state) is None:
            return 2
    print(f"job: {n} diffs to {state.get('osm_base')}, committed in {time.time() - t0:.0f}s", flush=True)
    return 0


def _loop(args, lease, ttl):
    """Always-on updater: the catalog stays on local disk between batches; each upload is conditional
    on the previous upload's ETag, and the lease is renewed every cycle. Uploads happen at most every
    --upload-every seconds (at the next cycle after that, so within --poll more), and at once when
    stopping or at --until: a crash loses only the batches since the last upload, which the next start
    applies again."""
    import objcatalog
    workdir = args.workdir or "."
    local = os.path.join(workdir, "catalog.ducklake")
    etag = objcatalog.fetch(args.catalog_url, local)
    print(f"job --loop: catalog {args.catalog_url} -> {local}", flush=True)
    dirty, unsent, last_upload = False, 0, 0.0
    while True:
        t = time.time()
        n, changed, state = _cycle(local, args, t + args.max_seconds)
        dirty, unsent = dirty or changed, unsent + n
        reached = args.until and state.get("osm_base", "") >= args.until
        if dirty and (not args.upload_every or STOP.is_set() or reached
                      or time.time() - last_upload >= args.upload_every):
            etag = _commit(args, local, etag, state)
            if etag is None:
                return 2  # someone else wrote the catalog: stop rather than fork it
            print(f"job --loop: {unsent} diffs to {state.get('osm_base')}, committed in {time.time() - t:.0f}s", flush=True)
            dirty, unsent, last_upload = False, 0, time.time()
        lease = objcatalog.renew_lease(args.catalog_url, lease, ttl)
        if not lease:
            print("job --loop: lost the writer lease; exiting", flush=True)
            return 3
        if STOP.is_set() or reached:
            return 0
        if not n and COMPACTION is None:
            STOP.wait(args.poll)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["run", "job", "apply", "compact", "publish"])
    ap.add_argument("files", nargs="*")
    ap.add_argument("--region", help="s,w,n,e: keep a regional extract consistent (omit for a planet lake)")
    ap.add_argument("--once", action="store_true", help="catch up and exit instead of following")
    ap.add_argument("--until", help="stop before the first diff ending after this timestamp (fixed test windows)")
    ap.add_argument("--areas-every", type=int, default=300, help="seconds between incremental area updates (0: never)")
    ap.add_argument("--compact-every", type=int, default=3600, help="seconds between compactions (0: never)")
    ap.add_argument("--publish-to", help="publish a frozen catalog here (e.g. s3://bucket/lake/catalog.ducklake)")
    ap.add_argument("--publish-every", type=int, default=300, help="seconds between frozen-catalog publications")
    ap.add_argument("--catalog-url", help="job: the catalog on object storage (s3://bucket/lake/catalog.ducklake)")
    ap.add_argument("--max-seconds", type=float, default=240, help="job: stop applying diffs after this long")
    ap.add_argument("--keep-versions", nargs="?", type=int, const=24, default=0, metavar="N",
                    help="job: also copy each committed catalog to catalogs/, keeping the newest N (default 24)")
    ap.add_argument("--upload-every", type=float, default=0, metavar="SECONDS",
                    help="job --loop: upload the catalog at most this often (default 0: after every batch)")
    ap.add_argument("--file-grace", type=int, default=3600, help="seconds before unreferenced files are deleted")
    ap.add_argument("--workdir", help="job: local scratch for the catalog (default: system temp)")
    ap.add_argument("--loop", action="store_true",
                    help="job: keep running with the catalog on local disk (always-on host; --workdir keeps it)")
    ap.add_argument("--poll", type=float, default=30, help="job --loop: seconds to wait when nothing is new")
    ap.add_argument("--lease-seconds", type=float, help="job: writer lease length (default: --max-seconds + 600); "
                                                        "set it above the longest run, e.g. a compaction")
    args = ap.parse_args()
    if args.cmd == "job":
        sys.exit(job(args))
    con = duckdb.connect()
    lakeconn.attach(con)
    if args.cmd == "compact":
        return compact(con)
    if args.cmd == "publish":
        import publish
        return publish.publish(con, args.publish_to or args.files[0])
    region = tuple(map(float, args.region.split(","))) if args.region else None
    up = Updater(con, region)
    if args.cmd == "apply":
        print(up.apply([pathlib.Path(f).read_bytes() for f in args.files]))
    else:
        follow(up, once=args.once, maintenance=Maintenance(con, args.areas_every, args.compact_every,
                                                           args.publish_to, args.publish_every), until=args.until)


if __name__ == "__main__":
    main()
