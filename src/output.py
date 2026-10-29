"""Overpass `out` statement and output formats (statements/print.cc, output_formats/output_*.cc).

A Document streams the output of every `out` statement in a request as Overpass XML, JSON or CSV. Element field order and presence follow the C++ writers; coordinates
are printed with 7 decimals like Overpass.
"""
import datetime as dt
import json
from dataclasses import dataclass, field
from xml.sax.saxutils import quoteattr

import quadtile

# Output_Mode bits (frontend/output_handler.h)
ID, COORDS, NDS, MEMBERS, TAGS, VERSION, META, GEOMETRY, BOUNDS, CENTER, COUNT = (
    0x1, 0x2, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80, 0x100, 0x200, 0x400)
MODES = {
    "ids": ID,
    "skel": ID | COORDS | NDS | MEMBERS,
    "body": ID | COORDS | NDS | MEMBERS | TAGS,
    "tags": ID | TAGS,
    "meta": ID | COORDS | NDS | MEMBERS | TAGS | VERSION | META,
    "count": ID | COUNT,
}
GEOMS = {None: 0, "geom": GEOMETRY | BOUNDS, "bb": BOUNDS, "center": CENTER}
NO_COORD = -2147483648
TYPE_NAME = {"n": "node", "w": "way", "r": "relation", "a": "area"}
COPYRIGHT = "The data included in this document is from www.openstreetmap.org. The data is made available under ODbL."
GENERATOR = "Overpass API 0.7.62 lake-spike"


@dataclass
class OutOptions:
    """`out [mode] [geometry] [order] [limit] [noids]` (the mode default is body)."""
    mode: str = "body"
    geometry: str | None = None
    order: str = "asc"            # asc (by id) or qt (by quadtile index)
    limit: int | None = None
    noids: bool = False
    clip: object = None           # out geom(s,w,n,e): engine.Bbox to clip geometry to

    def bits(self):
        b = MODES[self.mode] | GEOMS[self.geometry]
        return b & ~ID if self.noids else b


@dataclass
class CsvSettings:
    fields: list = field(default_factory=lambda: [("::id", True)])   # (name, is_special)
    header: bool = True
    separator: str = "\t"


def f7(v_e7):
    return round(v_e7 / 1e7, 7)


def iso(ts):
    if ts is None:
        return ""
    if isinstance(ts, (int, float)):
        ts = dt.datetime.fromtimestamp(ts, dt.timezone.utc)
    return ts.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def tag_items(tags):
    if not tags:
        return []
    return list(tags.items()) if isinstance(tags, dict) else list(tags)


def bounds_of(coords):
    """(minlat, minlon, maxlat, maxlon) in 1e-7 ints from (lat, lon) pairs, ignoring missing nodes."""
    pts = [(a, b) for a, b in coords if a != NO_COORD]
    if not pts:
        return None
    lats, lons = [p[0] for p in pts], [p[1] for p in pts]
    return min(lats), min(lons), max(lats), max(lons)


def _inside(p, clip):
    """Way_Bbox_Geometry_Store::matches_bbox (integer comparison like the node bbox filter)."""
    s7, w7, n7, e7 = clip.ints()
    return p[0] != NO_COORD and s7 <= p[0] <= n7 and w7 <= p[1] <= e7


def _seg_hits(a, b, clip):
    """Bbox_Double::intersects for west <= east, on Overpass-rounded coordinates."""
    if a[0] == NO_COORD or b[0] == NO_COORD:
        return False
    s, n, w, e = clip.s, clip.n, clip.w, clip.e
    flat, flon = (a[0] + 910000000) / 1e7 - 91.0, a[1] / 1e7
    tlat, tlon = (b[0] + 910000000) / 1e7 - 91.0, b[1] / 1e7
    if (flat < s and tlat < s) or (flat > n and tlat > n):
        return False
    df = (tlon - flon) * (s - flat) / (tlat - flat) if flat < s else (tlon - flon) * (n - flat) / (tlat - flat) if flat > n else 0
    dt = (flon - tlon) * (s - tlat) / (flat - tlat) if tlat < s else (flon - tlon) * (n - tlat) / (flat - tlat) if tlat > n else 0
    lo, hi = sorted((flon + df, tlon + dt))
    return lo <= e and hi >= w


def clip_way(coords, clip):
    """Way_Bbox_Geometry_Store::get_geometry, loop quirks included: keep a vertex if it or a neighbour is
    inside or an adjacent segment crosses the box; others become NO_COORD (printed as null)."""
    res = list(coords)
    n, i, last_dropped = len(res), 0, False
    while i < n:
        if i < n - 1 and _inside(coords[i + 1], clip):
            i += 3  # the next and the following point are kept
            last_dropped = False
            continue
        if not last_dropped:
            if _inside(coords[i], clip):
                i += 2
                last_dropped = False
                continue
            if i > 0 and _inside(coords[i - 1], clip):
                i += 1
                last_dropped = False
                continue
        if i < n - 1 and _seg_hits(coords[i], coords[i + 1], clip):
            i += 2
            last_dropped = False
            continue
        if not last_dropped and i > 0 and _seg_hits(coords[i - 1], coords[i], clip):
            i += 1
            last_dropped = False
            continue
        res[i] = (NO_COORD, NO_COORD)
        last_dropped = True
        i += 1
    return res


def clip_member_way(coords, clip):
    """Relation_Geometry_Store::get_geometry: a member-way vertex is dropped when it and both neighbours
    are outside the box (no segment test)."""
    res = list(coords)
    if len(res) == 1:
        return res if _inside(res[0], clip) else [(NO_COORD, NO_COORD)]
    m = [_inside(p, clip) for p in coords]
    for i in range(len(res)):
        if not m[i] and (i == 0 or not m[i - 1]) and (i == len(res) - 1 or not m[i + 1]):
            res[i] = (NO_COORD, NO_COORD)
    return res


def _json_coords(pts):
    return "[" + ", ".join("null" if a == NO_COORD else f'{{"lat": {a / 1e7:.7f}, "lon": {b / 1e7:.7f}}}'
                           for a, b in pts) + "]"


def center_of(b):
    return (b[0] + b[2]) / 2, (b[1] + b[3]) / 2  # Bbox_Double::center_* for west <= east


class OutputTooLarge(Exception):
    """[maxsize:N] exceeded (Overpass reports this as running out of memory)."""


class Document:
    """Writes Overpass output incrementally: header, then each `out` statement's elements as they are
    produced (in chunks of ids, so memory stays bounded), then remarks and the footer.

    `sink` receives text fragments; by default they are buffered for render(). The server passes a
    sink that streams them to the client."""

    CHUNK = 20000  # ids fetched and serialised per batch

    def __init__(self, fmt="xml", csv=None, timestamp=None, json_callback=None, sink=None, maxsize=None):
        self.fmt = fmt
        self.csv = csv or CsvSettings()
        self.timestamp = timestamp or ""
        self.callback = json_callback
        self.with_areas = False  # the request uses areas (Overpass's area_level > 0)
        self.areas_timestamp = ""
        self.clip = None
        self.remarks = []
        self.global_bbox = None  # [bbox:...]; XML prints it as <bounds> (Output_XML::print_global_bbox)
        self.maxsize = maxsize   # bytes of output allowed
        self._buf = []
        self.sink = sink or self._buf.append
        self.started = False
        self.finished = False
        self.n_elements = 0
        self.size = 0

    @property
    def content_type(self):
        if self.fmt == "json":
            return "application/javascript" if self.callback else "application/json"
        return {"xml": "application/osm3s+xml", "csv": "text/csv"}[self.fmt]

    # ---- writing -----------------------------------------------------------------

    def _write(self, text):
        self.size += len(text)
        self.sink(text)

    def start(self):
        if not self.started:
            self.started = True
            self._write(getattr(self, f"_head_{self.fmt}")())

    def emit(self, it):
        self.start()
        text = getattr(self, f"_line_{self.fmt}")(it)
        if self.maxsize is not None and self.size + len(text) > self.maxsize:
            raise OutputTooLarge(self.size + len(text))
        if self.fmt == "json" and self.n_elements:
            text = ",\n" + text
        self.n_elements += 1
        self._write(text)

    def finish(self):
        if not self.finished:
            self.start()
            self.finished = True
            self._write(getattr(self, f"_tail_{self.fmt}")())

    def render(self):
        """(content_type, body) for a buffered document."""
        self.finish()
        return self.content_type, "".join(self._buf)

    # ---- collecting ------------------------------------------------------------

    def out(self, engine, set_name, opts: OutOptions):
        bits = opts.bits()
        if bits & COUNT:
            counts = dict(engine._sql(f"SELECT type, count(*) FROM {set_name} GROUP BY type").fetchall())
            n, w, r, a = (counts.get(k, 0) for k in "nwra")
            tags = [("nodes", str(n)), ("ways", str(w)), ("relations", str(r))]
            if self.with_areas:  # make_count_tags lists areas when an area database exists
                tags.append(("areas", str(a)))
            self.emit({"type": "count", "id": 0, "tags": tags + [("total", str(n + w + r + a))]})
            return
        geo = bits & (GEOMETRY | BOUNDS | CENTER)
        self.clip = opts.clip
        left = opts.limit
        for t in "nwra":  # Overpass prints nodes, then ways, relations, areas
            if left is not None and left <= 0:
                break
            ids = engine._ids_of(set_name, t)
            if not ids:
                continue
            if opts.order == "qt":
                keys = engine.sort_keys(t, ids)
                ids = sorted(ids, key=lambda i: (keys.get(i, 0), i))
            else:
                ids = sorted(ids)
            for k in range(0, len(ids), self.CHUNK):
                chunk = ids[k:k + self.CHUNK]
                meta = engine.meta(t, chunk) if bits & META and t != "a" else {}
                found = {it["id"]: it for it in getattr(self, f"_items_{t}")(engine, chunk, bits, geo)}
                for i in chunk:
                    it = found.get(i)
                    if it is None:
                        continue
                    if meta:
                        it["meta"] = meta.get(i)
                    it["bits"] = bits
                    self.emit(it)
                    if left is not None:
                        left -= 1
                        if left <= 0:
                            return

    def _items_n(self, e, ids, bits, geo):
        cols = ["id", "lat", "lon"] + (["tags"] if bits & TAGS else [])
        for row in e.elements_by_id("n", ids, cols):
            it = {"type": "node", "id": row[0]}
            if bits & (COORDS | geo):
                it["lat"], it["lon"] = row[1], row[2]
            if bits & TAGS:
                it["tags"] = tag_items(row[3])
            yield it

    def _items_w(self, e, ids, bits, geo):
        need_geo = bool(geo) or False
        cols = ["id", "nds"] + (["nd_lat", "nd_lon"] if need_geo else []) + (["tags"] if bits & TAGS else [])
        for row in e.elements_by_id("w", ids, cols):
            r = dict(zip(cols, row))
            it = {"type": "way", "id": r["id"]}
            if bits & NDS:
                it["nodes"] = r["nds"]
            if need_geo:
                coords = list(zip(r["nd_lat"], r["nd_lon"]))
                if self.clip is not None and bits & GEOMETRY:
                    coords = clip_way(coords, self.clip)
                b = bounds_of(coords)
                if b and bits & BOUNDS:
                    it["bounds"] = b
                elif b and bits & CENTER:
                    it["center"] = center_of(b)
                if bits & GEOMETRY:
                    it["geometry"] = coords
            if bits & TAGS:
                it["tags"] = tag_items(r["tags"])
            yield it

    def _items_r(self, e, ids, bits, geo):
        cols = ["id", "members"] + (["tags"] if bits & TAGS else [])
        rows = [dict(zip(cols, row)) for row in e.elements_by_id("r", ids, cols)]
        node_xy, way_xy = {}, {}
        if geo:
            mem = [m for r in rows for m in r["members"]]
            node_xy = {i: (a, b) for i, a, b in e.elements_by_id("n", [m["ref"] for m in mem if m["type"] == "n"],
                                                                 ["id", "lat", "lon"])}
            way_rows = e.elements_by_id("w", [m["ref"] for m in mem if m["type"] == "w"], ["id", "nd_lat", "nd_lon", "qt"])
            way_xy = {i: list(zip(a, b)) for i, a, b, _ in way_rows}
            if self.clip is not None and bits & GEOMETRY:
                node_xy = {i: p for i, p in node_xy.items() if _inside(p, self.clip)}
                # Relation_Geometry_Store loads only member ways indexed in calc_parents(bbox ranges);
                # the others print an empty geometry
                parents = quadtile.calc_parents(quadtile.calc_ranges(self.clip.s, self.clip.n, self.clip.w, self.clip.e))
                loaded = {i for i, _, _, qt in way_rows
                          if any(lo <= quadtile.key31(qt) < hi for lo, hi in parents)}
                way_xy = {i: clip_member_way(c, self.clip) for i, c in way_xy.items() if i in loaded}
        for r in rows:
            it = {"type": "relation", "id": r["id"]}
            if geo:
                coords = []
                for m in r["members"]:
                    if m["type"] == "n" and m["ref"] in node_xy:
                        coords.append(node_xy[m["ref"]])
                    elif m["type"] == "w" and m["ref"] in way_xy:
                        coords.extend(way_xy[m["ref"]])
                b = bounds_of(coords)
                if b and bits & BOUNDS:
                    it["bounds"] = b
                elif b and bits & CENTER:
                    it["center"] = center_of(b)
            if bits & MEMBERS:
                members = []
                for m in r["members"]:
                    mm = {"type": TYPE_NAME[m["type"]], "ref": m["ref"], "role": m["role"]}
                    if bits & GEOMETRY:
                        if m["type"] == "n" and m["ref"] in node_xy:
                            mm["lat"], mm["lon"] = node_xy[m["ref"]]
                        elif m["type"] == "w":
                            # Overpass prints a geometry list for every way member; nulls if unknown
                            mm["geometry"] = way_xy.get(m["ref"], [])
                    members.append(mm)
                it["members"] = members
            if bits & TAGS:
                it["tags"] = tag_items(r["tags"])
            yield it

    def _items_a(self, e, ids, bits, geo):
        cols = ["id"] + (["tags"] if bits & TAGS else [])
        for row in e.elements_by_id("a", ids, cols):
            it = {"type": "area", "id": row[0]}
            if bits & TAGS:
                it["tags"] = tag_items(row[1])
            yield it

    # ---- serialisation -----------------------------------------------------------

    def _head_json(self):
        head = {"version": 0.6, "generator": GENERATOR,
                "osm3s": {"timestamp_osm_base": self.timestamp}}
        if self.areas_timestamp:
            head["osm3s"]["timestamp_areas_base"] = self.areas_timestamp
        head["osm3s"]["copyright"] = COPYRIGHT
        body = json.dumps(head, ensure_ascii=False, indent=2)[:-2] + ',\n  "elements": [\n'
        return (f"{self.callback}(" if self.callback else "") + body

    def _line_json(self, it):
        """One element, built as text (json.dumps of nested dicts dominates large outputs).
        Coordinates print with 7 fixed decimals, like Overpass's JSON writer."""
        if it["type"] == "count":
            return "    " + json.dumps(self._json_elem(it), ensure_ascii=False, separators=(", ", ": "))
        bits = it["bits"]
        o = ['    {"type": "', it["type"], '"']
        if bits & ID:
            o.append(f', "id": {it["id"]}')
        if "lat" in it:
            o.append(f', "lat": {it["lat"] / 1e7:.7f}, "lon": {it["lon"] / 1e7:.7f}')
        m = it.get("meta")
        if m:
            version, ts, changeset, uid, user = m
            o.append(f', "timestamp": "{iso(ts)}", "version": {version}, "changeset": {changeset}')
            if user:
                o.append(', "user": ' + json.dumps(user, ensure_ascii=False))
            o.append(f', "uid": {uid}')
        if "bounds" in it:
            b = it["bounds"]
            o.append(f', "bounds": {{"minlat": {b[0] / 1e7:.7f}, "minlon": {b[1] / 1e7:.7f}, '
                     f'"maxlat": {b[2] / 1e7:.7f}, "maxlon": {b[3] / 1e7:.7f}}}')
        elif "center" in it:
            c = it["center"]
            o.append(f', "center": {{"lat": {c[0] / 1e7:.7f}, "lon": {c[1] / 1e7:.7f}}}')
        if it.get("nodes"):
            o.append(', "nodes": [' + ", ".join(map(str, it["nodes"])) + "]")
        if "geometry" in it:
            o.append(', "geometry": ' + _json_coords(it["geometry"]))
        if it.get("members"):
            ms = []
            for mm in it["members"]:
                t = f'{{"type": "{mm["type"]}", "ref": {mm["ref"]}, "role": ' + json.dumps(mm["role"], ensure_ascii=False)
                if "lat" in mm:
                    t += f', "lat": {mm["lat"] / 1e7:.7f}, "lon": {mm["lon"] / 1e7:.7f}'
                if "geometry" in mm:
                    t += ', "geometry": ' + _json_coords(mm["geometry"])
                ms.append(t + "}")
            o.append(', "members": [' + ", ".join(ms) + "]")
        if it.get("tags"):
            o.append(', "tags": ' + json.dumps(dict(it["tags"]), ensure_ascii=False))
        o.append("}")
        return "".join(o)

    def _tail_json(self):
        tail = "\n  ]"
        if self.remarks:
            tail += ',\n  "remark": ' + json.dumps(" ".join(self.remarks), ensure_ascii=False)
        return tail + "\n}\n" + (");\n" if self.callback else "")

    @staticmethod
    def _json_pt(p):
        return None if p[0] == NO_COORD else {"lat": f7(p[0]), "lon": f7(p[1])}

    def _json_elem(self, it):
        if it["type"] == "count":
            return {"type": "count", "id": 0, "tags": dict(it["tags"])}
        bits = it["bits"]
        o = {"type": it["type"]}
        if bits & ID:
            o["id"] = it["id"]
        if "lat" in it:
            o["lat"], o["lon"] = f7(it["lat"]), f7(it["lon"])
        m = it.get("meta")
        if m:
            version, ts, changeset, uid, user = m
            o.update(timestamp=iso(ts), version=version, changeset=changeset)
            if user:
                o["user"] = user
            o["uid"] = uid
        if "bounds" in it:
            b = it["bounds"]
            o["bounds"] = {"minlat": f7(b[0]), "minlon": f7(b[1]), "maxlat": f7(b[2]), "maxlon": f7(b[3])}
        elif "center" in it:
            o["center"] = {"lat": f7(it["center"][0]), "lon": f7(it["center"][1])}
        if it.get("nodes"):
            o["nodes"] = list(it["nodes"])
        if "geometry" in it:
            o["geometry"] = [self._json_pt(p) for p in it["geometry"]]
        if it.get("members"):
            ms = []
            for m in it["members"]:
                mm = {"type": m["type"], "ref": m["ref"], "role": m["role"]}
                if "lat" in m:
                    mm["lat"], mm["lon"] = f7(m["lat"]), f7(m["lon"])
                if "geometry" in m:
                    mm["geometry"] = [self._json_pt(p) for p in m["geometry"]]
                ms.append(mm)
            o["members"] = ms
        if it.get("tags"):
            o["tags"] = dict(it["tags"])
        return o

    def _head_xml(self):
        out = ['<?xml version="1.0" encoding="UTF-8"?>',
               f'<osm version="0.6" generator="{GENERATOR}">',
               f"<note>{COPYRIGHT}</note>",
               f'<meta osm_base="{self.timestamp}"' + (f' areas="{self.areas_timestamp}"' if self.areas_timestamp else "")
               + "/>", ""]
        if self.global_bbox is not None:
            b = self.global_bbox
            out += [f'  <bounds minlat="{b.s:.7f}" minlon="{b.w:.7f}" maxlat="{b.n:.7f}" maxlon="{b.e:.7f}"/>', ""]
        return "\n".join(out) + "\n"

    def _line_xml(self, it):
        return self._xml_elem(it) + "\n"

    def _tail_xml(self):
        return "\n" + "".join(f"<remark> {r} </remark>\n" for r in self.remarks) + "</osm>\n"

    @staticmethod
    def _xml_pt(p, tag="nd"):
        return f'lat="{f7(p[0]):.7f}" lon="{f7(p[1]):.7f}"' if p[0] != NO_COORD else ""

    def _xml_elem(self, it):
        if it["type"] == "count":
            inner = "".join(f'\n    <tag k={quoteattr(k)} v={quoteattr(v)}/>' for k, v in it["tags"])
            return f'  <count id="0">{inner}\n  </count>'
        bits, name = it["bits"], it["type"]
        attrs = f' id="{it["id"]}"' if bits & ID else ""
        if "lat" in it:
            attrs += f' lat="{f7(it["lat"]):.7f}" lon="{f7(it["lon"]):.7f}"'
        m = it.get("meta")
        if m:
            version, ts, changeset, uid, user = m
            attrs += f' version="{version}" timestamp="{iso(ts)}" changeset="{changeset}" uid="{uid}"'
            if user:
                attrs += f" user={quoteattr(user)}"
        inner = []
        if "bounds" in it:
            b = it["bounds"]
            inner.append(f'    <bounds minlat="{f7(b[0]):.7f}" minlon="{f7(b[1]):.7f}" '
                         f'maxlat="{f7(b[2]):.7f}" maxlon="{f7(b[3]):.7f}"/>')
        elif "center" in it:
            inner.append(f'    <center lat="{f7(it["center"][0]):.7f}" lon="{f7(it["center"][1]):.7f}"/>')
        for i, ref in enumerate(it.get("nodes") or []):
            g = it.get("geometry")
            pt = (" " + self._xml_pt(g[i])) if g is not None and g[i][0] != NO_COORD else ""
            inner.append(f'    <nd ref="{ref}"{pt}/>')
        for mm in it.get("members") or []:
            a = f'    <member type="{mm["type"]}" ref="{mm["ref"]}" role={quoteattr(mm["role"])}'
            if "lat" in mm:
                a += f' lat="{f7(mm["lat"]):.7f}" lon="{f7(mm["lon"]):.7f}"'
            if "geometry" in mm:
                pts = "".join(f"\n      <nd {self._xml_pt(p)}/>" for p in mm["geometry"])
                inner.append(a + f">{pts}\n    </member>")
            else:
                inner.append(a + "/>")
        for k, v in it.get("tags") or []:
            inner.append(f"    <tag k={quoteattr(k)} v={quoteattr(v)}/>")
        if not inner:
            return f"  <{name}{attrs}/>"
        return f"  <{name}{attrs}>\n" + "\n".join(inner) + f"\n  </{name}>"

    def _head_csv(self):
        if not self.csv.header:
            return ""
        # Output_CSV::write_payload_header: special fields print as "@id", "@lat", ...
        return self.csv.separator.join("@" + name[2:] if special else self._csv_escape(name)
                                       for name, special in self.csv.fields) + "\n"

    def _line_csv(self, it):
        return self.csv.separator.join(self._csv_field(it, name, special) for name, special in self.csv.fields) + "\n"

    def _tail_csv(self):
        return ""

    @staticmethod
    def _csv_escape(v):
        if "\n" in v or "," in v or '"' in v:
            return '"' + v.replace('"', '""') + '"'
        return v

    def _csv_field(self, it, name, special):
        tags = dict(it.get("tags") or [])
        if not special:
            return self._csv_escape(tags.get(name, ""))
        key = name[2:]
        if it["type"] == "count":
            return {"count": tags.get("total", "0"), "count:nodes": tags.get("nodes", "0"),
                    "count:ways": tags.get("ways", "0"), "count:relations": tags.get("relations", "0"),
                    "count:total": tags.get("total", "0"), "type": "count", "id": "0"}.get(key, "")
        m = it.get("meta")
        if key == "id":
            return str(it["id"]) if it["bits"] & ID else ""
        if key == "type":
            return it["type"]
        if key == "otype":
            return {"node": "1", "way": "2", "relation": "3", "area": "4"}[it["type"]]
        if key in ("lat", "lon"):
            if "lat" in it:
                v = it["lat"] if key == "lat" else it["lon"]
            elif "center" in it:
                v = it["center"][0] if key == "lat" else it["center"][1]
            elif "bounds" in it:
                v = center_of(it["bounds"])[0 if key == "lat" else 1]
            else:
                return ""
            return f"{f7(v):.7f}"
        if m and key in ("version", "timestamp", "changeset", "uid", "user"):
            version, ts, changeset, uid, user = m
            return {"version": str(version), "timestamp": iso(ts), "changeset": str(changeset),
                    "uid": str(uid), "user": user or ""}[key]
        return ""
