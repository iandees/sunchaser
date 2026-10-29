"""Overpass `out` statement and output formats (statements/print.cc, output_formats/output_*.cc).

A Document collects the output of every `out` statement in a request and serialises it as
Overpass XML, JSON or CSV. Element field order and presence follow the C++ writers; coordinates
are printed with 7 decimals like Overpass.
"""
import datetime as dt
import json
from dataclasses import dataclass, field
from xml.sax.saxutils import quoteattr

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
TYPE_NAME = {"n": "node", "w": "way", "r": "relation"}
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


def center_of(b):
    return (b[0] + b[2]) / 2, (b[1] + b[3]) / 2  # Bbox_Double::center_* for west <= east


class Document:
    def __init__(self, fmt="xml", csv=None, timestamp=None, json_callback=None):
        self.fmt = fmt
        self.csv = csv or CsvSettings()
        self.timestamp = timestamp or ""
        self.callback = json_callback
        self.items = []        # dicts in a format-neutral shape
        self.remarks = []

    # ---- collecting ------------------------------------------------------------

    def out(self, engine, set_name, opts: OutOptions):
        bits = opts.bits()
        if bits & COUNT:
            counts = dict(engine._sql(f"SELECT type, count(*) FROM {set_name} GROUP BY type").fetchall())
            n, w, r = counts.get("n", 0), counts.get("w", 0), counts.get("r", 0)
            self.items.append({"type": "count", "id": 0, "tags": [
                ("nodes", str(n)), ("ways", str(w)), ("relations", str(r)), ("total", str(n + w + r))]})
            return
        geo = bits & (GEOMETRY | BOUNDS | CENTER)
        out = []
        for t in "nwr":
            ids = engine._ids_of(set_name, t)
            if not ids:
                continue
            meta = engine.meta(t, ids) if bits & META else {}
            keys = engine.sort_keys(t, ids) if opts.order == "qt" else None
            for item in getattr(self, f"_items_{t}")(engine, set_name, bits, geo):
                if meta:
                    item["meta"] = meta.get(item["id"])
                if keys is not None:
                    item["_key"] = keys.get(item["id"], 0)
                out.append(item)
        order = {"node": 0, "way": 1, "relation": 2}
        if opts.order == "qt":
            out.sort(key=lambda it: (order[it["type"]], it["_key"], it["id"]))
        else:
            out.sort(key=lambda it: (order[it["type"]], it["id"]))
        if opts.limit is not None:
            out = out[:opts.limit]
        for it in out:
            it["bits"] = bits
        self.items.extend(out)

    def _items_n(self, e, s, bits, geo):
        cols = ["id", "lat", "lon"] + (["tags"] if bits & TAGS else [])
        for row in e.elements(s, "n", cols):
            it = {"type": "node", "id": row[0]}
            if bits & (COORDS | geo):
                it["lat"], it["lon"] = row[1], row[2]
            if bits & TAGS:
                it["tags"] = tag_items(row[3])
            yield it

    def _items_w(self, e, s, bits, geo):
        need_geo = bool(geo) or False
        cols = ["id", "nds"] + (["nd_lat", "nd_lon"] if need_geo else []) + (["tags"] if bits & TAGS else [])
        for row in e.elements(s, "w", cols):
            r = dict(zip(cols, row))
            it = {"type": "way", "id": r["id"]}
            if bits & NDS:
                it["nodes"] = r["nds"]
            if need_geo:
                coords = list(zip(r["nd_lat"], r["nd_lon"]))
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

    def _items_r(self, e, s, bits, geo):
        cols = ["id", "members"] + (["tags"] if bits & TAGS else [])
        rows = [dict(zip(cols, row)) for row in e.elements(s, "r", cols)]
        node_xy, way_xy = {}, {}
        if geo:
            mem = [m for r in rows for m in r["members"]]
            node_xy = {i: (a, b) for i, a, b in e.elements_by_id("n", [m["ref"] for m in mem if m["type"] == "n"],
                                                                 ["id", "lat", "lon"])}
            way_xy = {i: list(zip(a, b)) for i, a, b in e.elements_by_id(
                "w", [m["ref"] for m in mem if m["type"] == "w"], ["id", "nd_lat", "nd_lon"])}
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

    # ---- serialisation -----------------------------------------------------------

    def render(self):
        """(content_type, body: str)"""
        return getattr(self, f"_render_{self.fmt}")()

    def _render_json(self):
        elems = [self._json_elem(it) for it in self.items]
        doc = {"version": 0.6, "generator": GENERATOR,
               "osm3s": {"timestamp_osm_base": self.timestamp, "copyright": COPYRIGHT},
               "elements": elems}
        if self.remarks:
            doc["remark"] = " ".join(self.remarks)
        body = json.dumps(doc, ensure_ascii=False, indent=2)
        if self.callback:
            return "application/javascript", f"{self.callback}({body});\n"
        return "application/json", body + "\n"

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

    def _render_xml(self):
        out = ['<?xml version="1.0" encoding="UTF-8"?>',
               f'<osm version="0.6" generator="{GENERATOR}">',
               f"<note>{COPYRIGHT}</note>",
               f'<meta osm_base="{self.timestamp}"/>', ""]
        for it in self.items:
            out.append(self._xml_elem(it))
        out += [""] + [f"<remark> {r} </remark>" for r in self.remarks] + ["</osm>", ""]
        return "application/osm3s+xml", "\n".join(out)

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

    def _render_csv(self):
        sep = self.csv.separator
        lines = []
        if self.csv.header:
            # Output_CSV::write_payload_header: special fields print as "@id", "@lat", ...
            lines.append(sep.join("@" + name[2:] if special else self._csv_escape(name)
                                  for name, special in self.csv.fields))
        for it in self.items:
            lines.append(sep.join(self._csv_field(it, name, special) for name, special in self.csv.fields))
        return "text/csv", "\n".join(lines) + "\n"

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
            return {"node": "1", "way": "2", "relation": "3"}[it["type"]]
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
