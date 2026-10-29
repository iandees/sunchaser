"""Overpass statement semantics on DuckDB.

Every Overpass set is a temp table (type VARCHAR 'n'|'w'|'r', id BIGINT). Statements mirror
src/overpass_api/statements/*: query (with bbox / tag / id filters), recurse (>, >>, <, <<),
union, difference. Two backends:

  mode="lake": DuckLake tables built by build_lake.py; plans use the Overpass-style keys
               (qt / qt_key31 ranges, tag_postings, elem_locator) to prune row groups.
  mode="raw":  the unkeyed Parquet from extract.py, evaluated by brute-force scans. Used as the
               reference oracle for the lake plans.
"""
import itertools
import math
import re
from dataclasses import dataclass, field

import duckdb

import quadtile as qt

# Bbox_Double::intersects (core/geometry.cc) for west <= east, no antimeridian handling.
SQL_MACROS = r"""
CREATE OR REPLACE MACRO ov_seg(flat, flon, tlat, tlon, s, n, w, e) AS
  CASE
    WHEN flat < s AND tlat < s THEN false
    WHEN flat > n AND tlat > n THEN false
    ELSE (
      least(flon + CASE WHEN flat < s THEN (tlon - flon) * (s - flat) / (tlat - flat)
                        WHEN flat > n THEN (tlon - flon) * (n - flat) / (tlat - flat) ELSE 0 END,
            tlon + CASE WHEN tlat < s THEN (flon - tlon) * (s - tlat) / (flat - tlat)
                        WHEN tlat > n THEN (flon - tlon) * (n - tlat) / (flat - tlat) ELSE 0 END) <= e
      AND
      greatest(flon + CASE WHEN flat < s THEN (tlon - flon) * (s - flat) / (tlat - flat)
                           WHEN flat > n THEN (tlon - flon) * (n - flat) / (tlat - flat) ELSE 0 END,
               tlon + CASE WHEN tlat < s THEN (flon - tlon) * (s - tlat) / (flat - tlat)
                           WHEN tlat > n THEN (flon - tlon) * (n - tlat) / (flat - tlat) ELSE 0 END) >= w)
  END;
-- Coordinates as Overpass's ::lat() computes them (ilat / 1e7 - 91.0): on bbox edges the rounding
-- differs from lat_e7 / 1e7 and decides membership.
CREATE OR REPLACE MACRO ov_lat(lat_e7) AS ((lat_e7::BIGINT + 910000000) / 1e7 - 91.0);
-- Bbox_Filter::matches: single-node ways must be contained, otherwise any segment intersects.
CREATE OR REPLACE MACRO ov_way_in_bbox(lats, lons, s, n, w, e) AS
  CASE
    WHEN len(lats) = 0 THEN false
    WHEN len(lats) = 1 THEN ov_lat(lats[1]) BETWEEN s AND n AND lons[1] / 1e7 BETWEEN w AND e
    ELSE list_bool_or(list_transform(range(2, len(lats) + 1), lambda i:
           ov_seg(ov_lat(lats[i - 1]), lons[i - 1] / 1e7, ov_lat(lats[i]), lons[i] / 1e7, s, n, w, e)))
  END;
"""


@dataclass
class Bbox:
    s: float
    w: float
    n: float
    e: float

    def ints(self):
        """Integer bounds the way filter_by_bbox compares node coordinates (ilat_/ilon_ rounding)."""
        to7 = lambda v: int(v * 10000000 + (0.5 if v > 0 else -0.5))
        return (int(round((self.s + 91.0) * 10000000 + 0.5)) - 910000000, to7(self.w),
                int(round((self.n + 91.0) * 10000000 + 0.5)) - 910000000, to7(self.e))


@dataclass
class Filter:
    """One Overpass tag filter. op in: '=', '!=', 'exists', '!exists', '~', '!~', 'k~v'."""
    op: str
    key: str
    value: str | None = None
    icase: bool = False

    def sql(self, col="tags"):
        k = sq(self.key)
        val = f"{col}[{k}]"
        flags = "'i'" if self.icase else "''"
        match self.op:
            case "=":
                return f"coalesce({val} = {sq(self.value)}, false)"
            case "!=":
                return f"coalesce({val} <> {sq(self.value)}, true)"
            case "exists":
                return f"{val} IS NOT NULL"
            case "!exists":
                return f"{val} IS NULL"
            case "~":
                return f"coalesce(regexp_matches({val}, {sq(self.value)}, {flags}), false)"
            case "!~":
                return f"NOT coalesce(regexp_matches({val}, {sq(self.value)}, {flags}), false)"
            case "k~v":
                return (f"len(map_entries({col}).list_filter(lambda x: regexp_matches(x.key, {k}, {flags})"
                        f" AND regexp_matches(x.value, {sq(self.value)}, {flags}))) > 0")
        raise ValueError(self.op)

    def postings_sql(self):
        """Predicate on tag_postings if this filter can drive an index lookup, else None.
        kv = 'key=value'; all values of a key form the contiguous range ['key=', 'key>')."""
        lo, hi = sq(self.key + "="), sq(self.key + ">")
        if self.op == "=":
            return f"kv = {sq(self.key + '=' + self.value)}"
        if self.op == "exists":
            return f"kv >= {lo} AND kv < {hi}"
        if self.op == "~" and not self.icase:
            return f"kv >= {lo} AND kv < {hi} AND regexp_matches(v, {sq(self.value)})"
        return None


def sq(s):
    return "'" + s.replace("'", "''") + "'"


TABLE = {"n": "nodes", "w": "ways", "r": "relations", "a": "areas"}
AREA_REL_OFFSET = 3600000000
AREA_WAY_OFFSET = 2400000000


@dataclass
class Plan:
    """Knobs for the lake backend (ignored in raw mode)."""
    qt_ranges: bool = True        # add Overpass calc_ranges / calc_parents predicates
    use_postings: bool = True     # use tag_postings for tag-only queries
    locator: bool = True          # route id fetches through elem_locator -> qt ranges
    max_or_ranges: int = 32       # zone-map ranges per predicate; more ranges prune better but cost CPU per row
    postings_max: int = 20000     # bbox+tag: drive from tag_postings if at most this many candidates
    rel_member_probe_max: int = 5000  # bbox relations: test members directly up to this many
    lance_scope: str = "all"      # with a Lance store: "all" node/way scans, or only id-driven ones ("ids")
    log: list = field(default_factory=list)


class Engine:
    _ids = itertools.count()

    def __init__(self, con, mode="lake", raw_dir="data/raw", plan=None, lance=None, init_macros=True):
        """lance: optional callable name -> lance.LanceDataset; routes node/way scans to Lance.
        init_macros=False when the macros were created once on the shared database (server)."""
        self.con = con
        self.lance = lance
        self.mode = mode
        self.plan = plan or Plan()
        self._locator_memo = {}
        if init_macros:
            con.sql(SQL_MACROS)
        try:
            con.sql("LOAD spatial")
        except duckdb.Error:
            pass
        if mode == "raw":
            con.sql(f"""
              CREATE OR REPLACE TEMP VIEW nodes AS SELECT * FROM '{raw_dir}/nodes.parquet';
              CREATE OR REPLACE TEMP VIEW ways AS
                SELECT *, list_min(nd_lat) min_lat, list_max(nd_lat) max_lat,
                          list_min(nd_lon) min_lon, list_max(nd_lon) max_lon
                FROM '{raw_dir}/ways.parquet';
              CREATE OR REPLACE TEMP VIEW relations AS SELECT * FROM '{raw_dir}/relations.parquet';""")
            self.t = {k: v for k, v in TABLE.items()}
        else:
            self.t = {k: f"lake.{v}" for k, v in TABLE.items()}

    # ---- attic ([date:...]) ----------------------------------------------------

    date = None

    @property
    def indexed(self):
        """Present-only indexes (tag postings, locator, reverse member indexes) are usable."""
        return self.mode == "lake" and self.date is None

    def _date_lit(self):
        return f"TIMESTAMPTZ '{self.date}'"

    def set_date(self, ts):
        """Answer as of ts: current rows valid then plus history rows covering it (Overpass's attic
        semantics: an element without history exists from its version timestamp on)."""
        if self.mode != "lake":
            raise ValueError("[date:] needs a lake")
        self.date = ts
        d = self._date_lit()
        cols = {"n": "qt, id, lat, lon, tags",
                "w": "qt_key31, qt_level, qt, id, min_lat, max_lat, min_lon, max_lon, tags, nds, nd_lat, nd_lon",
                "r": "qt_key31, qt_level, qt, id, min_lat, max_lat, min_lon, max_lon, tags, members"}
        for t, name in (("n", "nodes"), ("w", "ways"), ("r", "relations")):
            self.t[t] = (f"(SELECT {cols[t]} FROM lake.{name} WHERE valid_from <= {d} UNION ALL "
                         f"SELECT {cols[t]} FROM lake.{name}_history WHERE valid_from <= {d} AND valid_to > {d})")
        self.plan.use_postings = False
        self.plan.locator = False
        self.lance = None

    def newer_ids(self, t, ts):
        """(newer:"T"): elements whose version timestamp is after T."""
        name = {"n": "nodes", "w": "ways", "r": "relations"}[t]
        return self._materialize_ids(f"SELECT id FROM lake.{name}_meta WHERE ts > TIMESTAMPTZ '{ts}'")

    def changed_ids(self, t, t1, t2=None):
        """(changed:"T1"[,"T2"]): elements with a new version, a deletion or a geometry change (a member
        node moved; tags-only node edits don't cascade) in (T1, T2]."""
        upper = f" AND ts <= TIMESTAMPTZ '{t2}'" if t2 else ""
        return self._materialize_ids(
            f"SELECT DISTINCT id FROM lake.changes WHERE type = '{t}' AND ts > TIMESTAMPTZ '{t1}'{upper}")

    def history_start(self):
        """Earliest time attic queries can answer for (the lake's build snapshot)."""
        rows = self._sql("SELECT min(ts) FROM lake.changes").fetchall()
        return rows[0][0]

    # ---- set plumbing ------------------------------------------------------

    trace = None  # optional callable(label, fn) used by the profiler

    def _sql(self, sql):
        if self.trace is None:
            return self.con.sql(sql)
        return self.trace(sql, lambda: self.con.sql(sql))

    def _new(self, sql):
        name = f"s{next(self._ids)}"
        self._sql(f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT DISTINCT type, id FROM ({sql})")
        return name

    def ids(self, s):
        return set(self._sql(f"SELECT type, id FROM {s}").fetchall())

    def count(self, s):
        return self._sql(f"SELECT count(*) FROM {s}").fetchall()[0][0]

    def union(self, *sets):
        return self._new(" UNION ALL ".join(f"SELECT * FROM {s}" for s in sets))

    def difference(self, a, b):
        return self._new(f"SELECT * FROM {a} EXCEPT SELECT * FROM {b}")

    # ---- range / id predicates ----------------------------------------------

    def _ranges_sql(self, col, ranges):
        """OR of BETWEENs for zone-map pruning, coalesced to at most plan.max_or_ranges by merging the
        smallest gaps (DuckDB evaluates OR chains per row, so long chains are CPU-bound)."""
        if not ranges:
            return "false"
        ranges = coalesce_ranges(ranges, self.plan.max_or_ranges)
        return "(" + " OR ".join(f"{col} BETWEEN {lo} AND {hi - 1}" for lo, hi in ranges) + ")"

    @staticmethod
    def _value_ranges(values):
        """Collapse ints into [lo, hi) runs."""
        out = []
        for v in sorted(set(values)):
            if out and v == out[-1][1]:
                out[-1][1] = v + 1
            else:
                out.append([v, v + 1])
        return [tuple(r) for r in out]

    def _ids_table(self, ids):
        """Temp table of ids, loaded through Arrow: pasting tens of thousands of literals into SQL
        costs about a second of parsing per statement."""
        import pyarrow as pa
        name = f"ids{next(self._ids)}"
        self.con.register(f"{name}_arrow", pa.table({"id": pa.array(list(ids), type=pa.int64())}))
        self._sql(f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT id FROM {name}_arrow")
        self.con.unregister(f"{name}_arrow")
        return name

    def _id_pred(self, col, ids):
        """Exact membership via a hash semi-join on a temp table, plus coalesced ranges so the
        Parquet scan can skip row groups (a subquery alone only pushes down min/max)."""
        if not ids:
            return "false"
        name = self._ids_table(ids)
        pred = f"{col} IN (SELECT id FROM {name})"
        if self.mode == "lake":
            pred = f"{self._ranges_sql(col, self._value_ranges(ids))} AND {pred}"
        return pred

    def id_set(self, t, ids):
        """A set table holding the given ids of type t."""
        return self._new(f"SELECT '{t}' AS type, id FROM {self._ids_table(ids)}")

    def _materialize_ids(self, sql):
        """Run an id-producing query once; returns a sorted list of ids."""
        return sorted({i for (i,) in self._sql(sql).fetchall() if i is not None})

    def _ids_of(self, s, t):
        return self._materialize_ids(f"SELECT id FROM {s} WHERE type = '{t}'")

    # ---- element scans ---------------------------------------------------------

    def _scan(self, t, push, local=(), cols=("id",), select=None, tbl=None):
        """SQL selecting from elements of type t. `push` predicates are range/id/bbox comparisons a
        storage layer can prune with; `local` ones (tags, exact geometry, semi-joins) run in DuckDB.
        With plan.lance, node/way scans go to Lance (BTree indexes on id and qt), materialised as
        Arrow and registered in DuckDB; otherwise everything runs against DuckLake."""
        select = select or f"'{t}' AS type, id"
        tbl = tbl or self.t[t]
        if self._lance_route(t, push):
            name = self._lance_fetch(tbl, push, cols)
            return f"SELECT {select} FROM {name}" + (" WHERE " + " AND ".join(local) if local else "")
        preds = list(push) + list(local)
        return f"SELECT {select} FROM {tbl} WHERE " + (" AND ".join(preds) if preds else "true")

    LANCE_ID = "id IN ("  # prefix of the literal id-list predicates produced for Lance

    def _lance_route(self, t, push=None):
        """Whether a node/way scan goes to Lance. With scope "ids", only scans driven by an id list
        (recursion, id queries, tag-postings fetches) do; spatial scans stay on DuckLake zone maps."""
        if self.lance is None or t not in "nw":
            return False
        if self.plan.lance_scope == "all" or push is None:
            return True
        return any(p.startswith(self.LANCE_ID) for p in push)

    def _lance_fetch(self, tbl, push, cols):
        tables = {"lake.nodes": ("nodes_tagged", "nodes_untagged"), "lake.nodes_tagged": ("nodes_tagged",),
                  "lake.ways": ("ways",)}[tbl]
        flt = " AND ".join(f"({p})" for p in push) if push else None
        parts = [self.lance(name).to_table(columns=list(cols), filter=flt) for name in tables]
        name = f"lt{next(self._ids)}"
        arrow = parts[0] if len(parts) == 1 else __import__("pyarrow").concat_tables(parts)
        self.con.register(f"{name}_arrow", arrow)
        self._sql(f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT * FROM {name}_arrow")
        self.con.unregister(f"{name}_arrow")
        self.plan.log.append(("lance", tables, arrow.num_rows))
        return name

    def _id_parts(self, t, ids):
        """(push, local) predicates selecting ids of type t."""
        if not ids:
            return ["false"], []
        if t == "a":
            return [], [self._id_pred("id", ids)]  # areas: small table, no locator entry
        if self._lance_route(t):
            return [f"id IN ({','.join(map(str, ids))})"], []  # Lance BTree lookup
        push, local = [], [self._id_pred("id", ids)]
        if self.mode == "lake" and self.plan.locator:
            push.append(self._locator_pred(t, ids))
        return push, local

    # ---- query statement -----------------------------------------------------

    def query(self, types, bbox=None, filters=(), ids=None, within=None):
        """`node|way|rel|nwr [filters] (bbox) (id:...) (.within)` -> set."""
        parts = [self._query_one(t, bbox, list(filters), ids, within) for t in types]
        return self._new(" UNION ALL ".join(parts))

    # Filters that can only match elements carrying at least one tag.
    NEEDS_TAG = {"=", "exists", "~", "k~v"}

    def _query_one(self, t, bbox, filters, ids, within):
        if t == "a":
            return self._query_areas(bbox, filters, ids, within)
        tbl = self.t[t]
        if t == "n" and self.indexed and any(f.op in self.NEEDS_TAG for f in filters):
            tbl = "lake.nodes_tagged"
        push, local = [], [f.sql() for f in filters]
        cols = ["id", "tags"]
        for w in ([within] if isinstance(within, str) else (within or [])):
            local.append(f"id IN (SELECT id FROM {w} WHERE type = '{t}')")
        if ids is not None:
            p, l = self._id_parts(t, sorted(set(ids))) if bbox is None else ([], [self._id_pred("id", sorted(set(ids)))])
            push += p
            local += l
        lake_postings = self.mode == "lake" and self.plan.use_postings and ids is None and not within
        drive = next((f for f in filters if f.postings_sql()), None) if lake_postings else None
        if bbox is not None:
            if t == "r":
                push += self._rel_overlap(bbox)
            else:
                p, l = self._bbox_parts(t, bbox)
                push += p
                local += l
                if t == "w":
                    cols += ["nd_lat", "nd_lon"]
            if drive is not None:
                p, l = self._postings_parts(t, drive, bbox)
                push += p
                local += l
        elif drive is not None:
            p, l = self._postings_parts(t, drive)
            push += p
            local += l
        if not push and not local and bbox is None:
            raise ValueError("Overpass rejects unconstrained queries")
        if t == "r" and bbox is not None:
            # candidates first (bbox overlap, keys, tags), then the member test on those only
            cand = self._new(self._scan(t, push, local, cols=cols, tbl=tbl))
            return f"SELECT 'r' AS type, id FROM {self._ids_table(self._rels_with_member_in(cand, bbox))}"
        return self._scan(t, push, local, cols=cols, tbl=tbl)

    # ---- areas (statements/area_query.cc, map_to_area.cc, coord_query.cc, pivot.cc) ----

    def _query_areas(self, bbox, filters, ids, within):
        if self.date is not None:
            raise ValueError("areas are not available for [date:] queries")
        where = [f.sql() for f in filters]
        for w in ([within] if isinstance(within, str) else (within or [])):
            where.append(f"id IN (SELECT id FROM {w} WHERE type = 'a')")
        if ids is not None:
            where.append(f"id IN ({','.join(map(str, ids)) or 'NULL'})")
        # a bbox does not constrain areas: Bbox_Constraint has no area path ("TODO: filter areas"),
        # so area(s,w,n,e) alone returns every area
        if not where and bbox is not None:
            return "SELECT 'a' AS type, id FROM lake.areas"
        if not where:
            raise ValueError("Filters too weak in query statement: specify in addition a bbox, a tag filter, or similar.")
        return f"SELECT 'a' AS type, id FROM lake.areas WHERE " + " AND ".join(where)

    def _area_rows(self, area_ids):
        if not area_ids:
            return []
        return self._sql(f"""SELECT id, min_lat, max_lat, min_lon, max_lon, geom_wkb FROM lake.areas
                             WHERE id IN ({','.join(map(str, area_ids))})""").fetchall()

    def in_area_ids(self, t, area_ids, filters=(), bbox=None):
        """(area) / (area.x): nodes inside or on the border; ways with a vertex strictly inside or a
        segment crossing the interior (a way only touching the border does not count); relations
        with such a node or way member."""
        found = set()
        for aid, s7, n7, w7, e7, wkb in self._area_rows(area_ids):
            box = Bbox(ov_lat_f(s7), w7 / 1e7, ov_lat_f(n7), e7 / 1e7)
            if bbox is not None:
                box = Bbox(max(box.s, bbox.s), max(box.w, bbox.w), min(box.n, bbox.n), min(box.e, bbox.e))
                if box.s > box.n or box.w > box.e:
                    continue
            name = f"area{next(self._ids)}"
            self.con.execute(f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT ST_GeomFromWKB(?::BLOB) AS g", [wkb])
            cand = self._new(self._query_one(t, box, list(filters), None, None))
            found |= self._in_area(t, cand, name)
        return sorted(found)

    def in_poly_ids(self, t, pts, filters=(), bbox=None):
        """(poly:"lat lon ..."): like (area) but direct nodes and ways on the border count
        (polygon_query.cc collect_* with add_border=true); relation members must be inside."""
        ring = list(pts) + ([pts[0]] if pts[0] != pts[-1] else [])
        wkt = "POLYGON((" + ", ".join(f"{lo!r} {la!r}" for la, lo in ring) + "))"
        name = f"poly{next(self._ids)}"
        self._sql(f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT ST_GeomFromText('{wkt}') AS g")
        box = Bbox(min(p[0] for p in pts), min(p[1] for p in pts), max(p[0] for p in pts), max(p[1] for p in pts))
        if bbox is not None:
            box = Bbox(max(box.s, bbox.s), max(box.w, bbox.w), min(box.n, bbox.n), min(box.e, bbox.e))
            if box.s > box.n or box.w > box.e:
                return []
        cand = self._new(self._query_one(t, box, list(filters), None, None))
        return sorted(self._in_area(t, cand, name, add_border=True))

    def _in_area(self, t, cand, area_tbl, add_border=None):
        """Elements of set cand inside the geometry in area_tbl. add_border: whether touching the
        border counts (None = Overpass's area defaults: yes for nodes, no for ways)."""
        point = "ST_Point(lon / 1e7, ov_lat(lat))"
        line = ("CASE WHEN len(nd_lat) = 1 THEN ST_Point(nd_lon[1] / 1e7, ov_lat(nd_lat[1])) ELSE "
                "ST_MakeLine(list_transform(list_filter(range(1, len(nd_lat) + 1), lambda i: nd_lat[i] <> -2147483648), "
                "lambda i: ST_Point(nd_lon[i] / 1e7, ov_lat(nd_lat[i])))) END")
        if t == "n":
            rows = self.elements(cand, "n", ["id", "lat", "lon"])
            test = "ST_Intersects(a.g, p)" if add_border in (None, True) else "ST_Contains(a.g, p)"
            sel = f"SELECT id FROM (SELECT id, {point} AS p FROM x), {area_tbl} a WHERE {test}"
        elif t == "w":
            rows = self.elements(cand, "w", ["id", "nd_lat", "nd_lon"])
            test = "ST_Intersects(a.g, l)" + ("" if add_border else " AND NOT ST_Touches(a.g, l)")
            sel = (f"SELECT id FROM (SELECT id, {line} AS l FROM x WHERE len(list_filter(nd_lat, lambda v: v <> -2147483648)) > 0), "
                   f"{area_tbl} a WHERE {test}")
        else:
            members = self.elements(cand, "r", ["id", "members"])
            node_ids = sorted({m["ref"] for _, ms in members for m in ms if m["type"] == "n"})
            way_ids = sorted({m["ref"] for _, ms in members for m in ms if m["type"] == "w"})
            # relation members never count on the border (collect_* with add_border=false)
            inside_n = set(self._in_area("n", self._new(self._fetch("n", node_ids)), area_tbl, False)) if node_ids else set()
            inside_w = set(self._in_area("w", self._new(self._fetch("w", way_ids)), area_tbl, False)) if way_ids else set()
            return {i for i, ms in members
                    if any((m["type"] == "n" and m["ref"] in inside_n) or (m["type"] == "w" and m["ref"] in inside_w)
                           for m in ms)}
        if not rows:
            return set()
        import pyarrow as pa
        cols = ["id", "lat", "lon"] if t == "n" else ["id", "nd_lat", "nd_lon"]
        self.con.register("x", pa.table({c: [r[i] for r in rows] for i, c in enumerate(cols)}))
        try:
            return {i for (i,) in self.con.sql(sel).fetchall()}
        finally:
            self.con.unregister("x")

    def areas_base(self):
        """Timestamp of the data the areas were built from (lags osm_base, as in Overpass)."""
        try:
            rows = self._sql("SELECT value FROM lake.state WHERE key = 'areas_base'").fetchall()
            return rows[0][0] if rows else ""
        except duckdb.Error:
            return ""

    def has_areas(self):
        if not hasattr(self, "_has_areas"):
            try:
                self._sql("SELECT 1 FROM lake.areas LIMIT 0").fetchall()
                self._has_areas = self.mode == "lake"
            except duckdb.Error:
                self._has_areas = False
        return self._has_areas

    def map_to_area_ids(self, s):
        """map_to_area: relations -> area id + 3600000000 (ways -> + 2400000000) where the area exists."""
        cand = [i + AREA_REL_OFFSET for i in self._ids_of(s, "r")] + [i + AREA_WAY_OFFSET for i in self._ids_of(s, "w")]
        if not cand:
            return []
        return self._materialize_ids(f"SELECT id FROM lake.areas WHERE id IN ({','.join(map(str, cand))})")

    def is_in_ids(self, pts):
        """is_in: areas containing any of the (lat, lon) points (border included)."""
        if not pts:
            return []
        conds = " OR ".join(f"(min_lat <= {round(la * 1e7)} AND max_lat >= {round(la * 1e7)} AND "
                            f"min_lon <= {round(lo * 1e7)} AND max_lon >= {round(lo * 1e7)} AND "
                            f"ST_Intersects(ST_GeomFromWKB(geom_wkb), ST_Point({lo}, {la})))" for la, lo in pts)
        return self._materialize_ids(f"SELECT id FROM lake.areas WHERE {conds}")

    def pivot_ids(self, s, t):
        """(pivot): the relation (or way) an area was made from."""
        ids = self._ids_of(s, "a")
        if t == "r":
            return [i - AREA_REL_OFFSET for i in ids if i >= AREA_REL_OFFSET]
        if t == "w":
            return [i - AREA_WAY_OFFSET for i in ids if AREA_WAY_OFFSET <= i < AREA_REL_OFFSET]
        return []

    def _bbox_parts(self, t, b):
        s7, w7, n7, e7 = b.ints()
        if t == "n":
            push = [f"lat BETWEEN {s7} AND {n7} AND lon BETWEEN {w7} AND {e7}"]
            if self.mode == "lake" and self.plan.qt_ranges:
                push.append(self._ranges_sql("qt", qt.calc_ranges(b.s, b.n, b.w, b.e)))
            return push, []
        push = [f"max_lat >= {s7} AND min_lat <= {n7} AND max_lon >= {w7} AND min_lon <= {e7}"]
        if self.mode == "lake" and self.plan.qt_ranges:
            push.append(self._ranges_sql("qt_key31", qt.calc_parents(qt.calc_ranges(b.s, b.n, b.w, b.e))))
        return push, [f"ov_way_in_bbox(nd_lat, nd_lon, {b.s}, {b.n}, {b.w}, {b.e})"]

    def _rel_overlap(self, b):
        s7, w7, n7, e7 = b.ints()
        if self.mode == "raw":
            return []  # brute force: no precomputed relation bbox in the raw data
        push = [f"max_lat >= {s7} AND min_lat <= {n7} AND max_lon >= {w7} AND min_lon <= {e7}"]
        if self.plan.qt_ranges:
            push.append(self._ranges_sql("qt_key31", qt.calc_parents(qt.calc_ranges(b.s, b.n, b.w, b.e))))
        return push

    def _rels_with_member_in(self, cand, b):
        """filter_relations_expensive: relations of set cand with a member node inside the bbox or a
        member way intersecting it."""
        rows = self.elements(cand, "r", ["id", "members"])
        if not rows:
            return []
        node_ids = sorted({m["ref"] for _, ms in rows for m in ms if m["type"] == "n"})
        way_ids = sorted({m["ref"] for _, ms in rows for m in ms if m["type"] == "w"})
        s7, w7, n7, e7 = b.ints()
        hit_n, hit_w = set(), set()
        if len(node_ids) + len(way_ids) > self.plan.rel_member_probe_max:
            # many candidate members (big relations around a small box): elements in the box are fewer
            hit_n = set(node_ids) & set(self._materialize_ids(self._query_one("n", b, [], None, None).replace(
                "SELECT 'n' AS type, id", "SELECT id", 1)))
            hit_w = set(way_ids) & set(self._materialize_ids(self._query_one("w", b, [], None, None).replace(
                "SELECT 'w' AS type, id", "SELECT id", 1)))
            node_ids = way_ids = []
        if node_ids:
            push, local = self._id_parts("n", node_ids)
            hit_n = set(self._materialize_ids(self._scan(
                "n", push + [f"lat BETWEEN {s7} AND {n7} AND lon BETWEEN {w7} AND {e7}"], local, select="id")))
        if way_ids:
            push, local = self._id_parts("w", way_ids)
            if self.mode == "lake":
                push.append(f"max_lat >= {s7} AND min_lat <= {n7} AND max_lon >= {w7} AND min_lon <= {e7}")
            hit_w = set(self._materialize_ids(self._scan(
                "w", push, local + [f"ov_way_in_bbox(nd_lat, nd_lon, {b.s}, {b.n}, {b.w}, {b.e})"],
                cols=("id", "nd_lat", "nd_lon", "min_lat", "max_lat", "min_lon", "max_lon"), select="id")))
        return sorted(i for i, ms in rows
                      if any((m["type"] == "n" and m["ref"] in hit_n) or (m["type"] == "w" and m["ref"] in hit_w)
                             for m in ms))

    def _postings_parts(self, t, f, bbox=None):
        """tags_global equivalent: the (qt, id) pairs come from the postings, so the element fetch can
        prune by qt exactly like Overpass's (key, value, idx) index does. With a bbox, the postings
        are restricted to the bbox's key ranges (tags_local-like) and used only if selective enough."""
        where = f"type = '{t}' AND {f.postings_sql()}"
        limit = ""
        if bbox is not None:
            node_ranges = qt.calc_ranges(bbox.s, bbox.n, bbox.w, bbox.e)
            key_ranges = ([(lo << 1, hi << 1) for lo, hi in node_ranges] if t == "n"
                          else qt.calc_parents(node_ranges))
            where += " AND " + self._ranges_sql("qt_key31", key_ranges)
            limit = f" LIMIT {self.plan.postings_max + 1}"
        rows = self._sql(f"SELECT qt_key31, id FROM lake.tag_postings WHERE {where}{limit}").fetchall()
        self.plan.log.append(("postings", t, len(rows)))
        if bbox is not None and len(rows) > self.plan.postings_max:
            return [], []  # not selective: the spatial scan alone is cheaper
        if not rows:
            return ["false"], []
        ids = sorted({i for _, i in rows})
        if self._lance_route(t) and (bbox is None or self.plan.lance_scope == "all"):
            # global tag lookups scatter across the planet: id BTree; inside a bbox the candidates are
            # spatially clustered, so DuckLake's key ranges + zone maps do better
            return [f"id IN ({','.join(map(str, ids))})"], []
        keycol = "qt" if t == "n" else "qt_key31"
        keys = [k >> 1 for k, _ in rows] if t == "n" else [k for k, _ in rows]
        name = self._ids_table(ids)
        return [self._ranges_sql(keycol, self._value_ranges(keys))], [f"id IN (SELECT id FROM {name})"]

    def _locator_pred(self, t, ids, col=None):
        """.map-file equivalent: id -> spatial key, then a key-range predicate on the clustered table.
        Resolved keys are memoised per engine (Overpass keeps its .map files hot in the page cache).
        col overrides the key column name (the *_meta tables call it k)."""
        memo = self._locator_memo.setdefault(t, {})
        todo = [i for i in ids if i not in memo]
        if todo:
            rows = self._sql(f"SELECT id, qt FROM lake.elem_locator WHERE type = '{t}' AND {self._id_pred('id', todo)}").fetchall()
            memo.update(rows)
            memo.update((i, None) for i in todo if i not in memo)  # not in the extract
            self.plan.log.append(("locator", t, len(rows)))
        keys = [memo[i] for i in ids if memo[i] is not None]
        if t != "n":
            keys = [qt.key31(q) for q in keys]
        return self._ranges_sql(col or ("qt" if t == "n" else "qt_key31"), self._value_ranges(keys))

    # ---- payload for output --------------------------------------------------

    def elements(self, s, t, cols):
        """Rows (tuples in `cols` order) of the type-t elements of set s."""
        ids = self._ids_of(s, t)
        if not ids:
            return []
        push, local = self._id_parts(t, ids)
        return self._sql(self._scan(t, push, local, cols=tuple(cols), select=", ".join(cols))).fetchall()

    def elements_by_id(self, t, ids, cols, select=None):
        """Rows of the type-t elements with these ids: `cols`, or the `select` expressions over them."""
        if not ids:
            return []
        push, local = self._id_parts(t, sorted(set(ids)))
        return self._sql(self._scan(t, push, local, cols=tuple(cols), select=select or ", ".join(cols))).fetchall()

    META_TABLE = {"n": "nodes_meta", "w": "ways_meta", "r": "relations_meta"}

    def meta(self, t, ids):
        """id -> (version, ts, changeset, uid, user)."""
        if not ids:
            return {}
        ids = sorted(set(ids))
        if self.date is not None:
            return self._meta_attic(t, ids)
        tbl = self.META_TABLE[t] if self.mode == "lake" else self.t[t]
        where = self._id_pred("id", ids)
        if self.mode == "lake":
            where += " AND " + self._locator_pred(t, ids, col="k")
            tbl = f"lake.{tbl}"
        rows = self._sql(f'SELECT id, version, ts, changeset, uid, "user" FROM {tbl} WHERE {where}').fetchall()
        return {r[0]: r[1:] for r in rows}

    def _meta_attic(self, t, ids):
        """Metadata of the versions valid at self.date: history rows first, else the current version."""
        name = {"n": "nodes", "w": "ways", "r": "relations"}[t]
        out = {r[0]: r[1:] for r in self._sql(
            f'SELECT id, version, ts, changeset, uid, "user" FROM lake.{name}_history '
            f"WHERE valid_from <= {self._date_lit()} AND valid_to > {self._date_lit()} AND {self._id_pred('id', ids)}").fetchall()}
        rest = [i for i in ids if i not in out]
        if rest:
            out.update((r[0], r[1:]) for r in self._sql(
                f'SELECT id, version, ts, changeset, uid, "user" FROM lake.{name}_meta WHERE {self._id_pred("id", rest)}').fetchall())
        return out

    def sort_keys(self, t, ids):
        """id -> Overpass index (for `out qt`)."""
        if self.indexed:
            self._locator_pred(t, sorted(set(ids)))
            return {i: self._locator_memo[t].get(i) or 0 for i in ids}
        return {i: 0 for i in ids}

    # ---- recurse statement ---------------------------------------------------

    def _fetch_where(self, t, ids):
        """DuckDB-only id predicate (used for relations and in raw mode)."""
        push, local = self._id_parts(t, ids)
        return " AND ".join(push + local)

    def _fetch(self, t, ids):
        """Elements of type t whose id is in ids, via the locator (DuckLake) or the id BTree (Lance)."""
        push, local = self._id_parts(t, ids)
        return self._scan(t, push, local)

    def recurse(self, s, op):
        """Recurse_Statement::execute (statements/recurse.cc)."""
        if op == ">":
            return self._down(s, s)
        if op == ">>":
            rels = self._rel_closure(f"SELECT * FROM {s} WHERE type = 'r'", down=True)
            ways_and_rels = self.union(rels, self._new(f"SELECT * FROM {s} WHERE type = 'w'"))
            return self.union(rels, self._down(ways_and_rels, rels))
        if op in ("<", "<<"):
            return self._up(s, closure=(op == "<<"))
        raise ValueError(op)

    def _members(self, rel_set, mtype, role=None):
        """Member refs of type mtype (optionally with this role) of the relations in rel_set."""
        rel_ids = self._ids_of(rel_set, "r")
        if not rel_ids:
            return []
        role_pred = f" AND role = {sq(role)}" if role is not None else ""
        return self._materialize_ids(
            f"SELECT ref FROM (SELECT unnest(members, recursive := true) FROM {self.t['r']} "
            f"WHERE {self._fetch_where('r', rel_ids)}) WHERE type = '{mtype}'{role_pred}")

    # ---- around ----------------------------------------------------------------------

    def around_ids(self, t, radius, pts=None, src=None, bbox=None, filters=()):
        """Ids of type-t elements within `radius` metres of the points (or of the nodes / way
        vertices of set `src`), like (around:...). Ways count if any segment is within range;
        relations if any member node or member-way segment is. A way in `src` contributes its
        vertices only (segment-to-segment distance is not implemented)."""
        import math
        if pts is None:
            pts = [(a / 1e7, b / 1e7) for _, a, b in self.elements(src, "n", ["id", "lat", "lon"])]
            for _, la, lo in self.elements(src, "w", ["id", "nd_lat", "nd_lon"]):
                pts += [(a / 1e7, b / 1e7) for a, b in zip(la, lo) if a != -2147483648]
        if not pts:
            return []
        dlat = radius / 111_000 * 1.01
        groups = {}  # cluster points into ~1 km cells so each prefilter bbox stays small
        for la, lo in pts:
            groups.setdefault((round(la * 50), round(lo * 30)), []).append((la, lo))
        found = set()
        for cell_pts in groups.values():
            s_ = min(p[0] for p in cell_pts) - dlat
            n_ = max(p[0] for p in cell_pts) + dlat
            dlon = dlat / max(0.01, math.cos(math.radians(max(abs(s_), abs(n_)))))
            box = Bbox(s_, min(p[1] for p in cell_pts) - dlon, n_, max(p[1] for p in cell_pts) + dlon)
            if bbox is not None:
                box = Bbox(max(box.s, bbox.s), max(box.w, bbox.w), min(box.n, bbox.n), min(box.e, bbox.e))
                if box.s > box.n or box.w > box.e:
                    continue
            cand = self._new(self._query_one(t, box, list(filters), None, None))
            found |= self._within_distance(t, cand, cell_pts, radius)
        return sorted(found)

    def _within_distance(self, t, cand, pts, radius):
        def d_pt(a, b):
            return great_circle_dist(a[0], a[1], b[0], b[1])

        def d_seg(p, a, b):
            # local equirectangular projection around p (adequate at Overpass radii)
            k = math.cos(math.radians(p[0]))
            ax, ay = (a[1] - p[1]) * k, a[0] - p[0]
            bx, by = (b[1] - p[1]) * k, b[0] - p[0]
            dx, dy = bx - ax, by - ay
            L = dx * dx + dy * dy
            u = 0.0 if L == 0 else max(0.0, min(1.0, -(ax * dx + ay * dy) / L))
            cx, cy = ax + u * dx, ay + u * dy
            return d_pt(p, (p[0] + cy, p[1] + cx / k))

        def line_near(coords):
            coords = [(a / 1e7, b / 1e7) for a, b in coords if a != -2147483648]
            if len(coords) == 1:
                return any(d_pt(p, coords[0]) <= radius for p in pts)
            return any(d_seg(p, coords[i], coords[i + 1]) <= radius
                       for i in range(len(coords) - 1) for p in pts)

        out = set()
        if t == "n":
            for i, la, lo in self.elements(cand, "n", ["id", "lat", "lon"]):
                if any(d_pt(p, (la / 1e7, lo / 1e7)) <= radius for p in pts):
                    out.add(i)
        elif t == "w":
            for i, la, lo in self.elements(cand, "w", ["id", "nd_lat", "nd_lon"]):
                if line_near(zip(la, lo)):
                    out.add(i)
        else:
            for i, members in self.elements(cand, "r", ["id", "members"]):
                nodes = {m["ref"] for m in members if m["type"] == "n"}
                ways = {m["ref"] for m in members if m["type"] == "w"}
                hit = any(any(d_pt(p, (a / 1e7, b / 1e7)) <= radius for p in pts)
                          for _, a, b in self.elements_by_id("n", nodes, ["id", "lat", "lon"]))
                if not hit:
                    hit = any(line_near(zip(a, b)) for _, a, b in self.elements_by_id("w", ways, ["id", "nd_lat", "nd_lon"]))
                if hit:
                    out.add(i)
        return out

    # ---- recurse filters for query statements: node(w), way(r), rel(bn), ... ----------

    def way_node_ids(self, s):
        """(w): nodes of the ways in s."""
        way_ids = self._ids_of(s, "w")
        if not way_ids:
            return []
        push, local = self._id_parts("w", way_ids)
        return self._materialize_ids(self._scan("w", push, local, cols=("id", "nds"), select="unnest(nds)"))

    def member_ids(self, s, mtype, role=None):
        """(r) / (r:"role"): members of type mtype of the relations in s."""
        return self._members(s, mtype, role)

    def parent_way_ids(self, s):
        """(bn) for ways: ways having a node of s."""
        node_ids = self._ids_of(s, "n")
        if not node_ids:
            return []
        if self.indexed:
            return self._materialize_ids(
                f"SELECT way_id FROM lake.node_parent_ways WHERE {self._id_pred('node_id', node_ids)}")
        return self._materialize_ids(
            f"SELECT id FROM {self.t['w']} WHERE len(list_intersect(nds, [{','.join(map(str, node_ids))}])) > 0")

    def parent_rel_ids(self, s, mtype, role=None):
        """(bn) / (bw) / (br) for relations: relations having an element of type mtype of s as member."""
        ids = self._ids_of(s, mtype)
        if not ids:
            return []
        role_pred = f" AND role = {sq(role)}" if role is not None else ""
        if self.indexed and role is None:
            return self._materialize_ids(
                f"SELECT rel_id FROM lake.member_parent_rels WHERE mtype = '{mtype}' AND {self._id_pred('mid', ids)}")
        return self._materialize_ids(
            f"SELECT id FROM (SELECT id, unnest(members, recursive := true) FROM {self.t['r']}) "
            f"WHERE type = '{mtype}' AND ref IN ({','.join(map(str, ids))}){role_pred}")

    def _down(self, s, way_src):
        """add_nw_member_objects: member ways of s's relations; nodes of (s's ways + those member ways)
        plus member nodes of s's relations. `way_src` supplies the relations whose member ways count."""
        member_ways = self._new(self._fetch("w", self._members(way_src, "w")))
        all_ways = self._materialize_ids(f"SELECT id FROM {s} WHERE type = 'w' UNION SELECT id FROM {member_ways}")
        push, local = self._id_parts("w", all_ways)
        way_nodes = self._materialize_ids(self._scan("w", push, local, cols=("id", "nds"), select="unnest(nds)"))
        node_ids = sorted(set(way_nodes) | set(self._members(s, "n")))
        return self._new(f"SELECT * FROM {member_ways} UNION ALL " + self._fetch("n", node_ids))

    def _parent_rels(self, member_set):
        """Relations having any (type, id) of the set table member_set as a member (collect_relations)."""
        if self.indexed:
            rel_ids = set()
            for mt in "nwr":
                ids = self._ids_of(member_set, mt)
                if ids:
                    rel_ids |= set(self._materialize_ids(
                        f"SELECT rel_id FROM lake.member_parent_rels WHERE mtype = '{mt}' AND {self._id_pred('mid', ids)}"))
            return f"SELECT 'r' AS type, id FROM {self._ids_table(sorted(rel_ids))}"
        return (f"SELECT DISTINCT 'r' AS type, r.id FROM (SELECT id, unnest(members, recursive := true) FROM {self.t['r']}) r "
                f"JOIN {member_set} x ON x.type = r.type AND x.id = r.ref")

    def _rel_closure(self, start_sql, down):
        cur = self._new(start_sql)
        while True:
            if down:
                step = self._new(self._fetch("r", self._members(cur, "r")))
            else:
                step = self._new(self._parent_rels(cur))
            nxt = self.union(cur, step)
            if self.count(nxt) == self.count(cur):
                return nxt
            cur = nxt

    def _up(self, s, closure):
        if self.indexed:
            node_ids = self._ids_of(s, "n")
            way_ids = self._materialize_ids(
                f"SELECT way_id FROM lake.node_parent_ways WHERE {self._id_pred('node_id', node_ids)}") if node_ids else []
            ways_t = self._new(self._fetch("w", way_ids))
        else:
            ways_t = self._new(f"SELECT 'w' AS type, id FROM {self.t['w']} "
                               f"WHERE len(list_intersect(nds, (SELECT list(id) FROM {s} WHERE type = 'n'))) > 0")
        members = self._new(f"SELECT * FROM {s} WHERE type IN ('n', 'w') UNION ALL SELECT * FROM {ways_t}")
        rels = self._new(f"SELECT * FROM {s} WHERE type = 'r' UNION ALL " + self._parent_rels(members))
        if closure:
            rels = self._rel_closure(f"SELECT * FROM {rels}", down=False)
        return self.union(ways_t, rels)


DEG_TO_ARC = math.acos(0) / 90.0
ARC_TO_METER = 10 * 1000 * 1000 / math.acos(0)  # Overpass's earth: a 10,000 km quarter meridian


def great_circle_dist(lat1, lon1, lat2, lon2):
    """core/geometry.cc great_circle_dist, exactly."""
    if lat1 == lat2 and lon1 == lon2:
        return 0.0
    sp = (math.cos((lat2 - lat1) * DEG_TO_ARC)
          + math.cos(lat1 * DEG_TO_ARC) * math.cos(lat2 * DEG_TO_ARC) * (math.cos((lon2 - lon1) * DEG_TO_ARC) - 1))
    return math.acos(min(1.0, sp)) * ARC_TO_METER


def ov_lat_f(lat_e7):
    return (lat_e7 + 910000000) / 1e7 - 91.0


def coalesce_ranges(ranges, k):
    """Merge [lo, hi) ranges (sorted, disjoint) down to at most k by closing the smallest gaps."""
    if len(ranges) <= k:
        return ranges
    gaps = sorted(range(len(ranges) - 1), key=lambda i: ranges[i + 1][0] - ranges[i][1])
    close = set(gaps[:len(ranges) - k])
    out = [list(ranges[0])]
    for i in range(1, len(ranges)):
        if (i - 1) in close:
            out[-1][1] = ranges[i][1]
        else:
            out.append(list(ranges[i]))
    return [tuple(r) for r in out]
