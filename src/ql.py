"""Overpass QL front end: parser (frontend/map_ql_parser.cc subset) and executor over engine.Engine.

Supported:
  settings     [out:json|xml|csv(...)] [timeout:N] [maxsize:N] [bbox:s,w,n,e]
  queries      node|way|rel|relation|nwr|nw|nr|wr  with .set inputs, tag filters
               [k] [!k] [k=v] [k!=v] [k~re] [k!~re] [~kre~vre] (",i" for case-insensitive),
               (s,w,n,e) (id) (id:1,2) (.set) (w) (r) (r:"role") (bn) (bw) (br) (with .set),
               (around:radius,lat,lon) (around.set:radius)
  blocks       ( ... ) union, ( a; - b; ) difference, "-> .name" on every statement
  recursion    > >> < <<  (optionally ".set >")
  item         .set;
  output       out [ids|skel|body|tags|meta|count] [geom|bb|center] [asc|qt] [N] [noids]
Unsupported statements raise QLError (HTTP 400), matching how Overpass reports static errors.
"""
import re
from dataclasses import dataclass, field

from engine import Bbox, Filter
from output import CsvSettings, Document, OutOptions


class QLError(Exception):
    def __init__(self, msg, line=None):
        super().__init__(msg)
        self.line = line


# ---- AST -----------------------------------------------------------------------

@dataclass
class Query:
    types: str
    inputs: list = field(default_factory=list)       # set names the result must be in
    filters: list = field(default_factory=list)      # engine.Filter
    bbox: Bbox | None = None
    ids: list | None = None
    recurse: list = field(default_factory=list)      # (kind, set, role) e.g. ("w", "_", None)
    around: tuple | None = None                      # (radius, points or set name)
    into: str = "_"
    line: int = 0


@dataclass
class Union:
    body: list
    into: str = "_"


@dataclass
class Difference:
    left: object
    right: object
    into: str = "_"


@dataclass
class Recurse:
    op: str
    src: str = "_"
    into: str = "_"


@dataclass
class Item:
    src: str
    into: str = "_"


@dataclass
class Out:
    src: str
    opts: OutOptions


@dataclass
class Script:
    settings: dict
    body: list


# ---- parser ------------------------------------------------------------------------

TYPE_WORDS = {"node": "n", "way": "w", "rel": "r", "relation": "r", "nwr": "nwr", "nw": "nw", "nr": "nr", "wr": "wr"}
UNSUPPORTED = {"area", "foreach", "if", "for", "complete", "retro", "compare", "make", "convert", "timeline",
               "local", "is_in", "map_to_area", "pivot", "derived", "changed", "newer"}


class Parser:
    def __init__(self, text):
        self.s = text
        self.i = 0

    # -- lexical helpers
    def line(self):
        return self.s.count("\n", 0, self.i) + 1

    def err(self, msg):
        raise QLError(f"line {self.line()}: parse error: {msg}", self.line())

    def ws(self):
        while self.i < len(self.s):
            c = self.s[self.i]
            if c.isspace():
                self.i += 1
            elif self.s.startswith("//", self.i):
                j = self.s.find("\n", self.i)
                self.i = len(self.s) if j < 0 else j + 1
            elif self.s.startswith("/*", self.i):
                j = self.s.find("*/", self.i + 2)
                if j < 0:
                    self.err("unterminated comment")
                self.i = j + 2
            else:
                break

    def peek(self, n=1):
        self.ws()
        return self.s[self.i:self.i + n]

    def eat(self, tok):
        if self.peek(len(tok)) == tok:
            self.i += len(tok)
            return True
        return False

    def expect(self, tok):
        if not self.eat(tok):
            got = self.s[self.i:self.i + 12] or "end of input"
            self.err(f"'{tok}' expected - '{got}' found.")

    def word(self):
        self.ws()
        m = re.compile(r"[A-Za-z_][A-Za-z0-9_]*").match(self.s, self.i)
        if not m:
            return None
        self.i = m.end()
        return m.group()

    def number(self):
        self.ws()
        m = re.compile(r"[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?").match(self.s, self.i)
        if not m:
            self.err("number expected")
        self.i = m.end()
        return m.group()

    def string(self):
        """Quoted string with C-style escapes, or a bare token (used for tag keys/values)."""
        self.ws()
        q = self.s[self.i:self.i + 1]
        if q in "\"'":
            j, out = self.i + 1, []
            while j < len(self.s) and self.s[j] != q:
                if self.s[j] == "\\" and j + 1 < len(self.s):
                    nxt = self.s[j + 1]
                    out.append({"n": "\n", "t": "\t", "\\": "\\", '"': '"', "'": "'"}.get(nxt, "\\" + nxt))
                    j += 2
                else:
                    out.append(self.s[j])
                    j += 1
            if j >= len(self.s):
                self.err("unterminated string")
            self.i = j + 1
            return "".join(out)
        m = re.compile(r"[^\s\[\]()=!~,;\"']+").match(self.s, self.i)
        if not m:
            self.err("string expected")
        self.i = m.end()
        return m.group()

    def setname(self):
        """'.name' -> name"""
        if not self.eat("."):
            return None
        w = self.word()
        if w is None:
            self.err("set name expected after '.'")
        return w

    def into(self):
        if self.eat("->"):
            n = self.setname()
            if n is None:
                self.err("'.' expected after '->'")
            return n
        return "_"

    # -- grammar
    def script(self):
        settings = {}
        start = self.i
        while self.peek() == "[":
            self.setting(settings)
        if self.i != start:
            self.expect(";")
        body = self.statements(end=None)
        return Script(settings, body)

    def setting(self, settings):
        self.expect("[")
        key = self.word()
        if key is None:
            self.err("setting name expected")
        self.expect(":")
        if key == "out":
            fmt = self.word()
            if fmt not in ("json", "xml", "csv"):
                raise QLError(f"line {self.line()}: static error: unsupported output format \"{fmt}\".")
            settings["out"] = fmt
            if fmt == "csv":
                settings["csv"] = self.csv_spec()
        elif key in ("timeout", "maxsize"):
            settings[key] = int(float(self.number()))
        elif key == "bbox":
            v = [float(self.number())]
            for _ in range(3):
                self.expect(",")
                v.append(float(self.number()))
            settings["bbox"] = Bbox(v[0], v[1], v[2], v[3])
        elif key in ("date", "diff", "adiff"):
            raise QLError(f"line {self.line()}: static error: [{key}:...] (attic data) is not supported yet.")
        else:
            self.err(f"unknown setting '{key}'")
        self.expect("]")

    def csv_spec(self):
        self.expect("(")
        fields = []
        while True:
            if self.eat("::"):
                fields.append(("::" + (self.word() or ""), True))
            else:
                fields.append((self.string(), False))
            if not self.eat(","):
                break
        cs = CsvSettings(fields=fields)
        if self.eat(";"):
            w = self.word()
            cs.header = w != "false"
            if self.eat(";"):
                cs.separator = self.string()
        self.expect(")")
        return cs

    def statements(self, end):
        body = []
        while True:
            self.ws()
            if self.i >= len(self.s):
                if end is not None:
                    self.err(f"'{end}' expected - end of input found.")
                return body
            if end is not None and self.peek(len(end)) == end:
                return body
            if end == ")" and self.peek() == "-":
                return body
            body.append(self.statement())

    def statement(self):
        line = self.line()
        c = self.peek()
        if c == "(":
            return self.block()
        src = "_"
        if c == ".":
            src = self.setname()
            c = self.peek()
            if c in "<>":
                return self.recurse(src)
            if self.peek(3) == "out":
                return self.out(src)
            into = self.into()
            self.expect(";")
            return Item(src, into)
        if c in "<>":
            return self.recurse(src)
        save = self.i
        w = self.word()
        if w is None:
            self.err(f"statement expected - '{self.s[self.i:self.i + 10]}' found.")
        if w == "out":
            self.i = save
            return self.out("_")
        if w in TYPE_WORDS:
            return self.query(TYPE_WORDS[w], line)
        if w in UNSUPPORTED:
            raise QLError(f"line {line}: static error: the statement '{w}' is not supported yet.", line)
        self.err(f"unknown statement '{w}'")

    def block(self):
        self.expect("(")
        body = self.statements(end=")")
        if self.eat("-"):
            right = self.statements(end=")")
            self.expect(")")
            into = self.into()
            self.expect(";")
            if len(body) != 1 or len(right) != 1:
                self.err("difference takes exactly one statement on each side")
            return Difference(body[0], right[0], into)
        self.expect(")")
        into = self.into()
        self.expect(";")
        return Union(body, into)

    def recurse(self, src):
        for op in (">>", "<<", ">", "<"):
            if self.eat(op):
                into = self.into()
                self.expect(";")
                return Recurse(op, src, into)
        self.err("recursion operator expected")

    def out(self, src):
        self.expect("out")
        o = OutOptions()
        while True:
            self.ws()
            if self.peek() == ";":
                break
            if self.peek().isdigit():
                o.limit = int(self.number())
                continue
            w = self.word()
            if w in ("ids", "skel", "body", "tags", "meta", "count"):
                o.mode = w
            elif w in ("geom", "bb", "center"):
                o.geometry = w
                if w == "geom" and self.peek() == "(":   # out geom(s,w,n,e): clipping not supported, ignored
                    self.expect("(")
                    while not self.eat(")"):
                        self.i += 1
            elif w in ("asc", "qt"):
                o.order = w
            elif w == "noids":
                o.noids = True
            else:
                self.err(f"unknown out parameter '{w}'")
        self.expect(";")
        return Out(src, o)

    def query(self, types, line):
        q = Query(types=types, line=line)
        while True:
            c = self.peek()
            if c == ".":
                q.inputs.append(self.setname())
            elif c == "[":
                q.filters.append(self.tag_filter())
            elif c == "(":
                self.paren_filter(q)
            else:
                break
        q.into = self.into()
        self.expect(";")
        return q

    def tag_filter(self):
        self.expect("[")
        neg = False
        if self.eat("~"):
            kre = self.string()
            self.expect("~")
            vre = self.string()
            icase = self._icase()
            self.expect("]")
            return Filter("k~v", kre, vre, icase)
        if self.eat("!"):
            neg = True
        key = self.string()
        if neg:
            self.expect("]")
            return Filter("!exists", key)
        if self.eat("]"):
            return Filter("exists", key)
        if self.eat("!="):
            v = self.string()
            self.expect("]")
            return Filter("!=", key, v)
        if self.eat("!~"):
            v = self.string()
            icase = self._icase()
            self.expect("]")
            return Filter("!~", key, v, icase)
        if self.eat("="):
            v = self.string()
            self.expect("]")
            return Filter("=", key, v)
        if self.eat("~"):
            v = self.string()
            icase = self._icase()
            self.expect("]")
            return Filter("~", key, v, icase)
        self.err("'=', '!=', '~' or ']' expected in tag filter")

    def _icase(self):
        if self.eat(","):
            if self.word() != "i":
                self.err("only ',i' is allowed after a regular expression")
            return True
        return False

    def paren_filter(self, q):
        self.expect("(")
        c = self.peek()
        if c == ".":
            q.inputs.append(self.setname())
        elif c.isdigit() or c in "-+.":
            nums = [self.number()]
            while self.eat(","):
                nums.append(self.number())
            if len(nums) == 4:
                q.bbox = Bbox(*map(float, nums))
            elif len(nums) == 1:
                q.ids = (q.ids or []) + [int(nums[0])]
            else:
                self.err("bbox needs four numbers")
        else:
            w = self.word()
            if w == "id":
                self.expect(":")
                ids = [int(self.number())]
                while self.eat(","):
                    ids.append(int(self.number()))
                q.ids = (q.ids or []) + ids
            elif w in ("w", "r", "bn", "bw", "br"):
                src = self.setname() or "_"
                role = None
                if self.eat(":"):
                    role = self.string()
                q.recurse.append((w, src, role))
            elif w == "around":
                src = self.setname()
                self.expect(":")
                radius = float(self.number())
                if src is not None:
                    q.around = (radius, src)
                else:
                    pts = []
                    while self.eat(","):
                        lat = float(self.number())
                        self.expect(",")
                        pts.append((lat, float(self.number())))
                    if not pts:
                        self.err("around needs a point or a set")
                    q.around = (radius, pts)
            elif w in ("poly", "area", "pivot", "if", "user", "uid", "newer", "changed", "user_touched"):
                raise QLError(f"line {self.line()}: static error: the filter '({w}...)' is not supported yet.")
            else:
                self.err(f"unknown filter '{w}'")
        self.expect(")")


def parse(text):
    return Parser(text).script()


# ---- executor ----------------------------------------------------------------------

class Executor:
    def __init__(self, engine, timestamp=""):
        self.e = engine
        self.timestamp = timestamp

    def run(self, script, json_callback=None):
        st = script.settings
        doc = Document(fmt=st.get("out", "xml"), csv=st.get("csv"), timestamp=self.timestamp,
                       json_callback=json_callback)
        self.global_bbox = st.get("bbox")
        self.sets = {"_": self.e._new("SELECT NULL::VARCHAR AS type, NULL::BIGINT AS id WHERE false")}
        self.doc = doc
        for stmt in script.body:
            self.exec(stmt)
        return doc

    def get(self, name):
        if name not in self.sets:
            self.sets[name] = self.e._new("SELECT NULL::VARCHAR AS type, NULL::BIGINT AS id WHERE false")
        return self.sets[name]

    def exec(self, st):
        """Execute a statement; returns the result set table (also stored in st.into)."""
        if isinstance(st, Out):
            self.doc.out(self.e, self.get(st.src), st.opts)
            return None
        if isinstance(st, Item):
            res = self.get(st.src)
        elif isinstance(st, Recurse):
            res = self.e.recurse(self.get(st.src), st.op)
        elif isinstance(st, Union):
            parts = [r for r in (self.exec(s) for s in st.body) if r is not None]
            res = self.e.union(*parts) if parts else self.get("__empty")
        elif isinstance(st, Difference):
            a = self.exec(st.left)
            b = self.exec(st.right)
            res = self.e.difference(a, b)
        elif isinstance(st, Query):
            res = self.query(st)
        else:
            raise QLError(f"unsupported statement {st!r}")
        self.sets[st.into] = res
        return res

    def query(self, q):
        e = self.e
        parts = []
        for t in q.types:
            ids = None if q.ids is None else set(q.ids)
            # input sets and recurse filters restrict ids (intersection), like Overpass constraints
            for name in q.inputs:
                ids = self._meet(ids, e._ids_of(self.get(name), t))
            for kind, src, role in q.recurse:
                s = self.get(src)
                if kind == "w":
                    cand = e.way_node_ids(s) if t == "n" else []
                elif kind == "r":
                    cand = e.member_ids(s, t, role)
                elif kind == "bn":
                    cand = e.parent_way_ids(s) if t == "w" else e.parent_rel_ids(s, "n", role) if t == "r" else []
                elif kind == "bw":
                    cand = e.parent_rel_ids(s, "w", role) if t == "r" else []
                elif kind == "br":
                    cand = e.parent_rel_ids(s, "r", role) if t == "r" else []
                ids = self._meet(ids, cand)
            bbox = q.bbox
            if self.global_bbox is not None and bbox is None:
                bbox = self.global_bbox
            elif self.global_bbox is not None:
                bbox = intersect_bbox(bbox, self.global_bbox)
            if q.around is not None:
                ids, bbox = self._around(q, t, ids, bbox)
                if ids is not None and not ids:
                    continue
            if ids is not None and not ids:
                continue
            if bbox is None and ids is None and not q.filters:
                raise QLError(f"line {q.line}: static error: a query needs at least one condition.")
            parts.append(e._query_one(t, bbox, list(q.filters), None if ids is None else sorted(ids), None))
        if not parts:
            return e._new("SELECT NULL::VARCHAR AS type, NULL::BIGINT AS id WHERE false")
        return e._new(" UNION ALL ".join(parts))

    @staticmethod
    def _meet(ids, cand):
        cand = set(cand)
        return cand if ids is None else ids & cand

    def _around(self, q, t, ids, bbox):
        """(around:r, points) / (around.set:r) via engine.around_ids (great-circle distance)."""
        radius, where = q.around
        pts = where if isinstance(where, list) else None
        src = None if pts is not None else self.get(where)
        cand = self.e.around_ids(t, radius, pts=pts, src=src, bbox=bbox, filters=q.filters)
        return self._meet(ids, cand), bbox


def intersect_bbox(a, b):
    return Bbox(max(a.s, b.s), max(a.w, b.w), min(a.n, b.n), min(a.e, b.e))
