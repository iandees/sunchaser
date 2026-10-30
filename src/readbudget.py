"""Pre-read budget: estimate, from the DuckLake catalog alone, how many bytes a table scan will read
from object storage, and refuse a query whose scans add up past a limit before they run.

The catalog records for every data file its size, and for every column of every file its compressed
size and min/max (ducklake_file_column_stats). Engine scans carry zone-map predicates (qt / qt_key31 /
lat / lon / id ranges), so the files DuckLake can prune are the ones whose [min, max] misses every
range. The estimate is the compressed size of the columns the scan references, summed over the files
that survive. It is an UPPER BOUND at file granularity: Parquet row-group pruning inside a surviving
file, which the catalog knows nothing about, can only make the real read smaller. A predicate the
parser does not understand is ignored (more files survive, never fewer), and any catalog error means
"no estimate" (the query runs), so a failure here can never reject a query that would have passed.

Cost: one catalog query per table per request (a few ms on a local SQLite catalog), then arithmetic.
"""
import re

import duckdb

_STR = re.compile(r"'(?:[^']|'')*'")
_BETWEEN = re.compile(r"(\w+)\s+BETWEEN\s+(-?\d+)\s+AND\s+(-?\d+)")
_CMP = re.compile(r"^(\w+)\s*(>=|<=|=)\s*(-?\d+)$")
_BTW1 = re.compile(r"^(\w+) BETWEEN (-?\d+) ~ (-?\d+)$")
_IDLIST = re.compile(r"^(\w+) IN \((-?\d+(?:\s*,\s*-?\d+)*)\)$")
INF = 1 << 80


class ReadBudgetExceeded(Exception):
    def __init__(self, estimated, limit):
        super().__init__(f"estimated read {estimated} bytes exceeds the limit of {limit} bytes")
        self.estimated, self.limit = estimated, limit


def _split(s, sep):
    """Split on `sep` (' AND ' / ' OR ') outside parentheses."""
    out, depth, start, i = [], 0, 0, 0
    while i < len(s):
        ch = s[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0 and s.startswith(sep, i):
            out.append(s[start:i])
            i += len(sep)
            start = i
            continue
        i += 1
    out.append(s[start:])
    return [p.strip() for p in out]


def _strip(s):
    while s.startswith("(") and s.endswith(")") and len(_split(s[1:-1], " AND ")) == 1 and _balanced(s[1:-1]):
        s = s[1:-1].strip()
    return s


def _balanced(s):
    depth = 0
    for ch in s:
        depth += (ch == "(") - (ch == ")")
        if depth < 0:
            return False
    return depth == 0


def _leaf(s):
    """One comparison -> (column, lo, hi), or None."""
    s = _strip(s)
    if m := _BTW1.match(s):
        return m[1], int(m[2]), int(m[3])
    if m := _CMP.match(s):
        v = int(m[3])
        return (m[1], v, INF) if m[2] == ">=" else (m[1], -INF, v) if m[2] == "<=" else (m[1], v, v)
    if m := _IDLIST.match(s):
        vals = [int(x) for x in m[2].split(",")]
        return m[1], min(vals), max(vals)
    return None


def parse_ranges(preds):
    """Conjunctive SQL predicates -> [(column, [(lo, hi), ...])]: the column must fall in one of the
    intervals. Anything not understood is dropped (a weaker, still valid, filter)."""
    out = []
    for p in preds:
        p = _BETWEEN.sub(r"\1 BETWEEN \2 ~ \3", p)
        for conj in _split(p, " AND "):
            terms = [_leaf(t) for t in _split(_strip(conj), " OR ")]
            if terms and all(terms) and len({t[0] for t in terms}) == 1:
                out.append((terms[0][0], [(t[1], t[2]) for t in terms]))
    return out


class ReadBudget:
    """Accumulates estimated bytes over a query's scans; raises ReadBudgetExceeded past `limit`."""

    VIEWS = {"nodes": ("nodes_tagged", "nodes_untagged")}  # views over tables, as build_lake creates them

    def __init__(self, con, limit, catalog="lake"):
        self.con, self.limit, self.catalog = con, limit, catalog
        self.spent = 0
        self.log = []  # (table, files kept, files total, bytes) per scan, for tests and tooling
        self._files = {}

    # ---- catalog statistics ------------------------------------------------

    def files(self, table):
        """[{size, rows, cols: {top-level column: bytes}, ranges: {column: (min, max)}}] of the table's current files."""
        if table in self._files:
            return self._files[table]
        meta = f"__ducklake_metadata_{self.catalog}"
        try:
            rows = self.con.sql(f"""
                SELECT f.data_file_id, f.file_size_bytes, f.record_count, c.column_id, c.parent_column, c.column_name,
                       s.column_size_bytes, s.min_value, s.max_value
                FROM {meta}.ducklake_table t
                JOIN {meta}.ducklake_data_file f ON f.table_id = t.table_id AND f.end_snapshot IS NULL
                JOIN {meta}.ducklake_file_column_stats s ON s.data_file_id = f.data_file_id
                JOIN {meta}.ducklake_column c ON c.table_id = t.table_id AND c.column_id = s.column_id
                                              AND c.end_snapshot IS NULL
                WHERE t.table_name = '{table}' AND t.end_snapshot IS NULL""").fetchall()
            cols = self.con.sql(f"""
                SELECT c.column_id, c.parent_column, c.column_name FROM {meta}.ducklake_table t
                JOIN {meta}.ducklake_column c ON c.table_id = t.table_id AND c.end_snapshot IS NULL
                WHERE t.table_name = '{table}' AND t.end_snapshot IS NULL""").fetchall()
        except duckdb.Error:
            self._files[table] = None
            return None
        top = {r[0]: r[2] for r in cols if r[1] is None}
        parent = {r[0]: r[1] for r in cols}

        def root(cid):
            while parent.get(cid) is not None:
                cid = parent[cid]
            return cid

        files = {}
        for fid, size, nrows, cid, par, name, csize, lo, hi in rows:
            f = files.setdefault(fid, {"size": size, "rows": nrows, "cols": {}, "ranges": {}})
            col = top.get(root(cid))
            f["cols"][col] = f["cols"].get(col, 0) + (csize or 0)
            if par is None:
                try:
                    f["ranges"][name] = (int(lo), int(hi))
                except (TypeError, ValueError):
                    pass
        self._files[table] = list(files.values())
        return self._files[table]

    # ---- estimate ------------------------------------------------------------

    def tables_of(self, tbl):
        names = []
        for t in re.findall(rf"{self.catalog}\.(\w+)", tbl):
            names += self.VIEWS.get(t, (t,))
        return names

    def estimate(self, tbl, preds, sql):
        """Bytes the scan `sql` of `tbl` (a lake table, view or expression) may read, or None if unknown."""
        constraints = parse_ranges(preds)
        text = _STR.sub("''", sql)
        total, known = 0, False
        for table in self.tables_of(tbl):
            files = self.files(table)
            if files is None:
                continue
            known = True
            kept = [f for f in files if all(
                c not in f["ranges"] or any(f["ranges"][c][0] <= hi and lo <= f["ranges"][c][1] for lo, hi in iv)
                for c, iv in constraints)]
            names = {c for f in files for c in f["cols"]}
            used = {c for c in names if re.search(rf"\b{re.escape(c)}\b", text)} or names
            nbytes = sum(f["cols"].get(c, 0) for f in kept for c in used)
            self.log.append((table, len(kept), len(files), nbytes))
            total += nbytes
        return total if known else None

    def charge(self, tbl, preds, sql):
        est = self.estimate(tbl, preds, sql)
        if est:
            self.spent += est
            if self.limit and self.spent > self.limit:
                raise ReadBudgetExceeded(self.spent, self.limit)
