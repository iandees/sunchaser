"""Overpass evaluators for (if: ...) filters (statements/*_endomorphisms.cc, unary_*.cc, binary_operators.cc).

Supported: string and number literals; t["key"] / t[key]; is_tag("k"); count_tags(); count_members();
count_distinct_members(); id(); type(); length(); is_closed(); number(x); is_number(x); suffix? no;
operators ! - * / + - < <= > >= == != && || and the ternary ?:. Values are strings, as in Overpass:
comparisons and arithmetic are numeric when both sides parse as numbers, lexicographic otherwise.

An element is a dict: {"type": "node"|"way"|"relation"|"area", "id", "tags": {...},
"coords": [(lat, lon), ...] (ways), "nds": [...] (ways), "members": [...] (relations)}.
"""
import math
import re

TOKEN = re.compile(r"""\s*(?:(?P<num>\d+\.\d*|\.\d+|\d+(?:[eE][-+]?\d+)?)|(?P<str>"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')|
                     (?P<op>&&|\|\||==|!=|<=|>=|[!<>+\-*/?:()\[\],])|(?P<name>[A-Za-z_][A-Za-z0-9_]*))""", re.X)


class EvalError(Exception):
    pass


def num(v):
    try:
        f = float(v)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def fmt(x):
    """Overpass prints whole numbers without a fraction."""
    if isinstance(x, bool):
        return "1" if x else "0"
    if isinstance(x, float):
        return str(int(x)) if x.is_integer() else repr(x)
    return str(x)


def truthy(v):
    n = num(v)
    return (n != 0) if n is not None else v != ""


def tokenize(text):
    out, pos = [], 0
    text = text.strip()
    while pos < len(text):
        m = TOKEN.match(text, pos)
        if not m or m.end() == pos:
            raise EvalError(f"cannot parse evaluator near '{text[pos:pos + 15]}'")
        kind = m.lastgroup
        val = m.group(kind)
        if kind == "str":
            val = bytes(val[1:-1], "utf-8").decode("unicode_escape").encode("latin-1").decode("utf-8")
        out.append((kind, val))
        pos = m.end()
    return out


class Parser:
    def __init__(self, text):
        self.toks = tokenize(text)
        self.i = 0

    def peek(self):
        return self.toks[self.i] if self.i < len(self.toks) else (None, None)

    def take(self, val=None):
        tok = self.peek()
        if tok[0] is None or (val is not None and tok[1] != val):
            raise EvalError(f"expected {val!r}, found {tok[1]!r}")
        self.i += 1
        return tok

    def parse(self):
        e = self.ternary()
        if self.i != len(self.toks):
            raise EvalError(f"unexpected {self.peek()[1]!r}")
        return e

    def ternary(self):
        cond = self.binary(0)
        if self.peek()[1] == "?":
            self.take("?")
            a = self.ternary()
            self.take(":")
            b = self.ternary()
            return lambda el: a(el) if truthy(cond(el)) else b(el)
        return cond

    LEVELS = [["||"], ["&&"], ["==", "!="], ["<", "<=", ">", ">="], ["+", "-"], ["*", "/"]]

    def binary(self, level):
        if level == len(self.LEVELS):
            return self.unary()
        left = self.binary(level + 1)
        while self.peek()[0] == "op" and self.peek()[1] in self.LEVELS[level]:
            op = self.take()[1]
            right = self.binary(level + 1)
            left = binop(op, left, right)
        return left

    def unary(self):
        tok = self.peek()
        if tok[1] == "!":
            self.take()
            f = self.unary()
            return lambda el: "0" if truthy(f(el)) else "1"
        if tok[1] == "-":
            self.take()
            f = self.unary()
            return lambda el: fmt(-num(f(el))) if num(f(el)) is not None else "NaN"
        return self.primary()

    def primary(self):
        kind, val = self.take()
        if kind == "num":
            return lambda el, v=val: v
        if kind == "str":
            return lambda el, v=val: v
        if val == "(":
            e = self.ternary()
            self.take(")")
            return e
        if kind == "name":
            if val == "t" and self.peek()[1] == "[":
                self.take("[")
                k = self.take()[1]
                self.take("]")
                return lambda el, k=k: (el.get("tags") or {}).get(k, "")
            if self.peek()[1] == "(":
                self.take("(")
                args = []
                while self.peek()[1] != ")":
                    args.append(self.ternary())
                    if self.peek()[1] == ",":
                        self.take(",")
                self.take(")")
                return function(val, args)
        raise EvalError(f"unexpected {val!r}")


def binop(op, a, b):
    def cmp(el):
        x, y = a(el), b(el)
        nx, ny = num(x), num(y)
        if nx is not None and ny is not None:
            x, y = nx, ny
        return {"==": x == y, "!=": x != y, "<": x < y, "<=": x <= y, ">": x > y, ">=": x >= y}[op]

    if op in ("==", "!=", "<", "<=", ">", ">="):
        return lambda el: "1" if cmp(el) else "0"
    if op == "&&":
        return lambda el: "1" if truthy(a(el)) and truthy(b(el)) else "0"
    if op == "||":
        return lambda el: "1" if truthy(a(el)) or truthy(b(el)) else "0"

    def arith(el):
        x, y = num(a(el)), num(b(el))
        if x is None or y is None:
            return a(el) + b(el) if op == "+" else "NaN"  # "+" concatenates strings
        if op == "/" and y == 0:
            return "NaN"
        return fmt({"+": x + y, "-": x - y, "*": x * y, "/": x / y}[op])
    return arith


def _length(el):
    """core/geometry.cc length(): sum of Overpass great-circle distances between vertices."""
    from engine import great_circle_dist
    pts = [p for p in el.get("coords") or [] if p is not None]
    return fmt(round(sum(great_circle_dist(*a, *b) for a, b in zip(pts, pts[1:])), 3))


FUNCS = {
    "is_tag": lambda el, a: "1" if a(el) in (el.get("tags") or {}) else "0",
    "count_tags": lambda el: str(len(el.get("tags") or {})),
    "count_members": lambda el: str(len(el.get("members") or el.get("nds") or [])),
    "count_distinct_members": lambda el: str(len({(m["type"], m["ref"]) for m in el.get("members") or []}
                                                 or set(el.get("nds") or []))),
    "id": lambda el: str(el["id"]),
    "type": lambda el: el["type"],
    "length": _length,
    "is_closed": lambda el: "1" if el.get("nds") and len(el["nds"]) > 1 and el["nds"][0] == el["nds"][-1] else "0",
    "number": lambda el, a: fmt(num(a(el))) if num(a(el)) is not None else "NaN",
    "is_number": lambda el, a: "1" if num(a(el)) is not None else "0",
}


def function(name, args):
    f = FUNCS.get(name)
    if f is None:
        raise EvalError(f"evaluator function {name}() is not supported")
    return lambda el: f(el, *args)


def compile_expr(text):
    return Parser(text).parse()


def needs(text):
    """Which element data the expression reads (to avoid fetching geometry when unused)."""
    return {"coords": "length(" in text, "nds": "is_closed(" in text or "count_members(" in text
            or "count_distinct_members(" in text, "members": "count_members(" in text
            or "count_distinct_members(" in text}
