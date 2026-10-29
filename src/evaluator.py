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


# ---- Overpass's string <-> number conversions (data/utils.h), which follow C's strtod/strtoll ----

_DEC = r"(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?"
_HEX = r"0[xX](?:[0-9a-fA-F]+\.?[0-9a-fA-F]*|\.[0-9a-fA-F]+)(?:[pP][-+]?\d+)?"
_STRTOD = re.compile(rf"\s*([-+]?)(?:(inf(?:inity)?)|(nan(?:\([0-9A-Za-z_]*\))?)|({_HEX})|({_DEC}))", re.I)
_STRTOLL = re.compile(r"\s*([-+]?)(?:0[xX]([0-9a-fA-F]+)|(0[0-7]*)|([1-9]\d*))")
_I64 = 1 << 63


def _strtod(v):
    """(value, chars consumed, range error) or None, like strtod()."""
    m = _STRTOD.match(v)
    if not m:
        return None
    sign = -1.0 if m.group(1) == "-" else 1.0
    if m.group(2):
        return sign * math.inf, m.end(), False
    if m.group(3):
        return math.nan, m.end(), False
    x = float.fromhex(m.group(4)) if m.group(4) else float(m.group(5))
    return sign * x, m.end(), math.isinf(x)


def try_double(v):
    r = _strtod(v) if v else None
    return r[0] if r and r[1] == len(v) else None


def try_starts_with_double(v):
    r = _strtod(v) if v else None
    return r[0] if r and not r[2] else None


def try_int64(v):
    m = _STRTOLL.match(v) if v else None
    if not m or m.end() != len(v):
        return None
    x = int(m.group(2), 16) if m.group(2) else int(m.group(3), 8) if m.group(3) else int(m.group(4))
    x = -x if m.group(1) == "-" else x
    return max(-_I64, min(_I64 - 1, x))  # strtoll saturates (try_int64 ignores errno)


def _wrap(x):
    return (x + _I64) % (1 << 64) - _I64


def to_string(x):
    """std::ostream << setprecision(14), as Overpass's to_string()."""
    if isinstance(x, int):
        return str(x)
    if math.isnan(x):
        return "-nan" if math.copysign(1, x) < 0 else "nan"
    if math.isinf(x):
        return "inf" if x > 0 else "-inf"
    return f"{x:.14g}"


def num(v):
    """A string's numeric value (int64 preferred, as the operators try it first), or None."""
    i = try_int64(v)
    return i if i is not None else try_double(v)


def truthy(v):
    """string_represents_boolean_true"""
    d = try_double(v)
    return d != 0 if d is not None else v != ""


def fmt(x):
    if isinstance(x, bool):
        return "1" if x else "0"
    return to_string(x)


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
            return lambda el: _negate(f(el))
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


def _compare(op, x, y):
    """Evaluator_Equal/Less/...: int64 if both parse, else double, else byte-wise string order."""
    for conv in (try_int64, try_double):
        nx, ny = conv(x), conv(y)
        if nx is not None and ny is not None:
            x, y = nx, ny
            break
    else:
        x, y = x.encode(), y.encode()
    return {"==": x == y, "!=": x != y, "<": x < y, "<=": x <= y, ">": x > y, ">=": x >= y}[op]


def _arith(op, x, y):
    """Evaluator_Plus/Minus/Times (int64 first) and Divided (double only)."""
    if op != "/":
        ix, iy = try_int64(x), try_int64(y)
        if ix is not None and iy is not None:
            return to_string(_wrap({"+": ix + iy, "-": ix - iy, "*": ix * iy}[op]))
    dx, dy = try_double(x), try_double(y)
    if dx is None or dy is None:
        return x + y if op == "+" else "NaN"  # "+" concatenates strings
    if op == "/":
        if dy == 0:
            return to_string(math.nan if dx == 0 or math.isnan(dx) else math.copysign(math.inf, dx) * math.copysign(1, dy))
        return to_string(dx / dy)
    try:
        return to_string({"+": dx + dy, "-": dx - dy, "*": dx * dy}[op])
    except OverflowError:
        return to_string(math.inf)


def binop(op, a, b):
    if op in ("==", "!=", "<", "<=", ">", ">="):
        return lambda el: "1" if _compare(op, a(el), b(el)) else "0"
    if op == "&&":
        return lambda el: "1" if truthy(a(el)) and truthy(b(el)) else "0"
    if op == "||":
        return lambda el: "1" if truthy(a(el)) or truthy(b(el)) else "0"
    return lambda el: _arith(op, a(el), b(el))


def _negate(v):
    i = try_int64(v)
    if i is not None:
        return to_string(_wrap(-i))
    d = try_double(v)
    return to_string(-d) if d is not None else "NaN"


def _number(v):
    """Evaluator_Number"""
    i = try_int64(v)
    if i is not None:
        return to_string(i)
    d = try_starts_with_double(v)
    return to_string(d) if d is not None else "NaN"


def _length(el):
    """core/geometry.cc length(): sum of Overpass great-circle distances between vertices."""
    from engine import great_circle_dist
    pts = [p for p in el.get("coords") or [] if p is not None]
    return f"{sum(great_circle_dist(*a, *b) for a, b in zip(pts, pts[1:])):.3f}"  # fixed_to_string(.., 3)


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
    "number": lambda el, a: _number(a(el)),
    "is_number": lambda el, a: "1" if try_int64(a(el)) is not None or try_starts_with_double(a(el)) is not None else "0",
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
