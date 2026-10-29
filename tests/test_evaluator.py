"""Evaluator number semantics (Overpass data/utils.h: strtod/strtoll-based conversions)."""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))
from evaluator import compile_expr  # noqa: E402

EL = {"type": "way", "id": 7, "tags": {"lv": "5 floors", "x": "abc", "h": "0x10", "o": "010", "d": "2.50"},
      "nds": [1, 2, 3, 1]}

CASES = [
    ('number(t["x"]) >= 5', "0"),        # "NaN" compares as a NaN double, not as a string
    ('number(t["lv"])', "5"),            # numeric prefix
    ('number("NaN")', "nan"),            # strtod accepts nan; to_string prints it lowercase
    ('t["h"] + 1', "17"),                # strtoll base 0: hex
    ('t["o"] + 1', "9"),                 # ... and octal
    ('t["d"] * 2', "5"),
    ("1/3", "0.33333333333333"),         # setprecision(14)
    ("1/0", "inf"),
    ('-t["x"]', "NaN"),
    ('"10" < "9"', "0"),                 # numeric comparison when both parse
    ('"b" < "a"', "0"),
    ('"x" + "y"', "xy"),                 # + concatenates non-numbers
    ('is_number("5m")', "1"),
    ('!"0.0"', "1"),
    ("is_closed()", "1"),
    ("id() == 7 && type() == \"way\"", "1"),
]


def test_cases():
    for expr, want in CASES:
        assert compile_expr(expr)(EL) == want, expr
