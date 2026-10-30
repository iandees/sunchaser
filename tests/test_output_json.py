"""The DuckDB JSON serialiser (Document._sql_json) must stay byte-identical to the Python one
(Document._line_json): the first is the fast path for big json outputs, the second the reference.

  python -m pytest tests/test_output_json.py
"""
import json
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src")]
import duckdb  # noqa: E402

import output  # noqa: E402
from output import GEOMS, MODES, Document, OutOptions  # noqa: E402

NASTY = {"name": 'He said "hi"', "note": "back\\slash", "ü": "Bäckerei Ünal 東京", "empty": "",
         "k\"q": "v", "mix": 'a":"b', "comma": 'x","y', "tab": "a\tb", "nl": "a\nb"}

NODES = [  # id, lat_e7, lon_e7, tags
    (1, 525200000, 134050000, {}),
    (2, 525200001, 134050002, {"amenity": "cafe", **NASTY}),
    (3, -339000000, -1511000000, {"a": "b"}),
    (4, 0, 0, {}),
    (5, -5, 5, {"x": "y"}),
]
NO = -2147483648
WAYS = [  # id, nodes, (lat, lon) per node (NO = node missing from the extract), tags
    (10, [1, 2, 3], [(525200000, 134050000), (525210000, 134060000), (525220000, 134040000)],
     {"highway": "primary", **NASTY}),
    (11, [4], [(525200000, 134050000)], {}),
    (12, [1, 2, 3, 4], [(525200000, 134050000), (NO, NO), (525220000, 134040000), (525230000, 134070001)],
     {"building": "yes"}),
    (13, [], [], {}),
    (14, [5, 6], [(-5, 5), (-1000000000, -1800000000)], {"name": "Ü"}),
]
RELS = [(20, [{"type": "n", "ref": 1, "role": 'out"er'}, {"type": "w", "ref": 10, "role": ""}],
         {"type": "route", **NASTY})]


class FakeEngine:
    """The element fetch Document.out makes, answered from the in-memory tables."""

    def __init__(self, con):
        self.con = con

    def elements_by_id(self, t, ids, cols, select=None):
        tbl = {"n": "nodes", "w": "ways", "r": "rels"}[t]
        ids = ",".join(map(str, sorted(set(ids))))
        return self.con.sql(f"SELECT {select or ', '.join(cols)} FROM {tbl} WHERE id IN ({ids})").fetchall()


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    c.sql("CREATE TABLE nodes (id BIGINT, lat INTEGER, lon INTEGER, tags MAP(VARCHAR, VARCHAR))")
    c.sql("CREATE TABLE ways (id BIGINT, nds BIGINT[], nd_lat INTEGER[], nd_lon INTEGER[], tags MAP(VARCHAR, VARCHAR), qt UINTEGER)")
    c.sql("CREATE TABLE rels (id BIGINT, members STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[], "
          "tags MAP(VARCHAR, VARCHAR))")
    for i, la, lo, tg in NODES:
        c.execute("INSERT INTO nodes VALUES (?, ?, ?, ?)", [i, la, lo, tg])
    for i, nds, pts, tg in WAYS:
        c.execute("INSERT INTO ways VALUES (?, ?, ?, ?, ?, 0)", [i, nds, [p[0] for p in pts], [p[1] for p in pts], tg])
    for i, ms, tg in RELS:
        c.execute("INSERT INTO rels VALUES (?, ?, ?)", [i, ms, tg])
    return c


def python_lines(con, t, ids, bits, geo):
    d = Document(fmt="json")
    found = {it["id"]: it for it in getattr(d, f"_items_{t}")(FakeEngine(con), ids, bits, geo)}
    out = {}
    for i in ids:
        found[i]["bits"] = bits
        out[i] = d._line_json(found[i])
    return out


def sql_lines(con, t, ids, bits, geo):
    cols, select = Document(fmt="json")._sql_json(t, bits, geo)
    return dict(FakeEngine(con).elements_by_id(t, ids, cols, select=f"id, {select}"))


MODE_GEOM = [(m, g) for m in ("ids", "skel", "body", "tags") for g in (None, "geom", "bb", "center")]


@pytest.mark.parametrize("noids", [False, True])
@pytest.mark.parametrize("mode,geometry", MODE_GEOM)
@pytest.mark.parametrize("t,ids", [("n", [n[0] for n in NODES]), ("w", [w[0] for w in WAYS])])
def test_sql_and_line_json_are_byte_identical(con, t, ids, mode, geometry, noids):
    bits = OutOptions(mode=mode, geometry=geometry, noids=noids).bits()
    geo = bits & (output.GEOMETRY | output.BOUNDS | output.CENTER)
    got = sql_lines(con, t, ids, bits, geo)
    assert got == python_lines(con, t, ids, bits, geo)
    for line in got.values():
        json.loads(line)  # each line is valid JSON on its own


def test_paths_that_need_python(con):
    d = Document(fmt="json")
    assert d._sql_json("n", MODES["meta"], 0) is None                 # out meta
    assert d._sql_json("r", MODES["body"], 0) is None                 # relations
    assert Document(fmt="xml")._sql_json("n", MODES["body"], 0) is None
    d.clip = object()
    assert d._sql_json("w", MODES["body"] | GEOMS["geom"], output.GEOMETRY) is None  # clipped geometry


def test_meta_relation_and_count_lines_are_valid_json(con):
    bits = OutOptions(mode="body", geometry="center").bits()
    for line in python_lines(con, "r", [20], bits, bits & output.CENTER).values():
        assert json.loads(line)["members"][0]["role"] == 'out"er'
    d = Document(fmt="json")
    it = {"type": "node", "id": 1, "lat": 525200000, "lon": 134050000, "bits": MODES["meta"],
          "meta": (3, 1700000000, 99, 7, "Ünal")}
    assert json.loads(d._line_json(it))["user"] == "Ünal"
    line = d._line_json({"type": "count", "id": 0, "tags": [("nodes", "2"), ("total", "2")]})
    assert json.loads(line) == {"type": "count", "id": 0, "tags": {"nodes": "2", "total": "2"}}
