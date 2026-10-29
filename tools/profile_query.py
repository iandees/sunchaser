"""Per-statement request/byte breakdown of one corpus query against the range server."""
import json, pathlib, re, sys, time, urllib.request
ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "queries")]
import duckdb
from corpus import CORPUS
from engine import Engine, Plan

B = "http://127.0.0.1:8765/lake"
st = lambda p: json.loads(urllib.request.urlopen(f"{B.rsplit('/', 1)[0]}/{p}").read())
name, *settings = sys.argv[1:]
con = duckdb.connect()
con.sql("LOAD ducklake; LOAD httpfs; SET parquet_metadata_cache = true; SET enable_external_file_cache = false")
for s in settings:
    con.sql("SET " + s)
con.sql(f"ATTACH 'ducklake:{B}/osm.ducklake' AS lake (READ_ONLY, DATA_PATH '{B}/data/', OVERRIDE_DATA_PATH true)")
e = Engine(con, "lake")
prog = dict((n, p) for n, _, p in CORPUS)[name]
prog(e)  # warm footers/catalog


def trace(sql, fn):
    st("__reset"); t = time.perf_counter()
    rel = fn()
    try:
        rel.fetchall() if rel is not None else None
    except Exception:
        pass
    s = st("__stats")
    tables = sorted(set(re.findall(r"lake\.(\w+)", sql)))
    files = {k.split("/")[3] for k in s["by_file"] if k.startswith("/data/")}
    cat = {}
    for k, (r, b) in s["by_file"].items():
        kk = "catalog" if k.endswith(".ducklake") else k.split("/")[4]
        cat[kk] = cat.get(kk, 0) + r
    print(cat)
    print(f"{time.perf_counter() - t:6.3f}s {s['requests']:5} req {s['bytes'] >> 10:7} KiB  {','.join(tables) or '-':<28} {' '.join(sql.split())[:90]}")
    return rel

e.trace = trace
prog(e)
