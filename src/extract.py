"""Stage 1: OSM PBF -> raw Parquet (one pass, pyosmium with node locations).

Writes data/raw/{nodes,ways,relations}.parquet. Spatial keys are computed in build_lake.py
so the same logic can later run over osm-pds ORC instead of PBF.
"""
import argparse
import pathlib
import time

import numpy as np
import osmium
import pyarrow as pa
import pyarrow.parquet as pq

TAGS = pa.map_(pa.string(), pa.string())
META = [("version", pa.int32()), ("ts", pa.timestamp("s", tz="UTC")), ("changeset", pa.int64()),
        ("uid", pa.int32()), ("user", pa.string())]
SCHEMAS = {
    "nodes": pa.schema([("id", pa.int64()), ("lat", pa.int32()), ("lon", pa.int32()), ("tags", TAGS), *META]),
    "ways": pa.schema([("id", pa.int64()), ("nds", pa.list_(pa.int64())),
                       ("nd_lat", pa.list_(pa.int32())), ("nd_lon", pa.list_(pa.int32())),
                       ("tags", TAGS), *META]),
    "relations": pa.schema([("id", pa.int64()),
                            ("members", pa.list_(pa.struct([("type", pa.string()), ("ref", pa.int64()),
                                                            ("role", pa.string())]))),
                            ("tags", TAGS), *META]),
}
BATCH = 200_000
NO_COORD = np.iinfo(np.int32).min  # node missing from the extract


class Sink:
    def __init__(self, out_dir, kind):
        self.kind = kind
        self.schema = SCHEMAS[kind]
        self.writer = pq.ParquetWriter(out_dir / f"{kind}.parquet", self.schema, compression="zstd")
        self.cols = {f.name: [] for f in self.schema}
        self.count = 0

    def add(self, row):
        for k, v in zip(self.cols, row):
            self.cols[k].append(v)
        if len(self.cols["id"]) >= BATCH:
            self.flush()

    def flush(self):
        n = len(self.cols["id"])
        if n:
            self.writer.write_table(pa.table(self.cols, schema=self.schema))
            self.count += n
            self.cols = {k: [] for k in self.cols}

    def close(self):
        self.flush()
        self.writer.close()


def meta(o):
    return (o.version, o.timestamp, o.changeset, o.uid, o.user)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("pbf")
    ap.add_argument("--out", default="data/raw")
    args = ap.parse_args()
    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    sinks = {k: Sink(out, k) for k in SCHEMAS}
    t0 = time.time()
    fp = osmium.FileProcessor(args.pbf).with_locations()
    for o in fp:
        tags = [(t.k, t.v) for t in o.tags]
        if o.is_node():
            loc = o.location
            sinks["nodes"].add((o.id, loc.y, loc.x, tags, *meta(o)))
        elif o.is_way():
            refs, lats, lons = [], [], []
            for n in o.nodes:
                refs.append(n.ref)
                if n.location.valid():
                    lats.append(n.location.y)
                    lons.append(n.location.x)
                else:
                    lats.append(NO_COORD)
                    lons.append(NO_COORD)
            sinks["ways"].add((o.id, refs, lats, lons, tags, *meta(o)))
        elif o.is_relation():
            members = [{"type": m.type, "ref": m.ref, "role": m.role} for m in o.members]
            sinks["relations"].add((o.id, members, tags, *meta(o)))
    for s in sinks.values():
        s.close()
    print({k: s.count for k, s in sinks.items()}, f"{time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
