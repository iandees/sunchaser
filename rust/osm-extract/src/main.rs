//! osm-extract: OSM PBF -> bucketed raw Parquet for the lake build (src/build_lake.py).
//!
//! Everything that needs cross-element lookups is computed here, in one streaming pass, so the lake
//! build only has to sort:
//!   nodes/NNNN.parquet             id, lat, lon, tags, <meta>, qt
//!   ways/NNNN.parquet              id, nds, nd_lat, nd_lon, tags, <meta>, qt, min/max lat/lon
//!   relations/NNNN.parquet         id, members, tags, <meta>, qt, min/max lat/lon (node and way members)
//!   locator/{n,w,r}.parquet        id, qt in id order (the PBF's order: no sort needed)
//!   node_parent_ways/NNNN.parquet  node_id, way_id (distinct per way)
//!   manifest.json
//! With --history the input is a full-history file and the output also has nodes_history/,
//! ways_history/, relations_history/ and changes/ (see history.rs).
//! NNNN is a bucket of the table's sort key: the top --bucket-bits of qt for nodes, of the Overpass
//! Uint31 order key31(qt) for ways and relations, and of the node id for node_parent_ways. Buckets
//! are disjoint key ranges in key order, so each can be sorted on its own and appended.
//!
//! Lookups: node locations (for way geometry and keys) and way keys/bboxes (for relations). For
//! extracts they are id-sorted vectors in RAM. For the planet use --dense-index DIR: dense arrays
//! indexed by id in sparse mmap files (8 bytes per node id, 24 per way id: ~110 GB + ~35 GB of
//! address space, written once and read randomly; put them on local NVMe).
//!
//! Blocks are decoded in parallel in chunks. Within a chunk, node blocks are built first and their
//! locations stored, then way and relation blocks are built in parallel. This relies on the PBF being
//! sorted by type then id (checked), as planet files and extracts are.

mod history;
mod quadtile;

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use arrow::array::{
    Array, ArrayRef, Int32Builder, Int64Array, Int64Builder, ListBuilder, MapBuilder, MapFieldNames,
    StringBuilder, StructBuilder, TimestampMillisecondBuilder, UInt32Array, UInt32Builder,
};
use arrow::compute::take_record_batch;
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use memmap2::MmapMut;
use osmpbf::{BlobDecode, BlobReader, Info, PrimitiveBlock, RelMemberType};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;

const NO_COORD: i32 = i32::MIN;
const CHUNK_BLOBS: usize = 64;
const MISSING_BBOX: [i32; 4] = [i32::MAX, i32::MIN, i32::MAX, i32::MIN];

// ---- schema -------------------------------------------------------------------

fn map_names() -> MapFieldNames {
    MapFieldNames { entry: "entries".into(), key: "key".into(), value: "value".into() }
}

fn tags_field() -> Field {
    let entries = Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ])),
        false,
    );
    Field::new("tags", DataType::Map(Arc::new(entries), false), true)
}

fn meta_fields() -> Vec<Field> {
    vec![
        Field::new("version", DataType::Int32, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())), true),
        Field::new("changeset", DataType::Int64, true),
        Field::new("uid", DataType::Int32, true),
        Field::new("user", DataType::Utf8, true),
    ]
}

fn list_of(name: &str, dt: DataType) -> Field {
    Field::new(name, DataType::List(Arc::new(Field::new("item", dt, true))), true)
}

fn member_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("type", DataType::Utf8, true),
        Field::new("ref", DataType::Int64, true),
        Field::new("role", DataType::Utf8, true),
    ])
}

fn schema(kind: Kind) -> Arc<Schema> {
    let mut f = vec![Field::new("id", DataType::Int64, true)];
    match kind {
        Kind::Nodes => {
            f.push(Field::new("lat", DataType::Int32, true));
            f.push(Field::new("lon", DataType::Int32, true));
        }
        Kind::Ways => {
            f.push(list_of("nds", DataType::Int64));
            f.push(list_of("nd_lat", DataType::Int32));
            f.push(list_of("nd_lon", DataType::Int32));
        }
        Kind::Relations => {
            f.push(list_of("members", DataType::Struct(member_struct_fields())));
        }
    }
    f.push(tags_field());
    f.extend(meta_fields());
    f.push(Field::new("qt", DataType::UInt32, true));
    if kind != Kind::Nodes {
        for c in ["min_lat", "max_lat", "min_lon", "max_lon"] {
            f.push(Field::new(c, DataType::Int32, true));
        }
    }
    Arc::new(Schema::new(f))
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Kind {
    Nodes,
    Ways,
    Relations,
}

// ---- per-block builders -------------------------------------------------------

struct Common {
    id: Int64Builder,
    tags: MapBuilder<StringBuilder, StringBuilder>,
    version: Int32Builder,
    ts: TimestampMillisecondBuilder,
    changeset: Int64Builder,
    uid: Int32Builder,
    user: StringBuilder,
    qt: UInt32Builder,
}

impl Common {
    fn new() -> Self {
        Common {
            id: Int64Builder::new(),
            tags: MapBuilder::new(Some(map_names()), StringBuilder::new(), StringBuilder::new()),
            version: Int32Builder::new(),
            ts: TimestampMillisecondBuilder::new().with_timezone("UTC"),
            changeset: Int64Builder::new(),
            uid: Int32Builder::new(),
            user: StringBuilder::new(),
            qt: UInt32Builder::new(),
        }
    }

    fn tags<'a>(&mut self, it: impl Iterator<Item = (&'a str, &'a str)>) {
        for (k, v) in it {
            self.tags.keys().append_value(k);
            self.tags.values().append_value(v);
        }
        self.tags.append(true).unwrap();
    }

    fn meta(&mut self, version: i32, millis: i64, changeset: i64, uid: i32, user: &str) {
        self.version.append_value(version);
        self.ts.append_value(millis.div_euclid(1000) * 1000); // OSM timestamps are whole seconds
        self.changeset.append_value(changeset);
        self.uid.append_value(uid);
        self.user.append_value(user);
    }

    fn info(&mut self, info: &Info) {
        self.meta(
            info.version().unwrap_or(0),
            info.milli_timestamp().unwrap_or(0),
            info.changeset().unwrap_or(0),
            info.uid().unwrap_or(0),
            info.user().and_then(|u| u.ok()).unwrap_or(""),
        );
    }

    /// Columns after the type-specific ones, in schema order.
    fn finish(mut self, mut head: Vec<ArrayRef>) -> Vec<ArrayRef> {
        let tail: Vec<ArrayRef> = vec![
            Arc::new(self.tags.finish()),
            Arc::new(self.version.finish()),
            Arc::new(self.ts.finish()),
            Arc::new(self.changeset.finish()),
            Arc::new(self.uid.finish()),
            Arc::new(self.user.finish()),
            Arc::new(self.qt.finish()),
        ];
        head.insert(0, Arc::new(self.id.finish()));
        head.extend(tail);
        head
    }
}

/// Node block -> record batch + (id, lat, lon) for the location index.
fn build_nodes(block: &PrimitiveBlock) -> Result<Option<(RecordBatch, Vec<i64>, Vec<i32>, Vec<i32>)>> {
    let mut c = Common::new();
    let (mut lat, mut lon) = (Int32Builder::new(), Int32Builder::new());
    let (mut ids, mut lats, mut lons) = (Vec::new(), Vec::new(), Vec::new());
    for group in block.groups() {
        for n in group.dense_nodes() {
            let (la, lo) = (n.decimicro_lat(), n.decimicro_lon());
            c.id.append_value(n.id());
            lat.append_value(la);
            lon.append_value(lo);
            c.tags(n.tags());
            match n.info() {
                Some(i) => c.meta(i.version(), i.milli_timestamp(), i.changeset(), i.uid(), i.user().unwrap_or("")),
                None => c.meta(0, 0, 0, 0, ""),
            }
            c.qt.append_value(quadtile::node_index(la, lo));
            ids.push(n.id());
            lats.push(la);
            lons.push(lo);
        }
        for n in group.nodes() {
            let (la, lo) = (n.decimicro_lat(), n.decimicro_lon());
            c.id.append_value(n.id());
            lat.append_value(la);
            lon.append_value(lo);
            c.tags(n.tags());
            c.info(&n.info());
            c.qt.append_value(quadtile::node_index(la, lo));
            ids.push(n.id());
            lats.push(la);
            lons.push(lo);
        }
    }
    if ids.is_empty() {
        return Ok(None);
    }
    let cols = c.finish(vec![Arc::new(lat.finish()), Arc::new(lon.finish())]);
    Ok(Some((RecordBatch::try_new(schema(Kind::Nodes), cols)?, ids, lats, lons)))
}

// ---- id-keyed lookup stores ------------------------------------------------------

/// Fixed-size records by element id. An all-zero record means "absent", so encodings must never
/// produce one.
enum Store<const N: usize> {
    Sparse { ids: Vec<i64>, vals: Vec<[u8; N]> },
    Dense { map: MmapMut, cap: u64 },
}

impl<const N: usize> Store<N> {
    fn sparse() -> Self {
        Store::Sparse { ids: Vec::new(), vals: Vec::new() }
    }

    fn dense(path: &Path, max_id: u64) -> Result<Self> {
        let f = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(path)
            .with_context(|| format!("create {}", path.display()))?;
        f.set_len((max_id + 1) * N as u64)?; // sparse file: only touched pages use disk
        let map = unsafe { MmapMut::map_mut(&f)? };
        Ok(Store::Dense { map, cap: max_id + 1 })
    }

    /// Ids must arrive in increasing order (sorted PBF).
    fn put(&mut self, id: i64, v: [u8; N]) -> Result<()> {
        match self {
            Store::Sparse { ids, vals } => {
                if let Some(&last) = ids.last() {
                    if id <= last {
                        bail!("PBF not sorted by id ({id} after {last})");
                    }
                }
                ids.push(id);
                vals.push(v);
            }
            Store::Dense { map, cap } => {
                if id < 0 || id as u64 >= *cap {
                    bail!("id {id} beyond the dense index (raise --max-node-id / --max-way-id)");
                }
                let o = id as usize * N;
                map[o..o + N].copy_from_slice(&v);
            }
        }
        Ok(())
    }

    #[inline]
    fn get(&self, id: i64) -> Option<[u8; N]> {
        match self {
            Store::Sparse { ids, vals } => ids.binary_search(&id).ok().map(|i| vals[i]),
            Store::Dense { map, cap } => {
                if id < 0 || id as u64 >= *cap {
                    return None;
                }
                let o = id as usize * N;
                let v: [u8; N] = map[o..o + N].try_into().unwrap();
                if v.iter().all(|&b| b == 0) { None } else { Some(v) }
            }
        }
    }
}

/// Node location: lat offset so a real location is never all zero.
fn enc_node(lat: i32, lon: i32) -> [u8; 8] {
    let mut v = [0u8; 8];
    v[..4].copy_from_slice(&((lat as i64 + 900_000_001) as u32).to_le_bytes());
    v[4..].copy_from_slice(&lon.to_le_bytes());
    v
}

fn dec_node(v: [u8; 8]) -> (i32, i32) {
    let la = u32::from_le_bytes(v[..4].try_into().unwrap()) as i64 - 900_000_001;
    (la as i32, i32::from_le_bytes(v[4..].try_into().unwrap()))
}

/// Way record: present flag, qt, bbox (MISSING_BBOX when no node was located).
fn enc_way(qt: u32, b: [i32; 4]) -> [u8; 24] {
    let mut v = [0u8; 24];
    v[0] = 1;
    v[4..8].copy_from_slice(&qt.to_le_bytes());
    for (i, x) in b.iter().enumerate() {
        v[8 + 4 * i..12 + 4 * i].copy_from_slice(&x.to_le_bytes());
    }
    v
}

fn dec_way(v: [u8; 24]) -> (u32, [i32; 4]) {
    let qt = u32::from_le_bytes(v[4..8].try_into().unwrap());
    let mut b = [0i32; 4];
    for (i, x) in b.iter_mut().enumerate() {
        *x = i32::from_le_bytes(v[8 + 4 * i..12 + 4 * i].try_into().unwrap());
    }
    (qt, b)
}

fn extend_bbox(b: &mut [i32; 4], la_min: i32, la_max: i32, lo_min: i32, lo_max: i32) {
    b[0] = b[0].min(la_min);
    b[1] = b[1].max(la_max);
    b[2] = b[2].min(lo_min);
    b[3] = b[3].max(lo_max);
}

fn push_bbox(c: &mut [Int32Builder; 4], b: [i32; 4]) {
    for (i, x) in b.iter().enumerate() {
        if b[0] == i32::MAX { c[i].append_null() } else { c[i].append_value(*x) }
    }
}

fn bbox_builders() -> [Int32Builder; 4] {
    [Int32Builder::new(), Int32Builder::new(), Int32Builder::new(), Int32Builder::new()]
}

fn finish_bbox(c: [Int32Builder; 4]) -> Vec<ArrayRef> {
    c.into_iter().map(|mut b| Arc::new(b.finish()) as ArrayRef).collect()
}

struct WayPart {
    batch: RecordBatch,
    index: Vec<(i64, [u8; 24])>,
    parents: Vec<(i64, i64)>, // (node_id, way_id), distinct per way
}

fn build_ways(block: &PrimitiveBlock, loc: &Store<8>) -> Result<Option<WayPart>> {
    let mut c = Common::new();
    let mut nds = ListBuilder::new(Int64Builder::new());
    let mut nd_lat = ListBuilder::new(Int32Builder::new());
    let mut nd_lon = ListBuilder::new(Int32Builder::new());
    let mut bb = bbox_builders();
    let (mut index, mut parents) = (Vec::new(), Vec::new());
    let (mut node_idxs, mut refs) = (Vec::new(), Vec::new());
    for group in block.groups() {
        for w in group.ways() {
            node_idxs.clear();
            refs.clear();
            let mut b = MISSING_BBOX;
            for r in w.refs() {
                nds.values().append_value(r);
                refs.push(r);
                match loc.get(r).map(dec_node) {
                    Some((la, lo)) => {
                        nd_lat.values().append_value(la);
                        nd_lon.values().append_value(lo);
                        node_idxs.push(quadtile::node_index(la, lo));
                        extend_bbox(&mut b, la, la, lo, lo);
                    }
                    None => {
                        nd_lat.values().append_value(NO_COORD);
                        nd_lon.values().append_value(NO_COORD);
                    }
                }
            }
            nds.append(true);
            nd_lat.append(true);
            nd_lon.append(true);
            c.id.append_value(w.id());
            c.tags(w.tags());
            c.info(&w.info());
            let q = quadtile::calc_index(&node_idxs);
            c.qt.append_value(q);
            push_bbox(&mut bb, b);
            index.push((w.id(), enc_way(q, b)));
            refs.sort_unstable();
            refs.dedup();
            parents.extend(refs.iter().map(|&n| (n, w.id())));
        }
    }
    if index.is_empty() {
        return Ok(None);
    }
    let mut cols = c.finish(vec![Arc::new(nds.finish()), Arc::new(nd_lat.finish()), Arc::new(nd_lon.finish())]);
    cols.extend(finish_bbox(bb));
    Ok(Some(WayPart { batch: RecordBatch::try_new(schema(Kind::Ways), cols)?, index, parents }))
}

fn build_relations(block: &PrimitiveBlock, loc: &Store<8>, ways: &Store<24>) -> Result<Option<RecordBatch>> {
    let mut c = Common::new();
    let mut members = ListBuilder::new(StructBuilder::from_fields(member_struct_fields(), 0));
    let mut bb = bbox_builders();
    let mut any = false;
    let mut idxs = Vec::new();
    for group in block.groups() {
        for r in group.relations() {
            any = true;
            idxs.clear();
            let mut b = MISSING_BBOX;
            let mb = members.values();
            for m in r.members() {
                let t = match m.member_type {
                    RelMemberType::Node => {
                        if let Some((la, lo)) = loc.get(m.member_id).map(dec_node) {
                            idxs.push(quadtile::node_index(la, lo));
                            extend_bbox(&mut b, la, la, lo, lo);
                        }
                        "n"
                    }
                    RelMemberType::Way => {
                        if let Some((q, wb)) = ways.get(m.member_id).map(dec_way) {
                            idxs.push(q);
                            if wb[0] != i32::MAX {
                                extend_bbox(&mut b, wb[0], wb[1], wb[2], wb[3]);
                            }
                        }
                        "w"
                    }
                    RelMemberType::Relation => "r",
                };
                mb.field_builder::<StringBuilder>(0).unwrap().append_value(t);
                mb.field_builder::<Int64Builder>(1).unwrap().append_value(m.member_id);
                mb.field_builder::<StringBuilder>(2).unwrap().append_value(m.role().unwrap_or(""));
                mb.append(true);
            }
            members.append(true);
            c.id.append_value(r.id());
            c.tags(r.tags());
            c.info(&r.info());
            // Overpass: distinct, sorted member indexes (relation_updater.cc)
            idxs.sort_unstable();
            idxs.dedup();
            c.qt.append_value(quadtile::calc_index(&idxs));
            push_bbox(&mut bb, b);
        }
    }
    if !any {
        return Ok(None);
    }
    let mut cols = c.finish(vec![Arc::new(members.finish())]);
    cols.extend(finish_bbox(bb));
    Ok(Some(RecordBatch::try_new(schema(Kind::Relations), cols)?))
}

// ---- output ---------------------------------------------------------------------

fn props() -> Result<WriterProperties> {
    Ok(WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_max_row_group_row_count(Some(1 << 17))
        .build())
}

/// One lazily created Parquet file per bucket; buckets are written in parallel.
struct Buckets {
    dir: PathBuf,
    schema: Arc<Schema>,
    writers: Vec<Mutex<Option<ArrowWriter<File>>>>,
    rows: Vec<Mutex<u64>>,
}

impl Buckets {
    fn new(dir: PathBuf, schema: Arc<Schema>, n: usize) -> Result<Self> {
        std::fs::create_dir_all(&dir)?;
        Ok(Buckets {
            dir,
            schema,
            writers: (0..n).map(|_| Mutex::new(None)).collect(),
            rows: (0..n).map(|_| Mutex::new(0)).collect(),
        })
    }

    fn write(&self, bucket: usize, batch: &RecordBatch) -> Result<()> {
        let mut w = self.writers[bucket].lock().unwrap();
        if w.is_none() {
            let f = File::create(self.dir.join(format!("{bucket:04}.parquet")))?;
            *w = Some(ArrowWriter::try_new(f, self.schema.clone(), Some(props()?))?);
        }
        w.as_mut().unwrap().write(batch)?;
        *self.rows[bucket].lock().unwrap() += batch.num_rows() as u64;
        Ok(())
    }

    /// Split `batches` by bucket (rows keep their order) and write the pieces in parallel.
    fn write_split(&self, batches: &[RecordBatch], bucket_of: impl Fn(&RecordBatch) -> Vec<usize> + Sync) -> Result<()> {
        let mut pieces: BTreeMap<usize, Vec<RecordBatch>> = BTreeMap::new();
        for batch in batches {
            let mut by: BTreeMap<usize, Vec<u32>> = BTreeMap::new();
            for (row, b) in bucket_of(batch).into_iter().enumerate() {
                by.entry(b).or_default().push(row as u32);
            }
            for (b, rows) in by {
                let piece = if rows.len() == batch.num_rows() {
                    batch.clone()
                } else {
                    take_record_batch(batch, &UInt32Array::from(rows))?
                };
                pieces.entry(b).or_default().push(piece);
            }
        }
        pieces.into_par_iter().try_for_each(|(b, ps)| ps.iter().try_for_each(|p| self.write(b, p)))
    }

    /// Close all files; a table with no rows still gets one empty file so its schema is known.
    fn close(self) -> Result<u64> {
        let mut total = 0;
        for (w, r) in self.writers.into_iter().zip(self.rows) {
            if let Some(w) = w.into_inner().unwrap() {
                w.close()?;
            }
            total += r.into_inner().unwrap();
        }
        if total == 0 {
            let f = File::create(self.dir.join("0000.parquet"))?;
            ArrowWriter::try_new(f, self.schema.clone(), Some(props()?))?.close()?;
        }
        Ok(total)
    }
}

fn qt_column(batch: &RecordBatch) -> &UInt32Array {
    batch.column_by_name("qt").unwrap().as_any().downcast_ref::<UInt32Array>().unwrap()
}

fn key31(v: u32) -> u64 {
    (((v & 0x7FFF_FFFF) as u64) << 1) | (v >> 31) as u64
}

fn locator_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false), Field::new("qt", DataType::UInt32, false)]))
}

fn locator_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(locator_schema(), vec![batch.column(0).clone(), batch.column_by_name("qt").unwrap().clone()])?)
}

fn parents_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("node_id", DataType::Int64, false), Field::new("way_id", DataType::Int64, false)]))
}

// ---- driver ---------------------------------------------------------------------

/// ISO-8601 UTC for epoch seconds (civil-from-days, no date library needed).
fn iso8601(secs: i64) -> String {
    let (days, rem) = (secs.div_euclid(86400), secs.rem_euclid(86400));
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z", rem / 3600, rem % 3600 / 60, rem % 60)
}

/// `"osm_base": ..., "sequence": ...` manifest fields from the PBF header's replication info, so the
/// lake (and the updater) start where the file ends.
fn header_fields(input: &Path) -> Result<String> {
    let mut fields = String::new();
    if let Some(blob) = BlobReader::from_path(input)?.next() {
        if let Ok(BlobDecode::OsmHeader(h)) = blob?.decode() {
            if let Some(ts) = h.osmosis_replication_timestamp() {
                fields += &format!(", \"osm_base\": \"{}\"", iso8601(ts));
            }
            if let Some(seq) = h.osmosis_replication_sequence_number() {
                fields += &format!(", \"sequence\": {seq}");
            }
        }
    }
    Ok(fields)
}

struct Args {
    input: PathBuf,
    out: PathBuf,
    bucket_bits: u32,
    dense: Option<PathBuf>,
    max_node_id: u64,
    max_way_id: u64,
    history: bool,
}

fn parse_args() -> Result<Args> {
    let mut a = Args {
        input: PathBuf::new(),
        out: PathBuf::new(),
        bucket_bits: 8,
        dense: None,
        max_node_id: 1 << 34,
        max_way_id: 1 << 31,
        history: false,
    };
    let mut pos = Vec::new();
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut val = |name: &str| it.next().with_context(|| format!("{name} needs a value"));
        match arg.as_str() {
            "--bucket-bits" => a.bucket_bits = val("--bucket-bits")?.parse()?,
            "--dense-index" => a.dense = Some(PathBuf::from(val("--dense-index")?)),
            "--max-node-id" => a.max_node_id = val("--max-node-id")?.parse()?,
            "--max-way-id" => a.max_way_id = val("--max-way-id")?.parse()?,
            "--history" => a.history = true,
            _ if arg.starts_with("--") => bail!("unknown option {arg}"),
            _ => pos.push(arg),
        }
    }
    // every bucket of the four bucketed tables is an open file until the end: 4 << 12 at most
    if pos.len() != 2 || !(1..=12).contains(&a.bucket_bits) {
        bail!("usage: osm-extract <input.osm.pbf> <out-dir> [--bucket-bits 1..12 (8)] \
               [--dense-index DIR [--max-node-id N] [--max-way-id N]] [--history]");
    }
    a.input = PathBuf::from(&pos[0]);
    a.out = PathBuf::from(&pos[1]);
    Ok(a)
}

fn main() -> Result<()> {
    let args = parse_args()?;
    if args.history {
        return history::run(&args);
    }
    let out = &args.out;
    std::fs::create_dir_all(out)?;
    let t0 = Instant::now();
    let nb = 1usize << args.bucket_bits;
    let bits = args.bucket_bits;
    // node ids are < 2^34 for the foreseeable future; larger ones land in the last bucket
    let id_shift = 34 - bits;

    let wn = Buckets::new(out.join("nodes"), schema(Kind::Nodes), nb)?;
    let ww = Buckets::new(out.join("ways"), schema(Kind::Ways), nb)?;
    let wr = Buckets::new(out.join("relations"), schema(Kind::Relations), nb)?;
    let wp = Buckets::new(out.join("node_parent_ways"), parents_schema(), nb)?;
    std::fs::create_dir_all(out.join("locator"))?;
    let mut loc_writers = Vec::new();
    for t in ["n", "w", "r"] {
        let f = File::create(out.join("locator").join(format!("{t}.parquet")))?;
        loc_writers.push(ArrowWriter::try_new(f, locator_schema(), Some(props()?))?);
    }

    let (mut loc, mut ways): (Store<8>, Store<24>) = match &args.dense {
        Some(dir) => {
            std::fs::create_dir_all(dir)?;
            (Store::dense(&dir.join("node_locations.bin"), args.max_node_id)?,
             Store::dense(&dir.join("way_index.bin"), args.max_way_id)?)
        }
        None => (Store::sparse(), Store::sparse()),
    };

    let node_bucket = |b: &RecordBatch| qt_column(b).values().iter().map(|&q| (q >> (32 - bits)) as usize).collect::<Vec<_>>();
    let elem_bucket = |b: &RecordBatch| qt_column(b).values().iter().map(|&q| (key31(q) >> (33 - bits)) as usize).collect::<Vec<_>>();
    let (mut n_nodes, mut n_ways, mut n_rels) = (0u64, 0u64, 0u64);

    let mut blobs = BlobReader::from_path(&args.input).context("open pbf")?.peekable();
    while blobs.peek().is_some() {
        let chunk: Vec<_> = blobs.by_ref().take(CHUNK_BLOBS).collect::<std::result::Result<_, _>>()?;
        let blocks: Vec<PrimitiveBlock> = chunk
            .par_iter()
            .filter_map(|b| match b.decode() {
                Ok(BlobDecode::OsmData(block)) => Some(Ok(block)),
                Ok(_) => None,
                Err(e) => Some(Err(e)),
            })
            .collect::<std::result::Result<_, _>>()?;

        let node_parts: Vec<_> = blocks.par_iter().map(build_nodes).collect::<Result<_>>()?;
        let mut batches = Vec::new();
        for (batch, ids, lats, lons) in node_parts.into_iter().flatten() {
            for i in 0..ids.len() {
                loc.put(ids[i], enc_node(lats[i], lons[i]))?;
            }
            loc_writers[0].write(&locator_batch(&batch)?)?;
            n_nodes += batch.num_rows() as u64;
            batches.push(batch);
        }
        wn.write_split(&batches, node_bucket)?;

        let way_parts: Vec<_> = blocks.par_iter().map(|b| build_ways(b, &loc)).collect::<Result<_>>()?;
        let (mut batches, mut parents) = (Vec::new(), Vec::new());
        for part in way_parts.into_iter().flatten() {
            for (id, rec) in part.index {
                ways.put(id, rec)?;
            }
            loc_writers[1].write(&locator_batch(&part.batch)?)?;
            n_ways += part.batch.num_rows() as u64;
            batches.push(part.batch);
            parents.extend(part.parents);
        }
        ww.write_split(&batches, elem_bucket)?;
        if !parents.is_empty() {
            let (n, w): (Vec<i64>, Vec<i64>) = parents.into_iter().unzip();
            let pb = RecordBatch::try_new(parents_schema(), vec![Arc::new(Int64Array::from(n)), Arc::new(Int64Array::from(w))])?;
            wp.write_split(&[pb], |b| {
                let ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                ids.values().iter().map(|&i| ((i.max(0) as u64 >> id_shift) as usize).min(nb - 1)).collect()
            })?;
        }

        // Relations need every way's record; sorted PBFs put them last, so the way store is complete.
        let rel_parts: Vec<_> = blocks.par_iter().map(|b| build_relations(b, &loc, &ways)).collect::<Result<_>>()?;
        let batches: Vec<_> = rel_parts.into_iter().flatten().collect();
        for batch in &batches {
            loc_writers[2].write(&locator_batch(batch)?)?;
            n_rels += batch.num_rows() as u64;
        }
        wr.write_split(&batches, elem_bucket)?;
    }
    for w in loc_writers {
        w.close()?;
    }
    let n_parents = wp.close()?;
    wn.close()?;
    ww.close()?;
    wr.close()?;
    let mut m = File::create(out.join("manifest.json"))?;
    writeln!(
        m,
        "{{\"format\": 2, \"bucket_bits\": {bits}, \"node_id_shift\": {id_shift}, \"nodes\": {n_nodes}, \
         \"ways\": {n_ways}, \"relations\": {n_rels}, \"node_parent_ways\": {n_parents}{}}}",
        header_fields(&args.input)?
    )?;
    println!(
        "nodes={n_nodes} ways={n_ways} relations={n_rels} node_parent_ways={n_parents} {:.1}s ({} threads)",
        t0.elapsed().as_secs_f64(),
        rayon::current_num_threads()
    );
    Ok(())
}
