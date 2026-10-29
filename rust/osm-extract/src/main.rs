//! osm-extract: OSM PBF -> raw Parquet, the Rust counterpart of src/extract.py.
//!
//! Same schema as extract.py, plus Overpass spatial keys computed in the same pass:
//!   nodes.parquet      id, lat, lon, tags, version, ts, changeset, uid, user, qt
//!   ways.parquet       id, nds, nd_lat, nd_lon, tags, <meta>, qt
//!   relations.parquet  id, members, tags, <meta>, qt
//!
//! Blocks are decoded in parallel in chunks. Within a chunk, node blocks are built first and their
//! locations appended to an id-sorted location index, then way/relation blocks (which need the index)
//! are built in parallel. This relies on the PBF being sorted by type then id (checked).

mod quadtile;

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use arrow::array::{
    ArrayRef, Int32Builder, Int64Builder, ListBuilder, MapBuilder, MapFieldNames, StringBuilder,
    StructBuilder, TimestampMillisecondBuilder, UInt32Builder,
};
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use osmpbf::{BlobDecode, BlobReader, Info, PrimitiveBlock, RelMemberType};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;

const NO_COORD: i32 = i32::MIN;
const CHUNK_BLOBS: usize = 64;

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
    Arc::new(Schema::new(f))
}

#[derive(Clone, Copy, PartialEq)]
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

/// Id-sorted location index (planet scale would want a dense mmap array instead).
struct Locations {
    ids: Vec<i64>,
    lat: Vec<i32>,
    lon: Vec<i32>,
}

impl Locations {
    #[inline]
    fn get(&self, id: i64) -> Option<(i32, i32)> {
        self.ids.binary_search(&id).ok().map(|i| (self.lat[i], self.lon[i]))
    }
}

/// Way block -> record batch + (way id, way qt) for the relation pass.
fn build_ways(block: &PrimitiveBlock, loc: &Locations) -> Result<Option<(RecordBatch, Vec<(i64, u32)>)>> {
    let mut c = Common::new();
    let mut nds = ListBuilder::new(Int64Builder::new());
    let mut nd_lat = ListBuilder::new(Int32Builder::new());
    let mut nd_lon = ListBuilder::new(Int32Builder::new());
    let mut way_idx = Vec::new();
    let mut node_idxs = Vec::new();
    for group in block.groups() {
        for w in group.ways() {
            node_idxs.clear();
            for r in w.refs() {
                nds.values().append_value(r);
                match loc.get(r) {
                    Some((la, lo)) => {
                        nd_lat.values().append_value(la);
                        nd_lon.values().append_value(lo);
                        node_idxs.push(quadtile::node_index(la, lo));
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
            way_idx.push((w.id(), q));
        }
    }
    if way_idx.is_empty() {
        return Ok(None);
    }
    let cols = c.finish(vec![Arc::new(nds.finish()), Arc::new(nd_lat.finish()), Arc::new(nd_lon.finish())]);
    Ok(Some((RecordBatch::try_new(schema(Kind::Ways), cols)?, way_idx)))
}

fn build_relations(block: &PrimitiveBlock, loc: &Locations, ways: &[(i64, u32)]) -> Result<Option<RecordBatch>> {
    let mut c = Common::new();
    let mut members = ListBuilder::new(StructBuilder::from_fields(member_struct_fields(), 0));
    let mut any = false;
    let mut idxs = Vec::new();
    for group in block.groups() {
        for r in group.relations() {
            any = true;
            idxs.clear();
            let mb = members.values();
            for m in r.members() {
                let (t, idx) = match m.member_type {
                    RelMemberType::Node => ("n", loc.get(m.member_id).map(|(a, b)| quadtile::node_index(a, b))),
                    RelMemberType::Way => (
                        "w",
                        ways.binary_search_by_key(&m.member_id, |p| p.0).ok().map(|i| ways[i].1),
                    ),
                    RelMemberType::Relation => ("r", None),
                };
                if let Some(i) = idx {
                    idxs.push(i);
                }
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
        }
    }
    if !any {
        return Ok(None);
    }
    let cols = c.finish(vec![Arc::new(members.finish())]);
    Ok(Some(RecordBatch::try_new(schema(Kind::Relations), cols)?))
}

// ---- driver ---------------------------------------------------------------------

fn writer(dir: &Path, kind: Kind, name: &str) -> Result<ArrowWriter<File>> {
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_max_row_group_row_count(Some(1 << 20))
        .build();
    Ok(ArrowWriter::try_new(File::create(dir.join(name))?, schema(kind), Some(props))?)
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        bail!("usage: osm-extract <input.osm.pbf> <out-dir>");
    }
    let (input, out) = (PathBuf::from(&args[1]), PathBuf::from(&args[2]));
    std::fs::create_dir_all(&out)?;
    let t0 = Instant::now();

    let mut wn = writer(&out, Kind::Nodes, "nodes.parquet")?;
    let mut ww = writer(&out, Kind::Ways, "ways.parquet")?;
    let mut wr = writer(&out, Kind::Relations, "relations.parquet")?;
    let mut loc = Locations { ids: Vec::new(), lat: Vec::new(), lon: Vec::new() };
    let mut way_idx: Vec<(i64, u32)> = Vec::new();
    let (mut n_nodes, mut n_ways, mut n_rels) = (0usize, 0usize, 0usize);

    let mut blobs = BlobReader::from_path(&input).context("open pbf")?.peekable();
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
        for (batch, ids, lats, lons) in node_parts.into_iter().flatten() {
            if let (Some(&last), Some(&first)) = (loc.ids.last(), ids.first()) {
                if first <= last {
                    bail!("PBF not sorted by node id ({first} after {last})");
                }
            }
            n_nodes += batch.num_rows();
            wn.write(&batch)?;
            loc.ids.extend(ids);
            loc.lat.extend(lats);
            loc.lon.extend(lons);
        }

        let way_parts: Vec<_> = blocks.par_iter().map(|b| build_ways(b, &loc)).collect::<Result<_>>()?;
        for (batch, idx) in way_parts.into_iter().flatten() {
            n_ways += batch.num_rows();
            ww.write(&batch)?;
            way_idx.extend(idx);
        }

        // Relations need every way's index; sorted PBFs put them last, so the way list is complete.
        let rel_parts: Vec<_> = blocks
            .par_iter()
            .map(|b| build_relations(b, &loc, &way_idx))
            .collect::<Result<_>>()?;
        for batch in rel_parts.into_iter().flatten() {
            n_rels += batch.num_rows();
            wr.write(&batch)?;
        }
    }
    wn.close()?;
    ww.close()?;
    wr.close()?;
    println!(
        "nodes={n_nodes} ways={n_ways} relations={n_rels} {:.1}s ({} threads)",
        t0.elapsed().as_secs_f64(),
        rayon::current_num_threads()
    );
    Ok(())
}
