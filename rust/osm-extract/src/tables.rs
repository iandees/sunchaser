//! Output tables: Arrow schemas, the bucket layout, and the Parquet writers behind it.

use std::collections::BTreeMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use arrow::array::MapFieldNames;
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::{ArrowColumnChunk, ArrowLeafColumn, ArrowRowGroupWriterFactory, compute_leaves};
use parquet::arrow::{ArrowSchemaConverter, add_encoded_arrow_schema_to_metadata};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use rayon::prelude::*;

pub(crate) const NO_COORD: i32 = i32::MIN;
pub(crate) const MISSING_BBOX: [i32; 4] = [i32::MAX, i32::MIN, i32::MAX, i32::MIN];

// ---- schema -------------------------------------------------------------------

pub(crate) fn map_names() -> MapFieldNames {
    MapFieldNames { entry: "entries".into(), key: "key".into(), value: "value".into() }
}

pub(crate) fn tags_field() -> Field {
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

pub(crate) fn meta_fields() -> Vec<Field> {
    vec![
        Field::new("version", DataType::Int32, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())), true),
        Field::new("changeset", DataType::Int64, true),
        Field::new("uid", DataType::Int32, true),
        Field::new("user", DataType::Utf8, true),
    ]
}

pub(crate) fn list_of(name: &str, dt: DataType) -> Field {
    Field::new(name, DataType::List(Arc::new(Field::new("item", dt, true))), true)
}

pub(crate) fn member_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("type", DataType::Utf8, true),
        Field::new("ref", DataType::Int64, true),
        Field::new("role", DataType::Utf8, true),
    ])
}

/// Column order of a current table (`history: false`: ends with valid_from) or a history table
/// (also valid_to). Nodes have no bbox columns.
pub(crate) fn schema(kind: Kind, history: bool) -> Arc<Schema> {
    let ts = || DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
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
    f.push(Field::new("valid_from", ts(), true));
    if history {
        f.push(Field::new("valid_to", ts(), true));
    }
    Arc::new(Schema::new(f))
}

/// Element kind, in PBF order (nodes, then ways, then relations).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(crate) enum Kind {
    Nodes,
    Ways,
    Relations,
}

impl Kind {
    pub(crate) const ALL: [Kind; 3] = [Kind::Nodes, Kind::Ways, Kind::Relations];

    pub(crate) fn index(self) -> usize {
        self as usize
    }

    /// The single letter used in the `type` columns ("n", "w", "r").
    pub(crate) fn letter(self) -> &'static str {
        ["n", "w", "r"][self as usize]
    }

    /// The following kind (Relations stays).
    pub(crate) fn next(self) -> Kind {
        Kind::ALL[(self.index() + 1).min(2)]
    }
}

pub(crate) fn extend_bbox(b: &mut [i32; 4], la_min: i32, la_max: i32, lo_min: i32, lo_max: i32) {
    b[0] = b[0].min(la_min);
    b[1] = b[1].max(la_max);
    b[2] = b[2].min(lo_min);
    b[3] = b[3].max(lo_max);
}


// ---- output ---------------------------------------------------------------------

/// No dictionary encoding: on ids, coordinates and times it hashed every value into a dictionary
/// as large as the column and then fell back to plain, on the one thread that writes each bucket
/// (about 10% of the planet extract's CPU), and zstd compresses plain pages better anyway
/// (Minnesota: 2.6 GB -> 1.7 GB, the strings included).
pub(crate) fn props() -> Result<WriterProperties> {
    Ok(WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_max_row_group_row_count(Some(1 << 17))
        .set_dictionary_enabled(false)
        .build())
}

const ROW_GROUP_ROWS: usize = 1 << 17;

/// One Parquet file of a bucket: the rows of its next row group wait in `pending` until there are
/// ROW_GROUP_ROWS of them (or the memory budget says now), then the row group's columns are
/// encoded in parallel and appended. A chunk's rows fall into few buckets (consecutive ids are
/// neighbours), so parallelism across buckets alone left one thread encoding the busiest bucket
/// while the others waited for it.
struct BucketFile {
    w: SerializedFileWriter<File>,
    pending: Vec<RecordBatch>,
    rows: usize,
    bytes: usize,
}

impl BucketFile {
    /// Encode the first `n` pending rows (all of them past the end) as one row group.
    fn flush(&mut self, schema: &Arc<Schema>, n: usize) -> Result<()> {
        let (mut take, mut got) = (Vec::new(), 0);
        while got < n && !self.pending.is_empty() {
            let b = self.pending.remove(0);
            if got + b.num_rows() > n {
                take.push(b.slice(0, n - got));
                self.pending.insert(0, b.slice(n - got, b.num_rows() - (n - got)));
                got = n;
            } else {
                got += b.num_rows();
                take.push(b);
            }
        }
        if got == 0 {
            return Ok(());
        }
        self.rows -= got;
        self.bytes = self.pending.iter().map(|b| b.get_array_memory_size()).sum();
        let factory = ArrowRowGroupWriterFactory::new(&self.w, schema.clone());
        let writers = factory.create_column_writers(self.w.flushed_row_groups().len())?;
        let mut leaves: Vec<Vec<ArrowLeafColumn>> = (0..writers.len()).map(|_| Vec::new()).collect();
        for b in &take {
            let mut j = 0;
            for (field, col) in schema.fields().iter().zip(b.columns()) {
                for leaf in compute_leaves(field, col)? {
                    leaves[j].push(leaf);
                    j += 1;
                }
            }
        }
        let chunks: Vec<ArrowColumnChunk> = writers
            .into_par_iter()
            .zip(leaves)
            .map(|(mut w, ls)| -> Result<ArrowColumnChunk> {
                for l in &ls {
                    w.write(l)?;
                }
                Ok(w.close()?)
            })
            .collect::<Result<_>>()?;
        let mut rg = self.w.next_row_group()?;
        for c in chunks {
            c.append_to_row_group(&mut rg)?;
        }
        rg.close()?;
        Ok(())
    }
}

/// One lazily created Parquet file per bucket; buckets are written in parallel. Each open file
/// buffers its next row group in memory, so with thousands of buckets the buffers, not the data,
/// decide the memory use: past WRITER_BUDGET bytes per table the largest buffers are flushed early
/// (smaller row groups in the raw files, bounded memory at any scale).
pub(crate) struct Buckets {
    dir: PathBuf,
    schema: Arc<Schema>,
    props: Arc<WriterProperties>,
    files: Vec<Mutex<Option<BucketFile>>>,
    rows: Vec<Mutex<u64>>,
}

static WRITER_BUDGET: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(256 << 20);

pub(crate) fn set_writer_budget_mb(mb: usize) {
    WRITER_BUDGET.store(mb << 20, std::sync::atomic::Ordering::Relaxed);
}

impl Buckets {
    pub(crate) fn new(dir: PathBuf, schema: Arc<Schema>, n: usize) -> Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let mut props = props()?;
        add_encoded_arrow_schema_to_metadata(&schema, &mut props); // as ArrowWriter would
        Ok(Buckets {
            dir,
            schema,
            props: Arc::new(props),
            files: (0..n).map(|_| Mutex::new(None)).collect(),
            rows: (0..n).map(|_| Mutex::new(0)).collect(),
        })
    }

    fn open(&self, name: &str) -> Result<SerializedFileWriter<File>> {
        let f = File::create(self.dir.join(name))?;
        let desc = ArrowSchemaConverter::new().with_coerce_types(self.props.coerce_types()).convert(&self.schema)?;
        Ok(SerializedFileWriter::new(f, desc.root_schema_ptr(), self.props.clone())?)
    }

    fn write(&self, bucket: usize, batch: RecordBatch) -> Result<()> {
        let mut f = self.files[bucket].lock().unwrap();
        if f.is_none() {
            *f = Some(BucketFile { w: self.open(&format!("{bucket:04}.parquet"))?, pending: Vec::new(), rows: 0, bytes: 0 });
        }
        let f = f.as_mut().unwrap();
        *self.rows[bucket].lock().unwrap() += batch.num_rows() as u64;
        f.rows += batch.num_rows();
        f.bytes += batch.get_array_memory_size();
        f.pending.push(batch);
        while f.rows >= ROW_GROUP_ROWS {
            f.flush(&self.schema, ROW_GROUP_ROWS)?;
        }
        Ok(())
    }

    /// Group `rows` by `keys[i]` (each group keeps the rows' order), then build every bucket's
    /// batch with `build` and write it, in parallel across buckets. A bucket is touched by one
    /// thread only; the memory budget is enforced afterwards.
    pub(crate) fn write_rows<T: Sync>(
        &self,
        keys: &[usize],
        rows: &[&T],
        build: impl Fn(&[&T]) -> Result<RecordBatch> + Sync,
    ) -> Result<()> {
        let mut by: BTreeMap<usize, Vec<&T>> = BTreeMap::new();
        for (&k, &r) in keys.iter().zip(rows) {
            by.entry(k).or_default().push(r);
        }
        let groups: Vec<(usize, Vec<&T>)> = by.into_iter().collect();
        groups.into_par_iter().try_for_each(|(b, rs)| -> Result<()> {
            // a busy bucket's batch is built in slices, in parallel too
            let batches: Vec<RecordBatch> = rs.par_chunks(1 << 15).map(|c| build(c)).collect::<Result<_>>()?;
            for batch in batches {
                self.write(b, batch)?;
            }
            Ok(())
        })?;
        self.limit_memory()
    }

    fn limit_memory(&self) -> Result<()> {
        let budget = WRITER_BUDGET.load(std::sync::atomic::Ordering::Relaxed);
        let mut sizes: Vec<(usize, usize)> = Vec::new();
        let mut total = 0;
        for (i, f) in self.files.iter().enumerate() {
            if let Some(f) = f.lock().unwrap().as_ref() {
                total += f.bytes;
                if f.bytes > 0 {
                    sizes.push((f.bytes, i));
                }
            }
        }
        if total <= budget {
            return Ok(());
        }
        sizes.sort_unstable_by(|a, b| b.cmp(a));
        for (m, i) in sizes {
            if total <= budget / 2 {
                break;
            }
            self.files[i].lock().unwrap().as_mut().unwrap().flush(&self.schema, usize::MAX)?;
            total -= m;
        }
        Ok(())
    }

    /// Close all files; a table with no rows still gets one empty file so its schema is known.
    pub(crate) fn close(self) -> Result<u64> {
        let total: u64 = self.rows.iter().map(|r| *r.lock().unwrap()).sum();
        if total == 0 {
            self.open("0000.parquet")?.close()?;
        }
        // every open bucket writes its last row group here: all at once
        let schema = self.schema.clone();
        self.files.into_par_iter().try_for_each(|f| -> Result<()> {
            if let Some(mut f) = f.into_inner().unwrap() {
                f.flush(&schema, usize::MAX)?;
                f.w.close()?;
            }
            Ok(())
        })?;
        Ok(total)
    }
}


fn key31(v: u32) -> u64 {
    (((v & 0x7FFF_FFFF) as u64) << 1) | (v >> 31) as u64
}

pub(crate) fn locator_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false), Field::new("qt", DataType::UInt32, false)]))
}

pub(crate) fn parents_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("node_id", DataType::Int64, false), Field::new("way_id", DataType::Int64, false)]))
}

/// How rows are spread over the `1 << bits` bucket files of each table. Buckets are disjoint key
/// ranges in key order, so each can be sorted on its own and appended.
#[derive(Clone, Copy)]
pub(crate) struct Layout {
    pub(crate) bits: u32,
    pub(crate) count: usize,
    /// node ids are < 2^34 for the foreseeable future; larger ones land in the last bucket
    pub(crate) id_shift: u32,
}

impl Layout {
    pub(crate) fn new(bits: u32) -> Self {
        Layout { bits, count: 1 << bits, id_shift: 34 - bits }
    }

    /// Bucket of a row's qt: nodes by the top bits of qt, ways and relations by the Overpass
    /// Uint31 order key31(qt).
    pub(crate) fn bucket(&self, kind: Kind, qt: u32) -> usize {
        match kind {
            Kind::Nodes => (qt >> (32 - self.bits)) as usize,
            _ => (key31(qt) >> (33 - self.bits)) as usize,
        }
    }

    /// Bucket of node_parent_ways rows: by node id.
    pub(crate) fn parent_bucket(&self, node_id: i64) -> usize {
        ((node_id.max(0) as u64 >> self.id_shift) as usize).min(self.count - 1)
    }
}
