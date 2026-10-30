//! Output tables: Arrow schemas, the bucket layout, and the Parquet writers behind it.

use std::collections::BTreeMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use arrow::array::{MapFieldNames, UInt32Array};
use arrow::compute::take_record_batch;
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
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

pub(crate) fn props() -> Result<WriterProperties> {
    Ok(WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_max_row_group_row_count(Some(1 << 17))
        .build())
}

/// One lazily created Parquet file per bucket; buckets are written in parallel. Each open file
/// buffers its current row group in memory, so with thousands of buckets the buffers, not the data,
/// decide the memory use: past WRITER_BUDGET bytes per table the largest buffers are flushed early
/// (smaller row groups in the raw files, bounded memory at any scale).
pub(crate) struct Buckets {
    dir: PathBuf,
    schema: Arc<Schema>,
    writers: Vec<Mutex<Option<ArrowWriter<File>>>>,
    rows: Vec<Mutex<u64>>,
}

static WRITER_BUDGET: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(256 << 20);

pub(crate) fn set_writer_budget_mb(mb: usize) {
    WRITER_BUDGET.store(mb << 20, std::sync::atomic::Ordering::Relaxed);
}

impl Buckets {
    pub(crate) fn new(dir: PathBuf, schema: Arc<Schema>, n: usize) -> Result<Self> {
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
    pub(crate) fn write_split(&self, batches: &[RecordBatch], bucket_of: impl Fn(&RecordBatch) -> Vec<usize> + Sync) -> Result<()> {
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
        pieces.into_par_iter().try_for_each(|(b, ps)| ps.iter().try_for_each(|p| self.write(b, p)))?;
        self.limit_memory()
    }

    fn limit_memory(&self) -> Result<()> {
        let budget = WRITER_BUDGET.load(std::sync::atomic::Ordering::Relaxed);
        let mut sizes: Vec<(usize, usize)> = Vec::new();
        let mut total = 0;
        for (i, w) in self.writers.iter().enumerate() {
            if let Some(w) = w.lock().unwrap().as_ref() {
                let m = w.memory_size();
                total += m;
                if m > 0 {
                    sizes.push((m, i));
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
            self.writers[i].lock().unwrap().as_mut().unwrap().flush()?;
            total -= m;
        }
        Ok(())
    }

    /// Close all files; a table with no rows still gets one empty file so its schema is known.
    pub(crate) fn close(self) -> Result<u64> {
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


fn key31(v: u32) -> u64 {
    (((v & 0x7FFF_FFFF) as u64) << 1) | (v >> 31) as u64
}

pub(crate) fn locator_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false), Field::new("qt", DataType::UInt32, false)]))
}

pub(crate) fn locator_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(locator_schema(), vec![batch.column(0).clone(), batch.column_by_name("qt").unwrap().clone()])?)
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
