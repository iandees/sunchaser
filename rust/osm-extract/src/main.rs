//! osm-extract: OSM PBF -> bucketed raw Parquet for the lake build (src/build_lake.py).
//!
//! The input is a full-history PBF (a snapshot PBF works too: every element has one version).
//! Everything that needs cross-element lookups is computed here, in one streaming pass, so the lake
//! build only has to sort:
//!   nodes/NNNN.parquet             id, lat, lon, tags, <meta>, qt
//!   ways/NNNN.parquet              id, nds, nd_lat, nd_lon, tags, <meta>, qt, min/max lat/lon
//!   relations/NNNN.parquet         id, members, tags, <meta>, qt, min/max lat/lon (node and way members)
//!   locator/{n,w,r}.parquet        id, qt in id order (the PBF's order: no sort needed)
//!   node_parent_ways/NNNN.parquet  node_id, way_id (distinct per way)
//!   manifest.json
//! The output also has nodes_history/, ways_history/, relations_history/ and changes/ (see
//! history.rs); --no-history skips those and writes "history": false in the manifest.
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

// glibc's per-thread arenas fragment badly under parallel decoding of large blocks (RSS grew
// without bound on the planet); mimalloc keeps it flat.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod cut;
mod history;
mod quadtile;

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use arrow::array::{MapFieldNames, UInt32Array};
use arrow::compute::take_record_batch;
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use osmpbf::BlobReader;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;

const NO_COORD: i32 = i32::MIN;
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

fn extend_bbox(b: &mut [i32; 4], la_min: i32, la_max: i32, lo_min: i32, lo_max: i32) {
    b[0] = b[0].min(la_min);
    b[1] = b[1].max(la_max);
    b[2] = b[2].min(lo_min);
    b[3] = b[3].max(lo_max);
}


// ---- output ---------------------------------------------------------------------

fn props() -> Result<WriterProperties> {
    Ok(WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_max_row_group_row_count(Some(1 << 17))
        .build())
}

/// One lazily created Parquet file per bucket; buckets are written in parallel. Each open file
/// buffers its current row group in memory, so with thousands of buckets the buffers, not the data,
/// decide the memory use: past WRITER_BUDGET bytes per table the largest buffers are flushed early
/// (smaller row groups in the raw files, bounded memory at any scale).
struct Buckets {
    dir: PathBuf,
    schema: Arc<Schema>,
    writers: Vec<Mutex<Option<ArrowWriter<File>>>>,
    rows: Vec<Mutex<u64>>,
}

static WRITER_BUDGET: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(256 << 20);

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


fn key31(v: u32) -> u64 {
    (((v & 0x7FFF_FFFF) as u64) << 1) | (v >> 31) as u64
}

fn locator_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false), Field::new("qt", DataType::UInt32, false)]))
}

pub(crate) fn locator_batch(batch: &RecordBatch) -> Result<RecordBatch> {
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
/// lake (and the updater) start where the file ends. Captured while the input streams past.
static HEADER: std::sync::OnceLock<String> = std::sync::OnceLock::new();

fn note_header(h: &osmpbf::HeaderBlock) {
    HEADER.get_or_init(|| {
        let mut fields = String::new();
        if let Some(ts) = h.osmosis_replication_timestamp() {
            fields += &format!(", \"osm_base\": \"{}\"", iso8601(ts));
        }
        if let Some(seq) = h.osmosis_replication_sequence_number() {
            fields += &format!(", \"sequence\": {seq}");
        }
        fields
    });
}

fn header_fields() -> String {
    HEADER.get().cloned().unwrap_or_default()
}

/// Bytes read from the input so far, and its total size if known (a file's, or --input-size for a
/// pipe), for the progress lines.
static INPUT_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static INPUT_TOTAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

struct Counting<R>(R);

impl<R: Read> Read for Counting<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.0.read(buf)?;
        INPUT_BYTES.fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(n)
    }
}

/// Resident and peak memory of this process, as " | rss 12.3 GB (peak 14.0, anon 9.1)" (Linux only);
/// anon is the part that is not file-backed, so the memory-mapped dense index is not in it.
fn memory_note() -> String {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else { return String::new() };
    let kb = |key: &str| {
        status.lines().find(|l| l.starts_with(key)).and_then(|l| l.split_whitespace().nth(1)).and_then(|v| v.parse::<f64>().ok())
    };
    match (kb("VmRSS:"), kb("VmHWM:")) {
        (Some(r), Some(h)) => {
            let anon = kb("RssAnon:").map_or(String::new(), |a| format!(", anon {:.1}", a / 1e6));
            format!(" | rss {:.1} GB (peak {:.1}{anon})", r / 1e6, h / 1e6)
        }
        _ => String::new(),
    }
}

/// One progress line on stderr every 30 s: input read, share and ETA when the size is known, rate,
/// memory, and whatever counts the caller supplies. The long runs (a planet is hours) are otherwise silent.
pub(crate) struct Progress {
    t0: Instant,
    last: Instant,
}

impl Progress {
    pub(crate) fn new() -> Self {
        Progress { t0: Instant::now(), last: Instant::now() }
    }

    pub(crate) fn tick(&mut self, counts: impl FnOnce() -> String) {
        if self.last.elapsed().as_secs() < 30 {
            return;
        }
        self.last = Instant::now();
        let read = INPUT_BYTES.load(std::sync::atomic::Ordering::Relaxed) as f64;
        let total = INPUT_TOTAL.load(std::sync::atomic::Ordering::Relaxed) as f64;
        let secs = self.t0.elapsed().as_secs_f64();
        let share = if total > 0.0 && read > 0.0 {
            format!(" ({:.1}%, eta {:.0} min)", 100.0 * read / total, secs * (total - read).max(0.0) / read / 60.0)
        } else {
            String::new()
        };
        eprintln!("[{secs:>6.0}s] read {:.1} GB{share} at {:.0} MB/s{} | {}", read / 1e9, read / 1e6 / secs, memory_note(), counts());
    }
}

/// The PBF from a file, or from stdin for `-` (e.g. piped from a download, so the file never has
/// to be stored: the planet history is 164 GB).
fn open_input(path: &Path) -> Result<BlobReader<Box<dyn Read + Send>>> {
    let r: Box<dyn Read + Send> = if path.as_os_str() == "-" {
        Box::new(BufReader::with_capacity(16 << 20, Counting(std::io::stdin())))
    } else {
        let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
        if INPUT_TOTAL.load(std::sync::atomic::Ordering::Relaxed) == 0 {
            INPUT_TOTAL.store(f.metadata()?.len(), std::sync::atomic::Ordering::Relaxed);
        }
        Box::new(BufReader::with_capacity(16 << 20, Counting(f)))
    };
    Ok(BlobReader::new(r))
}

struct Args {
    input: PathBuf,
    out: PathBuf,
    bucket_bits: u32,
    dense: Option<PathBuf>,
    max_node_id: u64,
    max_way_id: u64,
    no_history: bool,
    cuts: Vec<String>,
}

fn parse_args() -> Result<Args> {
    let mut a = Args {
        input: PathBuf::new(),
        out: PathBuf::new(),
        bucket_bits: 8,
        dense: None,
        max_node_id: 1 << 34,
        max_way_id: 1 << 31,
        no_history: false,
        cuts: Vec::new(),
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
            "--history" => {} // accepted for compatibility: history is the only pipeline
            "--no-history" => a.no_history = true,
            "--writer-mem-mb" => WRITER_BUDGET.store(val("--writer-mem-mb")?.parse::<usize>()? << 20, std::sync::atomic::Ordering::Relaxed),
            "--input-size" => INPUT_TOTAL.store(val("--input-size")?.parse()?, std::sync::atomic::Ordering::Relaxed),
            "--cut" => a.cuts.push(val("--cut")?),
            _ if arg.starts_with("--") => bail!("unknown option {arg}"),
            _ => pos.push(arg),
        }
    }
    // every bucket of the four bucketed tables is an open file until the end: 4 << 12 at most
    if pos.len() != 2 || !(1..=12).contains(&a.bucket_bits) {
        bail!("usage: osm-extract <input.osm.pbf | - for stdin> <out-dir> [--bucket-bits 1..12 (8)] \
               [--dense-index DIR [--max-node-id N] [--max-way-id N]] [--no-history] [--cut name:s,w,n,e ...] [--writer-mem-mb N (256, per table)] [--input-size BYTES (for % and ETA on stdin)]");
    }
    a.input = PathBuf::from(&pos[0]);
    a.out = PathBuf::from(&pos[1]);
    Ok(a)
}

fn main() -> Result<()> {
    let args = parse_args()?;
    if !args.cuts.is_empty() {
        return cut::run(&args);
    }
    history::run(&args)
}
