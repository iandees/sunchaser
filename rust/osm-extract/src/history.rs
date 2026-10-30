//! Full-history PBF (every version, deletions as visible=false) -> current tables,
//! *_history tables and the changes log, as if the updater had applied every change since the
//! first version. Output (buckets as in the current tables, plus):
//!   nodes_history/, ways_history/, relations_history/   rows with valid_from/valid_to and meta
//!   changes/                                            ts, type, id, old/new version, action
//!
//! Semantics, matching src/updater.py (validated against Overpass's attic):
//! - Each version is valid from its timestamp to the next version's; versions sharing a second
//!   collapse to the last one (the updater sees one state per diff). Deletions end validity.
//! - A way gets a new row whenever a member node's position changes within a version (`geometry`
//!   change): [date:] needs its coordinates at that time.
//! - A relation's rows change only when its members, tags, key or bbox change; every member move
//!   (node position, member way coordinates) is still a `geometry` entry in the changes log.
//! Timelines (node positions, way keys/bboxes over time) are kept in RAM or, with --dense-index,
//! in files indexed by id.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Result, bail};
use arrow::array::{
    ArrayRef, Int32Builder, Int64Array, Int64Builder, ListBuilder, MapBuilder, StringBuilder, StructBuilder,
    TimestampMillisecondBuilder, UInt32Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use memmap2::{Mmap, MmapMut};
use osmpbf::{BlobDecode, PrimitiveBlock, RelMemberType};
use parquet::arrow::ArrowWriter;
use rayon::prelude::*;

use crate::Args;
use crate::quadtile;
use crate::relstate::{Contrib, RelState};
use crate::tables::{
    Buckets, Kind, Layout, MISSING_BBOX, NO_COORD, extend_bbox, locator_batch, locator_schema, map_names,
    member_struct_fields, parents_schema, props, schema,
};

const CHUNK_BLOBS: usize = 64;

// ---- decoded versions -------------------------------------------------------------

#[derive(Clone)]
struct Meta {
    version: i32,
    secs: i64,
    changeset: i64,
    uid: i32,
    user: String,
    visible: bool,
}

#[derive(Clone)]
enum Body {
    Node(i32, i32),
    Way(Vec<i64>),
    Rel(Vec<(Kind, i64, String)>), // (member kind, ref, role)
}

#[derive(Clone)]
struct Version {
    kind: Kind,
    id: i64,
    meta: Meta,
    tags: Vec<(String, String)>,
    body: Body,
}

fn tags_of<'a>(it: impl Iterator<Item = (&'a str, &'a str)>) -> Vec<(String, String)> {
    it.map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

fn dense_version(n: &osmpbf::DenseNode) -> Version {
    let meta = match n.info() {
        Some(i) => Meta {
            version: i.version(),
            secs: i.milli_timestamp().div_euclid(1000),
            changeset: i.changeset(),
            uid: i.uid(),
            user: i.user().unwrap_or("").to_string(),
            visible: i.visible(),
        },
        None => Meta { version: 0, secs: 0, changeset: 0, uid: 0, user: String::new(), visible: true },
    };
    Version { kind: Kind::Nodes, id: n.id(), tags: tags_of(n.tags()), body: Body::Node(n.decimicro_lat(), n.decimicro_lon()), meta }
}

fn node_version(n: &osmpbf::Node) -> Version {
    Version {
        kind: Kind::Nodes,
        id: n.id(),
        meta: info_meta(&n.info()),
        tags: tags_of(n.tags()),
        body: Body::Node(n.decimicro_lat(), n.decimicro_lon()),
    }
}

fn way_version(w: &osmpbf::Way) -> Version {
    Version { kind: Kind::Ways, id: w.id(), meta: info_meta(&w.info()), tags: tags_of(w.tags()), body: Body::Way(w.refs().collect()) }
}

fn rel_version(r: &osmpbf::Relation) -> Version {
    let members = r
        .members()
        .map(|m| {
            let t = match m.member_type {
                RelMemberType::Node => Kind::Nodes,
                RelMemberType::Way => Kind::Ways,
                RelMemberType::Relation => Kind::Relations,
            };
            (t, m.member_id, m.role().unwrap_or("").to_string())
        })
        .collect();
    Version { kind: Kind::Relations, id: r.id(), meta: info_meta(&r.info()), tags: tags_of(r.tags()), body: Body::Rel(members) }
}

fn decode(block: &PrimitiveBlock) -> Vec<Version> {
    let mut out = Vec::new();
    for g in block.groups() {
        out.extend(g.dense_nodes().map(|n| dense_version(&n)));
        out.extend(g.nodes().map(|n| node_version(&n)));
        out.extend(g.ways().map(|w| way_version(&w)));
        out.extend(g.relations().map(|r| rel_version(&r)));
    }
    out
}

fn info_meta(i: &osmpbf::Info) -> Meta {
    Meta {
        version: i.version().unwrap_or(0),
        secs: i.milli_timestamp().unwrap_or(0).div_euclid(1000),
        changeset: i.changeset().unwrap_or(0),
        uid: i.uid().unwrap_or(0),
        user: i.user().and_then(|u| u.ok()).unwrap_or("").to_string(),
        visible: i.visible(),
    }
}

/// Versions sorted by version; those sharing a timestamp collapse to the last one.
fn collapse(mut vs: Vec<Version>) -> Vec<Version> {
    vs.sort_by_key(|v| v.meta.version);
    let mut out: Vec<Version> = Vec::with_capacity(vs.len());
    for v in vs {
        if let Some(last) = out.last() {
            if last.meta.secs >= v.meta.secs {
                out.pop(); // same second (or a clock going backwards): the later version wins
            }
        }
        out.push(v);
    }
    out
}

// ---- timelines: id -> time-ordered fixed-size records ------------------------------

/// Per-id lists of N-byte records (the first 8 bytes are the record's time), appended in id order
/// during one phase and read randomly in the next. In RAM, or on disk: a dense array of one 8-byte
/// entry per id (40-bit start | 24-bit count; 0 = none), so absent ids cost nothing (sparse file),
/// plus the records file.
enum Timeline<const N: usize> {
    Ram { ids: Vec<i64>, offs: Vec<u64>, recs: Vec<[u8; N]> },
    Disk { idx: MmapMut, cap: u64, last: i64, n: u64, w: Option<BufWriter<File>>, path: PathBuf, recs: Option<Mmap> },
}

impl<const N: usize> Timeline<N> {
    fn ram() -> Self {
        Timeline::Ram { ids: Vec::new(), offs: vec![0], recs: Vec::new() }
    }

    fn disk(dir: &Path, name: &str, max_id: u64) -> Result<Self> {
        let f = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(dir.join(format!("{name}.idx")))?;
        f.set_len((max_id + 1) * 8)?;
        let path = dir.join(format!("{name}.recs"));
        let idx = unsafe { MmapMut::map_mut(&f)? };
        // The index is read at random (a sorted batch still touches pages far apart on a planet), so
        // the kernel must not read ahead around each page fault: with the md RAID0 default of 2 MB it
        // read 1.1 GB/s from the NVMe array to serve lookups that needed 4 KB each, and the planet's
        // ways phase ran at about 2,500 ways/s. MADV_RANDOM turns readahead off for these mappings.
        idx.advise(memmap2::Advice::Random)?;
        Ok(Timeline::Disk {
            idx,
            cap: max_id + 1,
            last: -1,
            n: 0,
            w: Some(BufWriter::with_capacity(1 << 22, File::create(&path)?)),
            path,
            recs: None,
        })
    }

    /// Records of one id (ids strictly increasing).
    fn push(&mut self, id: i64, recs: &[[u8; N]]) -> Result<()> {
        if recs.is_empty() {
            return Ok(());
        }
        match self {
            Timeline::Ram { ids, offs, recs: all } => {
                ids.push(id);
                all.extend_from_slice(recs);
                offs.push(all.len() as u64);
            }
            Timeline::Disk { idx, cap, last, n, w, .. } => {
                if id < 0 || id as u64 >= *cap {
                    bail!("id {id} beyond the dense index (raise --max-node-id / --max-way-id)");
                }
                if id <= *last {
                    bail!("PBF not sorted by id ({id} after {last})");
                }
                if recs.len() >= 1 << 24 || *n >= 1 << 40 {
                    bail!("timeline too large for its index entry");
                }
                for r in recs {
                    w.as_mut().unwrap().write_all(r)?;
                }
                let entry = (*n << 24) | recs.len() as u64;
                let o = id as usize * 8;
                idx[o..o + 8].copy_from_slice(&entry.to_le_bytes());
                *n += recs.len() as u64;
                *last = id;
            }
        }
        Ok(())
    }

    /// End of the writing phase (Disk: map the records file).
    fn finish(&mut self) -> Result<()> {
        if let Timeline::Disk { n, w, path, recs, .. } = self {
            if let Some(mut wr) = w.take() {
                wr.flush()?;
                if *n > 0 {
                    let m = unsafe { Mmap::map(&File::open(&*path)?)? };
                    m.advise(memmap2::Advice::Random)?; // see `disk`
                    *recs = Some(m);
                }
            }
        }
        Ok(())
    }

    fn get(&self, id: i64) -> &[[u8; N]] {
        match self {
            Timeline::Ram { ids, offs, recs } => match ids.binary_search(&id) {
                Ok(i) => &recs[offs[i] as usize..offs[i + 1] as usize],
                Err(_) => &[],
            },
            Timeline::Disk { idx, cap, recs, .. } => {
                if id < 0 || id as u64 >= *cap {
                    return &[];
                }
                let o = id as usize * 8;
                let e = u64::from_le_bytes(idx[o..o + 8].try_into().unwrap());
                let (a, len) = ((e >> 24) as usize, (e & 0xFF_FFFF) as usize);
                if len == 0 {
                    return &[];
                }
                let Some(recs) = recs.as_ref() else { return &[] };
                let bytes = &recs[a * N..(a + len) * N];
                // SAFETY: [u8; N] has alignment 1, and the slice length is a multiple of N
                unsafe { std::slice::from_raw_parts(bytes.as_ptr() as *const [u8; N], len) }
            }
        }
    }

    fn is_disk(&self) -> bool {
        matches!(self, Timeline::Disk { .. })
    }

    /// The timelines of `ids` as a small in-RAM timeline. The ids are read in sorted order by many
    /// threads: a dense index that does not fit in the page cache is otherwise read at random, one
    /// page fault per lookup, and each page is now touched once per batch instead. Measured on
    /// Germany in a 30 GB cgroup (index 102 GB): the ways phase went 3x faster, still far below the
    /// cached rate, so the planet extract is expected to be bound by this.
    fn gather(&self, mut ids: Vec<i64>) -> Timeline<N> {
        ids.par_sort_unstable();
        ids.dedup();
        let parts: Vec<(Vec<i64>, Vec<u64>, Vec<[u8; N]>)> = io_pool().install(|| {
            ids.par_chunks(1024)
                .map(|c| {
                    let (mut found, mut offs, mut recs) = (Vec::new(), vec![0u64], Vec::new());
                    for &id in c {
                        let r = self.get(id);
                        if !r.is_empty() {
                            found.push(id);
                            recs.extend_from_slice(r);
                            offs.push(recs.len() as u64);
                        }
                    }
                    (found, offs, recs)
                })
                .collect()
        });
        let (mut all_ids, mut all_offs, mut all_recs) = (Vec::new(), vec![0u64], Vec::new());
        for (found, offs, recs) in parts {
            let base = all_recs.len() as u64;
            all_ids.extend(found);
            all_offs.extend(offs[1..].iter().map(|o| o + base));
            all_recs.extend(recs);
        }
        Timeline::Ram { ids: all_ids, offs: all_offs, recs: all_recs }
    }
}

/// Many more threads than cores for `gather`: they mostly wait on page faults, and a deep queue is
/// what an NVMe drive needs.
fn io_pool() -> &'static rayon::ThreadPool {
    static POOL: std::sync::OnceLock<rayon::ThreadPool> = std::sync::OnceLock::new();
    POOL.get_or_init(|| rayon::ThreadPoolBuilder::new().num_threads(64).build().expect("io thread pool"))
}

fn rec_time<const N: usize>(r: &[u8; N]) -> i64 {
    i64::from_le_bytes(r[..8].try_into().unwrap())
}

/// The record in force at time t: the last one with time <= t.
fn at<const N: usize>(recs: &[[u8; N]], t: i64) -> Option<&[u8; N]> {
    let i = recs.partition_point(|r| rec_time(r) <= t);
    if i == 0 { None } else { Some(&recs[i - 1]) }
}

/// Node position record: time, lat, lon (NO_COORD: deleted).
fn npos(t: i64, lat: i32, lon: i32) -> [u8; 16] {
    let mut r = [0u8; 16];
    r[..8].copy_from_slice(&t.to_le_bytes());
    r[8..12].copy_from_slice(&lat.to_le_bytes());
    r[12..].copy_from_slice(&lon.to_le_bytes());
    r
}

fn npos_dec(r: &[u8; 16]) -> Option<(i32, i32)> {
    let la = i32::from_le_bytes(r[8..12].try_into().unwrap());
    if la == NO_COORD { None } else { Some((la, i32::from_le_bytes(r[12..].try_into().unwrap()))) }
}

/// Way geometry record: time, exists, qt, bbox.
fn wrec(t: i64, exists: bool, qt: u32, b: [i32; 4]) -> [u8; 32] {
    let mut r = [0u8; 32];
    r[..8].copy_from_slice(&t.to_le_bytes());
    r[8] = exists as u8;
    r[12..16].copy_from_slice(&qt.to_le_bytes());
    for (i, x) in b.iter().enumerate() {
        r[16 + 4 * i..20 + 4 * i].copy_from_slice(&x.to_le_bytes());
    }
    r
}

fn wrec_dec(r: &[u8; 32]) -> Option<(u32, [i32; 4])> {
    if r[8] == 0 {
        return None;
    }
    let mut b = [0i32; 4];
    for (i, x) in b.iter_mut().enumerate() {
        *x = i32::from_le_bytes(r[16 + 4 * i..20 + 4 * i].try_into().unwrap());
    }
    Some((u32::from_le_bytes(r[12..16].try_into().unwrap()), b))
}

// ---- rows -------------------------------------------------------------------------

struct Row {
    v: Version,
    from: i64,
    to: Option<i64>, // None: current
    qt: u32,
    bbox: [i32; 4],
    coords: Vec<(i32, i32)>, // ways
}

#[derive(Clone)]
struct Change {
    ts: i64,
    kind: Kind,
    id: i64,
    old: Option<i32>,
    new: Option<i32>,
    action: &'static str,
}

/// One element's position/geometry timeline, to be pushed into the matching `Timeline`.
enum Track {
    Node(Vec<[u8; 16]>),
    Way(Vec<[u8; 32]>),
}

#[derive(Default)]
struct Out {
    rows: Vec<Row>,
    changes: Vec<Change>,
    timeline: Option<(i64, Track)>,
    parents: Vec<(i64, i64)>,
}

fn version_change(out: &mut Out, v: &Version, prev: Option<i32>) {
    let action = if prev.is_some() { "modify" } else { "create" };
    out.changes.push(Change { ts: v.meta.secs, kind: v.kind, id: v.id, old: prev, new: Some(v.meta.version), action });
}

fn node_group(vs: Vec<Version>) -> Out {
    let vs = collapse(vs);
    let mut out = Out::default();
    let mut prev: Option<i32> = None;
    let mut prev_pos: Option<(i32, i32)> = None;
    let mut tl = Vec::new();
    for (k, v) in vs.iter().enumerate() {
        let t = v.meta.secs;
        let next = vs.get(k + 1).map(|n| n.meta.secs);
        let pos = if v.meta.visible {
            let Body::Node(la, lo) = v.body else { unreachable!() };
            version_change(&mut out, v, prev);
            out.rows.push(Row { v: v.clone(), from: t, to: next, qt: quadtile::node_index(la, lo), bbox: [la, la, lo, lo], coords: vec![] });
            prev = Some(v.meta.version);
            Some((la, lo))
        } else {
            if prev.is_some() {
                out.changes.push(Change { ts: t, kind: Kind::Nodes, id: v.id, old: prev, new: None, action: "delete" });
            }
            prev = None;
            None
        };
        if pos != prev_pos {
            let (la, lo) = pos.unwrap_or((NO_COORD, NO_COORD));
            tl.push(npos(t, la, lo));
        }
        prev_pos = pos;
    }
    out.timeline = vs.first().map(|v| (v.id, Track::Node(tl)));
    out
}

fn way_coords(refs: &[i64], nodes: &Timeline<16>, t: i64) -> Vec<(i32, i32)> {
    refs.iter().map(|&r| at(nodes.get(r), t).and_then(npos_dec).unwrap_or((NO_COORD, NO_COORD))).collect()
}

fn way_key(coords: &[(i32, i32)]) -> (u32, [i32; 4]) {
    let mut b = MISSING_BBOX;
    let mut idxs = Vec::with_capacity(coords.len());
    for &(la, lo) in coords {
        if la != NO_COORD {
            idxs.push(quadtile::node_index(la, lo));
            extend_bbox(&mut b, la, la, lo, lo);
        }
    }
    (quadtile::calc_index(&idxs), b)
}

fn way_group(vs: Vec<Version>, nodes: &Timeline<16>) -> Out {
    let vs = collapse(vs);
    let mut out = Out::default();
    let mut prev: Option<i32> = None;
    let mut prev_coords: Option<Vec<(i32, i32)>> = None;
    let mut tl = Vec::new();
    for (k, v) in vs.iter().enumerate() {
        let t = v.meta.secs;
        let end = vs.get(k + 1).map(|n| n.meta.secs);
        if !v.meta.visible {
            if prev.is_some() {
                out.changes.push(Change { ts: t, kind: Kind::Ways, id: v.id, old: prev, new: None, action: "delete" });
                tl.push(wrec(t, false, 0, MISSING_BBOX));
            }
            prev = None;
            prev_coords = None;
            continue;
        }
        let Body::Way(refs) = &v.body else { unreachable!() };
        let mut bps = vec![t];
        for &r in refs {
            for rec in nodes.get(r) {
                let rt = rec_time(rec);
                if rt > t && end.is_none_or(|e| rt < e) {
                    bps.push(rt);
                }
            }
        }
        bps.sort_unstable();
        bps.dedup();
        // slices: the version start, then every time a member's position actually changes
        let mut slices: Vec<(i64, Vec<(i32, i32)>)> = Vec::new();
        for (i, &s) in bps.iter().enumerate() {
            let c = way_coords(refs, nodes, s);
            if i > 0 && slices.last().is_some_and(|l| l.1 == c) {
                continue;
            }
            slices.push((s, c));
        }
        for (i, (s, c)) in slices.iter().enumerate() {
            if i == 0 {
                version_change(&mut out, v, prev);
            } else {
                out.changes.push(Change { ts: *s, kind: Kind::Ways, id: v.id, old: Some(v.meta.version), new: Some(v.meta.version), action: "geometry" });
            }
            let (qt, b) = way_key(c);
            if prev_coords.as_ref() != Some(c) {
                tl.push(wrec(*s, true, qt, b));
            }
            prev_coords = Some(c.clone());
            let to = slices.get(i + 1).map(|n| n.0).or(end);
            out.rows.push(Row { v: v.clone(), from: *s, to, qt, bbox: b, coords: c.clone() });
        }
        prev = Some(v.meta.version);
        if end.is_none() {
            let mut rs: Vec<i64> = refs.clone();
            rs.sort_unstable();
            rs.dedup();
            out.parents.extend(rs.into_iter().map(|n| (n, v.id)));
        }
    }
    out.timeline = vs.first().map(|v| (v.id, Track::Way(tl)));
    out
}

/// What a member contributes at time t: its index and bbox (None while it doesn't exist).
fn contrib(mt: Kind, r: i64, nodes: &Timeline<16>, ways: &Timeline<32>, t: i64) -> Contrib {
    match mt {
        Kind::Nodes => match at(nodes.get(r), t).and_then(npos_dec) {
            Some((la, lo)) => (Some(quadtile::node_index(la, lo)), Some([la, la, lo, lo])),
            None => (None, None),
        },
        Kind::Ways => match at(ways.get(r), t).and_then(wrec_dec) {
            Some((q, wb)) => (Some(q), if wb[0] != i32::MAX { Some(wb) } else { None }),
            None => (None, None),
        },
        _ => (None, None),
    }
}

fn rel_group(vs: Vec<Version>, nodes: &Timeline<16>, ways: &Timeline<32>) -> Out {
    let vs = collapse(vs);
    let mut out = Out::default();
    let mut prev: Option<i32> = None;
    for (k, v) in vs.iter().enumerate() {
        let t = v.meta.secs;
        let end = vs.get(k + 1).map(|n| n.meta.secs);
        if !v.meta.visible {
            if prev.is_some() {
                out.changes.push(Change { ts: t, kind: Kind::Relations, id: v.id, old: prev, new: None, action: "delete" });
            }
            prev = None;
            continue;
        }
        let Body::Rel(members) = &v.body else { unreachable!() };
        // state at the version's start, then member changes in time order
        let mut state = RelState::default();
        let mut cur: Vec<Contrib> = Vec::with_capacity(members.len());
        let mut events: Vec<(i64, usize)> = Vec::new();
        for (i, (mt, r, _)) in members.iter().enumerate() {
            let c = contrib(*mt, *r, nodes, ways, t);
            state.apply(&c, true);
            cur.push(c);
            let times: Box<dyn Iterator<Item = i64>> = match mt {
                Kind::Nodes => Box::new(nodes.get(*r).iter().map(rec_time)),
                Kind::Ways => Box::new(ways.get(*r).iter().map(rec_time)),
                _ => Box::new(std::iter::empty()),
            };
            events.extend(times.filter(|&x| x > t && end.is_none_or(|e| x < e)).map(|x| (x, i)));
        }
        events.sort_unstable();
        version_change(&mut out, v, prev);
        let (qt, b) = state.key();
        let mut starts: Vec<(i64, u32, [i32; 4])> = vec![(t, qt, b)];
        let mut e = 0;
        while e < events.len() {
            let s = events[e].0;
            while e < events.len() && events[e].0 == s {
                let i = events[e].1;
                let (mt, r, _) = &members[i];
                let c = contrib(*mt, *r, nodes, ways, s);
                state.apply(&cur[i], false);
                state.apply(&c, true);
                cur[i] = c;
                e += 1;
            }
            // every member move is a change of the relation (updater cascade / Overpass)...
            out.changes.push(Change { ts: s, kind: Kind::Relations, id: v.id, old: Some(v.meta.version), new: Some(v.meta.version), action: "geometry" });
            // ...but a new row only when what the row stores changes
            let (qt, b) = state.key();
            if starts.last().is_some_and(|l| (l.1, l.2) != (qt, b)) {
                starts.push((s, qt, b));
            }
        }
        for (i, &(s, qt, b)) in starts.iter().enumerate() {
            let to = starts.get(i + 1).map(|n| n.0).or(end);
            out.rows.push(Row { v: v.clone(), from: s, to, qt, bbox: b, coords: vec![] });
        }
        prev = Some(v.meta.version);
    }
    out
}

// ---- record batches -----------------------------------------------------------------

fn batch(kind: Kind, rows: &[&Row], history: bool) -> Result<RecordBatch> {
    let mut id = Int64Builder::new();
    let mut tags = MapBuilder::new(Some(map_names()), StringBuilder::new(), StringBuilder::new());
    let (mut version, mut ts, mut changeset, mut uid, mut user) = (
        Int32Builder::new(),
        TimestampMillisecondBuilder::new().with_timezone("UTC"),
        Int64Builder::new(),
        Int32Builder::new(),
        StringBuilder::new(),
    );
    let mut qt = UInt32Builder::new();
    let mut bb = [Int32Builder::new(), Int32Builder::new(), Int32Builder::new(), Int32Builder::new()];
    let (mut vf, mut vt) =
        (TimestampMillisecondBuilder::new().with_timezone("UTC"), TimestampMillisecondBuilder::new().with_timezone("UTC"));
    let (mut lat, mut lon) = (Int32Builder::new(), Int32Builder::new());
    let mut nds = ListBuilder::new(Int64Builder::new());
    let mut nd_lat = ListBuilder::new(Int32Builder::new());
    let mut nd_lon = ListBuilder::new(Int32Builder::new());
    let mut members = ListBuilder::new(StructBuilder::from_fields(member_struct_fields(), 0));
    for r in rows {
        let v = &r.v;
        id.append_value(v.id);
        for (k, x) in &v.tags {
            tags.keys().append_value(k);
            tags.values().append_value(x);
        }
        tags.append(true)?;
        version.append_value(v.meta.version);
        ts.append_value(v.meta.secs * 1000);
        changeset.append_value(v.meta.changeset);
        uid.append_value(v.meta.uid);
        user.append_value(&v.meta.user);
        qt.append_value(r.qt);
        for (i, x) in r.bbox.iter().enumerate() {
            if r.bbox[0] == i32::MAX { bb[i].append_null() } else { bb[i].append_value(*x) }
        }
        vf.append_value(r.from * 1000);
        match r.to {
            Some(t) => vt.append_value(t * 1000),
            None => vt.append_null(),
        }
        match &v.body {
            Body::Node(la, lo) => {
                lat.append_value(*la);
                lon.append_value(*lo);
            }
            Body::Way(refs) => {
                for (n, (la, lo)) in refs.iter().zip(&r.coords) {
                    nds.values().append_value(*n);
                    nd_lat.values().append_value(*la);
                    nd_lon.values().append_value(*lo);
                }
                nds.append(true);
                nd_lat.append(true);
                nd_lon.append(true);
            }
            Body::Rel(ms) => {
                let mb = members.values();
                for (t, rf, role) in ms {
                    mb.field_builder::<StringBuilder>(0).unwrap().append_value(t.letter());
                    mb.field_builder::<Int64Builder>(1).unwrap().append_value(*rf);
                    mb.field_builder::<StringBuilder>(2).unwrap().append_value(role);
                    mb.append(true);
                }
                members.append(true);
            }
        }
    }
    let mut cols: Vec<ArrayRef> = vec![Arc::new(id.finish())];
    match kind {
        Kind::Nodes => {
            cols.push(Arc::new(lat.finish()));
            cols.push(Arc::new(lon.finish()));
        }
        Kind::Ways => {
            cols.push(Arc::new(nds.finish()));
            cols.push(Arc::new(nd_lat.finish()));
            cols.push(Arc::new(nd_lon.finish()));
        }
        Kind::Relations => cols.push(Arc::new(members.finish())),
    }
    cols.push(Arc::new(tags.finish()));
    cols.push(Arc::new(version.finish()));
    cols.push(Arc::new(ts.finish()));
    cols.push(Arc::new(changeset.finish()));
    cols.push(Arc::new(uid.finish()));
    cols.push(Arc::new(user.finish()));
    cols.push(Arc::new(qt.finish()));
    if kind != Kind::Nodes {
        for mut b in bb {
            cols.push(Arc::new(b.finish()));
        }
    }
    cols.push(Arc::new(vf.finish()));
    if history {
        cols.push(Arc::new(vt.finish()));
    }
    Ok(RecordBatch::try_new(schema(kind, history), cols)?)
}

fn changes_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())), false),
        Field::new("type", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
        Field::new("old_version", DataType::Int32, true),
        Field::new("new_version", DataType::Int32, true),
        Field::new("action", DataType::Utf8, false),
        Field::new("batch", DataType::Int64, false),
    ]))
}

fn changes_batch(cs: &[Change]) -> Result<RecordBatch> {
    let mut ts = TimestampMillisecondBuilder::new().with_timezone("UTC");
    let (mut t, mut id, mut old, mut new, mut action, mut batch) = (
        StringBuilder::new(),
        Int64Builder::new(),
        Int32Builder::new(),
        Int32Builder::new(),
        StringBuilder::new(),
        Int64Builder::new(),
    );
    for c in cs {
        ts.append_value(c.ts * 1000);
        t.append_value(c.kind.letter());
        id.append_value(c.id);
        old.append_option(c.old);
        new.append_option(c.new);
        action.append_value(c.action);
        batch.append_value(0); // build-time history: before any updater batch
    }
    let cols: Vec<ArrayRef> = vec![
        Arc::new(ts.finish()),
        Arc::new(t.finish()),
        Arc::new(id.finish()),
        Arc::new(old.finish()),
        Arc::new(new.finish()),
        Arc::new(action.finish()),
        Arc::new(batch.finish()),
    ];
    Ok(RecordBatch::try_new(changes_schema(), cols)?)
}

// ---- driver -------------------------------------------------------------------------

struct Writers {
    cur: [Buckets; 3],
    /// None with --no-history: no *_history tables and no changes log
    hist: Option<([Buckets; 3], Buckets)>,
    parents: Buckets,
    locator: Vec<ArrowWriter<File>>,
}

impl Writers {
    fn write(&mut self, kind: Kind, outs: Vec<Out>, layout: Layout) -> Result<()> {
        let k = kind.index();
        let rows: Vec<&Row> = outs.iter().flat_map(|o| o.rows.iter()).collect();
        let cur: Vec<&Row> = rows.iter().copied().filter(|r| r.to.is_none()).collect();
        let hist: Vec<&Row> = rows.iter().copied().filter(|r| r.to.is_some_and(|t| t > r.from)).collect();
        let bucket = |q: u32| layout.bucket(kind, q);
        if !cur.is_empty() {
            let b = batch(kind, &cur, false)?;
            self.locator[k].write(&locator_batch(&b)?)?;
            let keys: Vec<usize> = cur.iter().map(|r| bucket(r.qt)).collect();
            self.cur[k].write_split(&[b], |_| keys.clone())?;
        }
        if let Some((hw, _)) = self.hist.as_ref().filter(|_| !hist.is_empty()) {
            let keys: Vec<usize> = hist.iter().map(|r| bucket(r.qt)).collect();
            hw[k].write_split(&[batch(kind, &hist, true)?], |_| keys.clone())?;
        }
        let cs: Vec<Change> = outs.iter().flat_map(|o| o.changes.iter().cloned()).collect();
        if let Some((_, changes)) = self.hist.as_ref().filter(|_| !cs.is_empty()) {
            // ~48-day buckets of change time, so the build sorts the log one period at a time
            let keys: Vec<usize> = cs.iter().map(|c| ((c.ts.max(0) as u64 >> 22) as usize).min(1023)).collect();
            changes.write_split(&[changes_batch(&cs)?], |_| keys.clone())?;
        }
        let parents: Vec<(i64, i64)> = outs.iter().flat_map(|o| o.parents.iter().copied()).collect();
        if !parents.is_empty() {
            let (n, w): (Vec<i64>, Vec<i64>) = parents.into_iter().unzip();
            let pb = RecordBatch::try_new(parents_schema(), vec![Arc::new(Int64Array::from(n)), Arc::new(Int64Array::from(w))])?;
            self.parents.write_split(&[pb], |b| {
                let ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                ids.values().iter().map(|&i| layout.parent_bucket(i)).collect()
            })?;
        }
        Ok(())
    }
}

/// Split versions into complete per-element groups; the last group may continue in the next chunk.
fn take_groups(pending: &mut Vec<Version>, last_chunk: bool) -> Vec<Vec<Version>> {
    let mut groups: Vec<Vec<Version>> = Vec::new();
    let all = std::mem::take(pending);
    for v in all {
        match groups.last_mut() {
            Some(g) if g[0].kind == v.kind && g[0].id == v.id => g.push(v),
            _ => groups.push(vec![v]),
        }
    }
    if !last_chunk {
        if let Some(g) = groups.pop() {
            *pending = g;
        }
    }
    groups
}

/// One history extraction: version groups in, bucketed raw tables out. Fed in PBF order; a group
/// may continue in the next feed (versions of one element spanning blocks).
struct Pipeline {
    out: PathBuf,
    w: Writers,
    nodes: Timeline<16>,
    ways: Timeline<32>,
    counts: [u64; 3],
    versions: u64,
    phase: Kind, // the kind being processed; timelines of earlier kinds are complete
    pending: Vec<Version>,
    layout: Layout,
}

impl Pipeline {
    fn new(out: &Path, bits: u32, history: bool, dense: Option<(&Path, u64, u64)>) -> Result<Self> {
        std::fs::create_dir_all(out)?;
        let layout = Layout::new(bits);
        let nb = layout.count;
        let mk = |sub: &str, s: Arc<Schema>, n: usize| Buckets::new(out.join(sub), s, n);
        std::fs::create_dir_all(out.join("locator"))?;
        let mut locator = Vec::new();
        for t in ["n", "w", "r"] {
            let f = File::create(out.join("locator").join(format!("{t}.parquet")))?;
            locator.push(ArrowWriter::try_new(f, locator_schema(), Some(props()?))?);
        }
        let w = Writers {
            cur: [
                mk("nodes", schema(Kind::Nodes, false), nb)?,
                mk("ways", schema(Kind::Ways, false), nb)?,
                mk("relations", schema(Kind::Relations, false), nb)?,
            ],
            hist: if history {
                Some((
                    [
                        mk("nodes_history", schema(Kind::Nodes, true), nb)?,
                        mk("ways_history", schema(Kind::Ways, true), nb)?,
                        mk("relations_history", schema(Kind::Relations, true), nb)?,
                    ],
                    mk("changes", changes_schema(), 1024)?,
                ))
            } else {
                None
            },
            parents: mk("node_parent_ways", parents_schema(), nb)?,
            locator,
        };
        let (nodes, ways) = match dense {
            Some((dir, max_node, max_way)) => {
                std::fs::create_dir_all(dir)?;
                (Timeline::disk(dir, "node_positions", max_node)?, Timeline::disk(dir, "way_geometry", max_way)?)
            }
            None => (Timeline::ram(), Timeline::ram()),
        };
        Ok(Pipeline {
            out: out.to_path_buf(),
            w,
            nodes,
            ways,
            counts: [0; 3],
            versions: 0,
            phase: Kind::Nodes,
            pending: Vec::new(),
            layout,
        })
    }

    /// Versions in PBF order; `last` flushes the final group.
    fn feed(&mut self, vs: Vec<Version>, last: bool) -> Result<()> {
        self.versions += vs.len() as u64;
        self.pending.extend(vs);
        let mut by_kind: [Vec<Vec<Version>>; 3] = [Vec::new(), Vec::new(), Vec::new()];
        for g in take_groups(&mut self.pending, last) {
            by_kind[g[0].kind.index()].push(g);
        }
        // process by type in PBF order; a type's timeline is complete once the next type starts
        for (kind, gs) in Kind::ALL.into_iter().zip(by_kind) {
            if gs.is_empty() {
                continue;
            }
            if kind < self.phase {
                bail!("PBF not sorted by type");
            }
            while self.phase < kind {
                match self.phase {
                    Kind::Nodes => self.nodes.finish()?,
                    _ => self.ways.finish()?,
                }
                self.phase = self.phase.next();
            }
            let (nodes, ways) = (&self.nodes, &self.ways);
            // dense index: copy what this batch looks up into RAM first, in sorted order
            let (mut want_n, mut want_w) = (Vec::new(), Vec::new());
            if nodes.is_disk() {
                for v in gs.iter().flatten() {
                    match &v.body {
                        Body::Way(refs) => want_n.extend_from_slice(refs),
                        Body::Rel(ms) => {
                            for (mk, r, _) in ms {
                                match mk {
                                    Kind::Nodes => want_n.push(*r),
                                    Kind::Ways => want_w.push(*r),
                                    Kind::Relations => {}
                                }
                            }
                        }
                        Body::Node(..) => {}
                    }
                }
            }
            let local_n = (!want_n.is_empty()).then(|| nodes.gather(want_n));
            let local_w = (!want_w.is_empty()).then(|| ways.gather(want_w));
            let (nodes, ways) = (local_n.as_ref().unwrap_or(nodes), local_w.as_ref().unwrap_or(ways));
            let outs: Vec<Out> = match kind {
                Kind::Nodes => gs.into_par_iter().map(node_group).collect(),
                Kind::Ways => gs.into_par_iter().map(|g| way_group(g, nodes)).collect(),
                Kind::Relations => gs.into_par_iter().map(|g| rel_group(g, nodes, ways)).collect(),
            };
            for o in &outs {
                self.counts[kind.index()] += o.rows.iter().filter(|r| r.to.is_none()).count() as u64;
                match &o.timeline {
                    Some((id, Track::Node(recs))) => self.nodes.push(*id, recs)?,
                    Some((id, Track::Way(recs))) => self.ways.push(*id, recs)?,
                    None => {}
                }
            }
            self.w.write(kind, outs, self.layout)?;
        }
        Ok(())
    }

    /// Close the files and write manifest.json; returns a summary line.
    fn finish(self) -> Result<String> {
        let Pipeline { out, w, counts, versions, layout, .. } = self;
        let Writers { cur, hist, parents, locator } = w;
        for l in locator {
            l.close()?;
        }
        let mut hist_rows = [0u64; 3];
        for b in cur {
            b.close()?;
        }
        let history = hist.is_some();
        let mut n_changes = 0;
        if let Some((hs, changes)) = hist {
            for (i, b) in hs.into_iter().enumerate() {
                hist_rows[i] = b.close()?;
            }
            n_changes = changes.close()?;
        }
        let n_parents = parents.close()?;
        let history_fields = if history {
            format!(
                ", \"nodes_history\": {}, \"ways_history\": {}, \"relations_history\": {}, \"changes\": {n_changes}",
                hist_rows[0], hist_rows[1], hist_rows[2]
            )
        } else {
            String::new()
        };
        let mut m = File::create(out.join("manifest.json"))?;
        writeln!(
            m,
            "{{\"format\": 2, \"history\": {history}, \"bucket_bits\": {}, \"node_id_shift\": {}, \
             \"versions\": {versions}, \"nodes\": {}, \"ways\": {}, \"relations\": {}{history_fields}, \
             \"node_parent_ways\": {n_parents}{}}}",
            layout.bits, layout.id_shift, counts[0], counts[1], counts[2], crate::header_fields()
        )?;
        Ok(format!(
            "{versions} versions -> current n={} w={} r={}, history n={} w={} r={}, changes={n_changes}",
            counts[0], counts[1], counts[2], hist_rows[0], hist_rows[1], hist_rows[2]
        ))
    }
}

pub fn run(args: &Args) -> Result<()> {
    let t0 = Instant::now();
    let dense = args.dense.as_deref().map(|d| (d, args.max_node_id, args.max_way_id));
    let mut p = Pipeline::new(&args.out, args.bucket_bits, !args.no_history, dense)?;
    let mut blobs = crate::open_input(&args.input)?.peekable();
    let mut progress = crate::Progress::new();
    loop {
        let chunk: Vec<_> = blobs.by_ref().take(CHUNK_BLOBS).collect::<std::result::Result<_, _>>()?;
        let last = chunk.is_empty() || blobs.peek().is_none();
        let decoded: Vec<Vec<Version>> = chunk
            .par_iter()
            .filter_map(|b| match b.decode() {
                Ok(BlobDecode::OsmData(block)) => Some(Ok(decode(&block))),
                Ok(BlobDecode::OsmHeader(hd)) => {
                    crate::note_header(&hd);
                    None
                }
                Ok(_) => None,
                Err(e) => Some(Err(e)),
            })
            .collect::<std::result::Result<_, _>>()?;
        p.feed(decoded.into_iter().flatten().collect(), last)?;
        progress.tick(|| format!("versions={} (current n={} w={} r={})", p.versions, p.counts[0], p.counts[1], p.counts[2]));
        if last {
            break;
        }
    }
    let summary = p.finish()?;
    println!("history: {summary} {:.1}s", t0.elapsed().as_secs_f64());
    Ok(())
}
