//! `--cut name:s,w,n,e` (repeatable, with --history): one pass over a full-history planet that
//! writes a history extraction for each region and counts the whole planet.
//!
//! Made for sizing the planet build when only the planet itself is available: each region's output
//! is what `--history` would produce for a regional history file (simple bbox strategy: nodes with
//! any version inside the box, ways with any version using such a node, relations with any version
//! using a kept member; members outside are missing, as in an extract), and `planet_stats.json`
//! has the planet's exact totals (versions, deletions, node position changes, refs, members, tags),
//! so measured per-version costs can be scaled to the planet.
//!
//! The whole planet is decoded, but only elements of a region are materialised, so the pass runs
//! at decode (or download) speed. Versions of one element that straddle a block boundary are
//! judged per block, so a few kept elements may miss their earliest versions; negligible for sizing.

use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use osmpbf::{BlobDecode, PrimitiveBlock, RelMemberType};
use rayon::prelude::*;
use roaring::RoaringTreemap;

use crate::Args;
use crate::history::{Pipeline, Version, dense_version, node_version, rel_version, way_version};

const CHUNK_BLOBS: usize = 64;

struct Region {
    name: String,
    bbox: [i32; 4], // s, w, n, e in 1e-7 degrees
    pipeline: Pipeline,
    nodes: RoaringTreemap,
    ways: RoaringTreemap,
    rels: RoaringTreemap,
}

fn parse_region(spec: &str) -> Result<(String, [i32; 4])> {
    let (name, b) = spec.split_once(':').context("--cut name:s,w,n,e")?;
    let v: Vec<f64> = b.split(',').map(|x| x.trim().parse()).collect::<std::result::Result<_, _>>()?;
    if v.len() != 4 {
        bail!("--cut {spec}: need s,w,n,e");
    }
    let e7 = |x: f64| (x * 1e7).round() as i32;
    Ok((name.to_string(), [e7(v[0]), e7(v[1]), e7(v[2]), e7(v[3])]))
}

fn inside(b: &[i32; 4], lat: i32, lon: i32) -> bool {
    b[0] <= lat && lat <= b[2] && b[1] <= lon && lon <= b[3]
}

/// Planet totals (exact, from every element).
#[derive(Default, Clone)]
struct Stats {
    versions: [u64; 3],
    deleted: [u64; 3],
    ids: [u64; 3],
    tags: [u64; 3],
    node_moves: u64, // timeline records: first position, each later change, deletions
    way_refs: u64,
    rel_members: u64,
    max_id: [i64; 3],
}

impl Stats {
    fn add(&mut self, o: &Stats) {
        for k in 0..3 {
            self.versions[k] += o.versions[k];
            self.deleted[k] += o.deleted[k];
            self.ids[k] += o.ids[k];
            self.tags[k] += o.tags[k];
            self.max_id[k] = self.max_id[k].max(o.max_id[k]);
        }
        self.node_moves += o.node_moves;
        self.way_refs += o.way_refs;
        self.rel_members += o.rel_members;
    }
}

fn kind_of(block: &PrimitiveBlock) -> Option<u8> {
    for g in block.groups() {
        if g.dense_nodes().next().is_some() || g.nodes().next().is_some() {
            return Some(0);
        }
        if g.ways().next().is_some() {
            return Some(1);
        }
        if g.relations().next().is_some() {
            return Some(2);
        }
    }
    None
}

/// Node block: stats, and per region the ids with a visible version inside the box plus all their
/// versions in this block.
fn node_block(block: &PrimitiveBlock, boxes: &[[i32; 4]]) -> (Stats, Vec<Vec<Version>>, Vec<Vec<u64>>) {
    let mut st = Stats::default();
    let mut ids: Vec<Vec<u64>> = vec![Vec::new(); boxes.len()];
    let mut last: Option<(i64, bool, i32, i32)> = None;
    let mut seen = |id: i64, visible: bool, lat: i32, lon: i32, ntags: usize, st: &mut Stats, ids: &mut Vec<Vec<u64>>| {
        st.versions[0] += 1;
        st.tags[0] += ntags as u64;
        st.max_id[0] = st.max_id[0].max(id);
        if !visible {
            st.deleted[0] += 1;
        }
        let pos = (visible, if visible { lat } else { 0 }, if visible { lon } else { 0 });
        match last {
            Some((lid, v, la, lo)) if lid == id => {
                if (v, la, lo) != pos {
                    st.node_moves += 1;
                }
            }
            _ => {
                st.ids[0] += 1;
                if visible {
                    st.node_moves += 1;
                }
            }
        }
        last = Some((id, pos.0, pos.1, pos.2));
        if visible {
            for (r, b) in boxes.iter().enumerate() {
                if inside(b, lat, lon) && ids[r].last() != Some(&(id as u64)) {
                    ids[r].push(id as u64);
                }
            }
        }
    };
    for g in block.groups() {
        for n in g.dense_nodes() {
            let visible = n.info().is_none_or(|i| i.visible());
            seen(n.id(), visible, n.decimicro_lat(), n.decimicro_lon(), n.tags().count(), &mut st, &mut ids);
        }
        for n in g.nodes() {
            seen(n.id(), n.info().visible(), n.decimicro_lat(), n.decimicro_lon(), n.tags().count(), &mut st, &mut ids);
        }
    }
    let mut vs: Vec<Vec<Version>> = vec![Vec::new(); boxes.len()];
    if ids.iter().any(|v| !v.is_empty()) {
        for g in block.groups() {
            for n in g.dense_nodes() {
                for r in 0..boxes.len() {
                    if ids[r].binary_search(&(n.id() as u64)).is_ok() {
                        vs[r].push(dense_version(&n));
                    }
                }
            }
            for n in g.nodes() {
                for r in 0..boxes.len() {
                    if ids[r].binary_search(&(n.id() as u64)).is_ok() {
                        vs[r].push(node_version(&n));
                    }
                }
            }
        }
    }
    (st, vs, ids)
}

fn way_block(block: &PrimitiveBlock, kept: &[&RoaringTreemap]) -> (Stats, Vec<Vec<Version>>, Vec<Vec<u64>>) {
    let mut st = Stats::default();
    let mut ids: Vec<Vec<u64>> = vec![Vec::new(); kept.len()];
    let mut last = None;
    for g in block.groups() {
        for w in g.ways() {
            st.versions[1] += 1;
            st.tags[1] += w.tags().count() as u64;
            st.max_id[1] = st.max_id[1].max(w.id());
            if !w.info().visible() {
                st.deleted[1] += 1;
            }
            if last != Some(w.id()) {
                st.ids[1] += 1;
            }
            last = Some(w.id());
            let refs: Vec<i64> = w.refs().collect();
            st.way_refs += refs.len() as u64;
            for (r, set) in kept.iter().enumerate() {
                if ids[r].last() != Some(&(w.id() as u64)) && refs.iter().any(|&n| set.contains(n as u64)) {
                    ids[r].push(w.id() as u64);
                }
            }
        }
    }
    let mut vs: Vec<Vec<Version>> = vec![Vec::new(); kept.len()];
    if ids.iter().any(|v| !v.is_empty()) {
        for g in block.groups() {
            for w in g.ways() {
                for r in 0..kept.len() {
                    if ids[r].binary_search(&(w.id() as u64)).is_ok() {
                        vs[r].push(way_version(&w));
                    }
                }
            }
        }
    }
    (st, vs, ids)
}

fn rel_block(block: &PrimitiveBlock, kept: &[[&RoaringTreemap; 3]]) -> (Stats, Vec<Vec<Version>>, Vec<Vec<u64>>) {
    let mut st = Stats::default();
    let mut ids: Vec<Vec<u64>> = vec![Vec::new(); kept.len()];
    let mut last = None;
    for g in block.groups() {
        for rel in g.relations() {
            st.versions[2] += 1;
            st.tags[2] += rel.tags().count() as u64;
            st.max_id[2] = st.max_id[2].max(rel.id());
            if !rel.info().visible() {
                st.deleted[2] += 1;
            }
            if last != Some(rel.id()) {
                st.ids[2] += 1;
            }
            last = Some(rel.id());
            let members: Vec<(usize, u64)> = rel
                .members()
                .map(|m| {
                    let t = match m.member_type {
                        RelMemberType::Node => 0,
                        RelMemberType::Way => 1,
                        RelMemberType::Relation => 2,
                    };
                    (t, m.member_id as u64)
                })
                .collect();
            st.rel_members += members.len() as u64;
            for (r, sets) in kept.iter().enumerate() {
                if ids[r].last() != Some(&(rel.id() as u64)) && members.iter().any(|&(t, m)| sets[t].contains(m)) {
                    ids[r].push(rel.id() as u64);
                }
            }
        }
    }
    let mut vs: Vec<Vec<Version>> = vec![Vec::new(); kept.len()];
    if ids.iter().any(|v| !v.is_empty()) {
        for g in block.groups() {
            for rel in g.relations() {
                for r in 0..kept.len() {
                    if ids[r].binary_search(&(rel.id() as u64)).is_ok() {
                        vs[r].push(rel_version(&rel));
                    }
                }
            }
        }
    }
    (st, vs, ids)
}

fn write_stats(out: &std::path::Path, planet: &Stats, blobs: u64, blocks: u64, secs: f64, partial: bool) -> Result<()> {
    let tmp = out.join("planet_stats.json.tmp");
    let mut f = File::create(&tmp)?;
    writeln!(
        f,
        "{{\"partial\": {partial}, \"versions\": {:?}, \"deleted\": {:?}, \"ids\": {:?}, \"tags\": {:?}, \
         \"node_moves\": {}, \"way_refs\": {}, \"rel_members\": {}, \"max_id\": {:?}, \"blobs\": {blobs}, \
         \"blocks\": {blocks}, \"seconds\": {secs:.0}{}}}",
        planet.versions,
        planet.deleted,
        planet.ids,
        planet.tags,
        planet.node_moves,
        planet.way_refs,
        planet.rel_members,
        planet.max_id,
        crate::header_fields()
    )?;
    drop(f);
    std::fs::rename(tmp, out.join("planet_stats.json"))?;
    Ok(())
}

pub fn run(args: &Args) -> Result<()> {
    let t0 = Instant::now();
    std::fs::create_dir_all(&args.out)?;
    let mut regions: Vec<Region> = Vec::new();
    for spec in &args.cuts {
        let (name, bbox) = parse_region(spec)?;
        let out: PathBuf = args.out.join(&name);
        regions.push(Region {
            pipeline: Pipeline::new(&out, args.bucket_bits, None)?,
            name,
            bbox,
            nodes: RoaringTreemap::new(),
            ways: RoaringTreemap::new(),
            rels: RoaringTreemap::new(),
        });
    }
    let boxes: Vec<[i32; 4]> = regions.iter().map(|r| r.bbox).collect();
    let mut planet = Stats::default();
    let (mut blobs_n, mut bytes) = (0u64, 0u64);
    let mut blobs = crate::open_input(&args.input)?.peekable();
    let mut last_report = Instant::now();
    loop {
        let chunk: Vec<_> = blobs.by_ref().take(CHUNK_BLOBS).collect::<std::result::Result<_, _>>()?;
        let last = chunk.is_empty() || blobs.peek().is_none();
        blobs_n += chunk.len() as u64;
        let decoded: Vec<PrimitiveBlock> = chunk
            .par_iter()
            .filter_map(|b| match b.decode() {
                Ok(BlobDecode::OsmData(block)) => Some(Ok(block)),
                Ok(BlobDecode::OsmHeader(hd)) => {
                    crate::note_header(&hd);
                    None
                }
                Ok(_) => None,
                Err(e) => Some(Err(e)),
            })
            .collect::<std::result::Result<_, _>>()?;
        bytes += decoded.len() as u64;
        let kinds: Vec<Option<u8>> = decoded.iter().map(kind_of).collect();
        // per region, versions in PBF order across this chunk's node, way and relation blocks
        let mut feed: Vec<Vec<Version>> = vec![Vec::new(); regions.len()];
        for k in 0..3u8 {
            let blocks: Vec<&PrimitiveBlock> =
                decoded.iter().zip(&kinds).filter(|(_, kd)| **kd == Some(k)).map(|(b, _)| b).collect();
            if blocks.is_empty() {
                continue;
            }
            let results: Vec<(Stats, Vec<Vec<Version>>, Vec<Vec<u64>>)> = match k {
                0 => blocks.par_iter().map(|b| node_block(b, &boxes)).collect(),
                1 => {
                    let sets: Vec<&RoaringTreemap> = regions.iter().map(|r| &r.nodes).collect();
                    blocks.par_iter().map(|b| way_block(b, &sets)).collect()
                }
                _ => {
                    let sets: Vec<[&RoaringTreemap; 3]> = regions.iter().map(|r| [&r.nodes, &r.ways, &r.rels]).collect();
                    blocks.par_iter().map(|b| rel_block(b, &sets)).collect()
                }
            };
            for (st, vs, ids) in results {
                planet.add(&st);
                for (r, (v, i)) in vs.into_iter().zip(ids).enumerate() {
                    feed[r].extend(v);
                    let set = match k {
                        0 => &mut regions[r].nodes,
                        1 => &mut regions[r].ways,
                        _ => &mut regions[r].rels,
                    };
                    set.extend(i);
                }
            }
        }
        for (r, vs) in feed.into_iter().enumerate() {
            regions[r].pipeline.feed(vs, last)?;
        }
        if last_report.elapsed().as_secs() >= 60 || last {
            last_report = Instant::now();
            // totals so far, rewritten as the pass goes: a crash keeps what was counted
            write_stats(&args.out, &planet, blobs_n, bytes, t0.elapsed().as_secs_f64(), true)?;
            let s = t0.elapsed().as_secs_f64();
            println!(
                "{:.0}s blobs={blobs_n} versions n={} w={} r={} | {}",
                s,
                planet.versions[0],
                planet.versions[1],
                planet.versions[2],
                regions.iter().map(|r| format!("{}={}", r.name, r.pipeline.versions)).collect::<Vec<_>>().join(" ")
            );
            std::io::stdout().flush()?;
        }
        if last {
            break;
        }
    }
    let secs = t0.elapsed().as_secs_f64();
    let mut summary = Vec::new();
    for r in regions {
        let extra = format!(", \"region\": \"{}\", \"bbox_e7\": {:?}", r.name, r.bbox);
        let name = r.name.clone();
        summary.push(format!("{name}: {}", r.pipeline.finish(&extra)?));
    }
    write_stats(&args.out, &planet, blobs_n, bytes, secs, false)?;
    println!("cut: {secs:.0}s; planet versions n={} w={} r={}", planet.versions[0], planet.versions[1], planet.versions[2]);
    for s in summary {
        println!("  {s}");
    }
    Ok(())
}
