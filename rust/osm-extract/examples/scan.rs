// Decode throughput: parallel blob decode + touching every element's id/coords/refs (no allocation).
use osmpbf::{BlobDecode, BlobReader};
use rayon::prelude::*;
use std::time::Instant;
fn main() {
    let path = std::env::args().nth(1).unwrap();
    let t = Instant::now();
    let mut blobs = BlobReader::from_path(&path).unwrap().peekable();
    let (mut n, mut bytes) = (0u64, std::fs::metadata(&path).unwrap().len());
    while blobs.peek().is_some() {
        let chunk: Vec<_> = blobs.by_ref().take(256).map(|b| b.unwrap()).collect();
        n += chunk.par_iter().map(|b| match b.decode() {
            Ok(BlobDecode::OsmData(block)) => {
                let mut c = 0u64;
                for g in block.groups() {
                    for d in g.dense_nodes() { c += (d.id() ^ d.decimicro_lat() as i64) as u64 & 1 | 1; }
                    for w in g.ways() { c += w.raw_refs().len() as u64; }
                    for r in g.relations() { c += r.members().count() as u64; }
                }
                c
            }
            _ => 0,
        }).sum::<u64>();
    }
    let s = t.elapsed().as_secs_f64();
    bytes = bytes.max(1);
    println!("{n} items, {:.1} MB/s ({:.1}s)", bytes as f64 / 1e6 / s, s);
}
