//! Incremental relation key and bbox.
//!
//! `RelState` is a relation's spatial key (`quadtile::calc_index` over its members' distinct
//! indexes) and bbox (extremes of its members' bboxes) maintained under member changes in O(log n)
//! each, instead of recomputing over all members (big boundaries have thousands of members and
//! thousands of member moves).
//!
//! Invariant: `key()` equals what `calc_index` (on the sorted distinct member indexes) and a
//! min/max over member bboxes would give for the contributions currently added and not removed.
//! Every `apply(c, false)` must therefore repeat exactly a contribution that was added with
//! `apply(c, true)` and not yet removed (the caller keeps each member's current one); removing one
//! that was never added corrupts the counts.

use std::collections::BTreeMap;

use crate::quadtile;
use crate::tables::MISSING_BBOX;

/// One member's (spatial index, bbox) at some instant; `None` parts mean it has none.
#[derive(Default)]
pub(crate) struct RelState {
    idx: BTreeMap<u32, u32>,     // distinct member indexes (with multiplicity)
    comp: [BTreeMap<u32, u32>; 4], // their calc_index components: lat_min, lon_min, lat_max, lon_max
    global: u32,                 // members with the planet-wide index 0x80000080
    bb: [BTreeMap<i32, u32>; 4], // member bboxes: min_lat, max_lat, min_lon, max_lon
}

pub(crate) type Contrib = (Option<u32>, Option<[i32; 4]>);

fn bump<K: Ord + Copy>(m: &mut BTreeMap<K, u32>, k: K, add: bool) {
    if add {
        *m.entry(k).or_insert(0) += 1;
    } else if let Some(c) = m.get_mut(&k) {
        *c -= 1;
        if *c == 0 {
            m.remove(&k);
        }
    }
}

impl RelState {
    pub(crate) fn apply(&mut self, c: &Contrib, add: bool) {
        if let Some(i) = c.0 {
            let before = self.idx.get(&i).copied().unwrap_or(0);
            bump(&mut self.idx, i, add);
            let after = self.idx.get(&i).copied().unwrap_or(0);
            if (before == 0) != (after == 0) {
                // the distinct set changed: add or remove this index's components
                match quadtile::components(i) {
                    Some((a, b, c2, d)) => {
                        for (m, k) in self.comp.iter_mut().zip([a, b, c2, d]) {
                            bump(m, k, after > 0);
                        }
                    }
                    None => {
                        if after > 0 { self.global += 1 } else { self.global -= 1 }
                    }
                }
            }
        }
        if let Some(b) = c.1 {
            for (m, k) in self.bb.iter_mut().zip(b) {
                bump(m, k, add);
            }
        }
    }

    pub(crate) fn key(&self) -> (u32, [i32; 4]) {
        let qt = match self.idx.keys().next() {
            None => 0xFE,
            Some(_) if self.global > 0 => 0x8000_0080,
            Some(&first) => quadtile::calc_index_from(
                first,
                *self.comp[0].keys().next().unwrap(),
                *self.comp[1].keys().next().unwrap(),
                *self.comp[2].keys().next_back().unwrap(),
                *self.comp[3].keys().next_back().unwrap(),
            ),
        };
        let bbox = if self.bb[0].is_empty() {
            MISSING_BBOX
        } else {
            [
                *self.bb[0].keys().next().unwrap(),
                *self.bb[1].keys().next_back().unwrap(),
                *self.bb[2].keys().next().unwrap(),
                *self.bb[3].keys().next_back().unwrap(),
            ]
        };
        (qt, bbox)
    }
}
