//! Overpass spatial index math (port of src/overpass_api/core/index_computations.h),
//! mirroring src/quadtile.py which is fuzz-tested against the C++ header.

#[inline]
fn spread(mut v: u32) -> u32 {
    v &= 0xFFFF_0000;
    v |= v >> 8;
    v &= 0xFF00_FF00;
    v |= v >> 4;
    v &= 0xF0F0_F0F0;
    v |= v >> 2;
    v &= 0xCCCC_CCCC;
    v |= v >> 1;
    v & 0xAAAA_AAAA
}

#[inline]
pub fn ll_upper(ilat: u32, ilon: u32) -> u32 {
    spread(ilat) | (spread(ilon) >> 1)
}

/// Node index from 1e-7 degree integer coordinates (`ll_upper_` in Overpass).
#[inline]
pub fn node_index(lat_e7: i32, lon_e7: i32) -> u32 {
    let ilat = (lat_e7 as i64 + 910_000_000) as u32;
    ll_upper(ilat, lon_e7 as u32) ^ 0x4000_0000
}

#[inline]
fn compact(mut q: u32) -> u32 {
    q |= q >> 1;
    q &= 0x3333_3333;
    q |= q >> 2;
    q &= 0x0F0F_0F0F;
    q |= q >> 4;
    q &= 0x00FF_00FF;
    q |= q >> 8;
    q & 0xFFFF
}

#[inline]
fn upper_ilat(q: u32) -> u32 {
    compact((q & 0xAAAA_AAAA) >> 1)
}

#[inline]
fn upper_ilon(q: u32) -> u32 {
    compact(q & 0x5555_5555)
}

const LEVELS: [(u32, u32, u32, u32); 7] = [
    (0x01, 0x2AAA_AAA8, 0x5555_5554, 0x3),
    (0x02, 0x2AAA_AA80, 0x5555_5540, 0xF),
    (0x04, 0x2AAA_A800, 0x5555_5400, 0x3F),
    (0x08, 0x2AAA_8000, 0x5555_4000, 0xFF),
    (0x10, 0x2AA8_0000, 0x5554_0000, 0x3FF),
    (0x20, 0x2A80_0000, 0x5540_0000, 0xFFF),
    (0x40, 0x2800_0000, 0x5400_0000, 0x3FFF),
];

/// (lat_min, lon_min, lat_max, lon_max) of a compound index, None for 0x80000080.
fn compound_bounds(idx: u32) -> Option<(u32, u32, u32, u32)> {
    for &(flag, mlat, mlon, span) in &LEVELS {
        if idx & flag != 0 {
            let lat = idx & mlat;
            let lon = idx & mlon;
            return Some((
                lat,
                lon,
                ll_upper((upper_ilat(lat) + span) << 16, 0),
                ll_upper(0, (upper_ilon(lon) + span) << 16),
            ));
        }
    }
    None
}

/// `calc_index`: spatial index of a way (node indexes) or relation (member node/way indexes).
pub fn calc_index(idxs: &[u32]) -> u32 {
    let Some(&first) = idxs.first() else { return 0xFE };
    let (mut lat_min, mut lon_min, mut lat_max, mut lon_max);
    if first & 0x8000_0000 != 0 {
        match compound_bounds(first) {
            None => return 0x8000_0080,
            Some((a, b, c, d)) => (lat_min, lon_min, lat_max, lon_max) = (a, b, c, d),
        }
    } else {
        lat_min = first & 0x2AAA_AAAA;
        lat_max = lat_min;
        lon_min = first & 0x5555_5555;
        lon_max = lon_min;
    }
    for &v in &idxs[1..] {
        if v & 0x8000_0000 != 0 {
            match compound_bounds(v) {
                None => return 0x8000_0080,
                Some((la, lo, la_u, lo_u)) => {
                    lat_min = lat_min.min(la);
                    lat_max = lat_max.max(la_u);
                    lon_min = lon_min.min(lo);
                    lon_max = lon_max.max(lo_u);
                }
            }
        } else {
            let (la, lo) = (v & 0x2AAA_AAAA, v & 0x5555_5555);
            if la < lat_min {
                lat_min = la;
            } else if la > lat_max {
                lat_max = la;
            }
            if lo < lon_min {
                lon_min = lo;
            } else if lo > lon_max {
                lon_max = lo;
            }
        }
    }
    if lat_max == lat_min && lon_max == lon_min {
        return first;
    }
    let (a, b, c, d) = (upper_ilat(lat_min), upper_ilat(lat_max), upper_ilon(lon_min), upper_ilon(lon_max));
    const STEPS: [(u32, u32, u32, u32); 7] = [
        (0xFFFE, 4, 0xFFFF_FFFC, 0x8000_0001),
        (0xFFF8, 0x10, 0xFFFF_FFC0, 0x8000_0002),
        (0xFFE0, 0x40, 0xFFFF_FC00, 0x8000_0004),
        (0xFF80, 0x100, 0xFFFF_C000, 0x8000_0008),
        (0xFE00, 0x400, 0xFFFC_0000, 0x8000_0010),
        (0xF800, 0x1000, 0xFFC0_0000, 0x8000_0020),
        (0xE000, 0x4000, 0xFC00_0000, 0x8000_0040),
    ];
    for (m16, lim, m32, flag) in STEPS {
        if (b & m16).wrapping_sub(a & m16) < lim && (d & m16).wrapping_sub(c & m16) < lim {
            return ((lon_min | lat_min) & m32) | flag;
        }
    }
    0x8000_0080
}
