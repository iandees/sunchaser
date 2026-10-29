"""Exact port of Overpass's spatial index math (src/overpass_api/core/index_computations.h).

Node index    = ll_upper_(lat, lon): 16+16 bit Morton key of the upper coordinate bits.
Way/rel index = calc_index(member idxs): the node tile if all members share one tile,
                otherwise 0x80000000 | coarse tile | level flag (0x01 .. 0x80).
"""
import numpy as np

M32 = 0xFFFFFFFF


def ilat(lat):
    return int((lat + 91.0) * 10000000 + 0.5)


def ilon(lon):
    return int(lon * 10000000 + (0.5 if lon > 0 else -0.5))


def _spread(v):
    v &= 0xFFFF0000
    v |= v >> 8
    v &= 0xFF00FF00
    v |= v >> 4
    v &= 0xF0F0F0F0
    v |= v >> 2
    v &= 0xCCCCCCCC
    v |= v >> 1
    v &= 0xAAAAAAAA
    return v


def ll_upper(ilat_, ilon_):
    return (_spread(ilat_ & M32) | (_spread(ilon_ & M32) >> 1)) & M32


def ll_upper_(ilat_, ilon_):
    return ll_upper(ilat_, ilon_) ^ 0x40000000


def _compact(q):
    q |= q >> 1
    q &= 0x33333333
    q |= q >> 2
    q &= 0x0F0F0F0F
    q |= q >> 4
    q &= 0x00FF00FF
    q |= q >> 8
    return q & 0xFFFF


def upper_ilat(q):
    return _compact((q & 0xAAAAAAAA) >> 1)


def upper_ilon(q):
    return _compact(q & 0x55555555)


# ---- vectorised node index -------------------------------------------------

def _spread_np(v):
    v = v & np.uint32(0xFFFF0000)
    for shift, mask in ((8, 0xFF00FF00), (4, 0xF0F0F0F0), (2, 0xCCCCCCCC), (1, 0xAAAAAAAA)):
        v = (v | (v >> np.uint32(shift))) & np.uint32(mask)
    return v


def node_index_np(lat_e7, lon_e7):
    """lat_e7/lon_e7: int64 arrays of round(coord*1e7). Returns uint32 Overpass node index."""
    il = (np.asarray(lat_e7, dtype=np.int64) + 910000000).astype(np.uint32)
    io = np.asarray(lon_e7, dtype=np.int64).astype(np.uint32)  # two's complement, as in C++
    return (_spread_np(il) | (_spread_np(io) >> np.uint32(1))) ^ np.uint32(0x40000000)


# ---- way / relation index --------------------------------------------------

# (flag, lat mask, lon mask, tile span in 16-bit units)
_LEVELS = [
    (0x01, 0x2AAAAAA8, 0x55555554, 0x3),
    (0x02, 0x2AAAAA80, 0x55555540, 0xF),
    (0x04, 0x2AAAA800, 0x55555400, 0x3F),
    (0x08, 0x2AAA8000, 0x55554000, 0xFF),
    (0x10, 0x2AA80000, 0x55540000, 0x3FF),
    (0x20, 0x2A800000, 0x55400000, 0xFFF),
    (0x40, 0x28000000, 0x54000000, 0x3FFF),
]


def _compound_bounds(idx):
    for flag, mlat, mlon, span in _LEVELS:
        if idx & flag:
            lat = idx & mlat
            lon = idx & mlon
            return lat, lon, ll_upper((upper_ilat(lat) + span) << 16, 0), ll_upper(0, (upper_ilon(lon) + span) << 16)
    return None  # 0x80000080


def calc_index(idxs):
    if not idxs:
        return 0xFE
    it = iter(idxs)
    first = next(it)
    lat_min = lat_max = first & 0x2AAAAAAA
    lon_min = lon_max = first & 0x55555555
    if first & 0x80000000:
        b = _compound_bounds(first)
        if b is None:
            return 0x80000080
        lat_min, lon_min, lat_max, lon_max = b
    for v in it:
        if v & 0x80000000:
            b = _compound_bounds(v)
            if b is None:
                return 0x80000080
            la, lo, la_u, lo_u = b
            lat_min = min(lat_min, la)
            lat_max = max(lat_max, la_u)
            lon_min = min(lon_min, lo)
            lon_max = max(lon_max, lo_u)
        else:
            la, lo = v & 0x2AAAAAAA, v & 0x55555555
            if la < lat_min:
                lat_min = la
            elif la > lat_max:
                lat_max = la
            if lo < lon_min:
                lon_min = lo
            elif lo > lon_max:
                lon_max = lo

    if lat_max == lat_min and lon_max == lon_min:
        return first
    a, b_, c, d = upper_ilat(lat_min), upper_ilat(lat_max), upper_ilon(lon_min), upper_ilon(lon_max)
    for m16, lim, m32, flag in (
        (0xFFFE, 4, 0xFFFFFFFC, 0x80000001),
        (0xFFF8, 0x10, 0xFFFFFFC0, 0x80000002),
        (0xFFE0, 0x40, 0xFFFFFC00, 0x80000004),
        (0xFF80, 0x100, 0xFFFFC000, 0x80000008),
        (0xFE00, 0x400, 0xFFFC0000, 0x80000010),
        (0xF800, 0x1000, 0xFFC00000, 0x80000020),
        (0xE000, 0x4000, 0xFC000000, 0x80000040),
    ):
        if (b_ & m16) - (a & m16) < lim and (d & m16) - (c & m16) < lim:
            return ((lon_min | lat_min) & m32) | flag
    return 0x80000080


def index_level(idx):
    """0 = single tile, 1..7 = compound levels 0x01..0x40, 8 = planet-wide (0x80000080)."""
    if not idx & 0x80000000:
        return 0
    for i, (flag, *_rest) in enumerate(_LEVELS, start=1):
        if idx & flag:
            return i
    return 8


# ---- bbox -> ranges ----------------------------------------------------------

def _rec(south, north, west, east, bitlevel, out):
    dist = ((0xFFFF0000 >> bitlevel) & 0xFFFF0000)
    if ((south + dist) & M32) == north and ((west + dist) & M32) == east:
        out.append((ll_upper_(south, west), ll_upper_(north, east) + 1))
        return
    dist = (dist >> 1) & 0xFFFF0000
    ns = (north | dist) != (south | dist)
    ew = (east | dist) != (west | dist)
    if ns and ew:
        _rec(south, south | dist, west, west | dist, bitlevel + 1, out)
        _rec(south, south | dist, ((west | dist) + 0x10000) & M32, east, bitlevel + 1, out)
        _rec(((south | dist) + 0x10000), north, west, west | dist, bitlevel + 1, out)
        _rec(((south | dist) + 0x10000), north, ((west | dist) + 0x10000) & M32, east, bitlevel + 1, out)
    elif ns:
        _rec(south, south | dist, west, east, bitlevel + 1, out)
        _rec(((south | dist) + 0x10000), north, west, east, bitlevel + 1, out)
    elif ew:
        _rec(south, north, west, west | dist, bitlevel + 1, out)
        _rec(south, north, ((west | dist) + 0x10000) & M32, east, bitlevel + 1, out)
    else:
        _rec(south, north, west, east, bitlevel + 1, out)


def _merge(ranges):
    ranges = sorted(ranges)
    out = []
    for lo, hi in ranges:
        if out and lo <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], hi))
        else:
            out.append((lo, hi))
    return out


def calc_ranges(south, north, west, east):
    """Half-open [lo, hi) ranges of node indexes touched by the bbox. West<=east, east>=0 handled;
    antimeridian-crossing boxes are out of scope for the spike."""
    s, n = ilat(south) & 0xFFFF0000, ilat(north) & 0xFFFF0000
    w, e = ilon(west) & 0xFFFF0000 & M32, ilon(east) & 0xFFFF0000 & M32
    out = []
    if west < 0 <= east:
        _rec(s, n, w, 0xFFFF0000, 1, out)
        _rec(s, n, 0, e, 1, out)
    elif west >= 0 or east < 0:
        _rec(s, n, w, e, 1, out)
    return _merge(out)


def _decomp(lo, hi):
    out = []
    while lo < hi:
        step = 1
        while step < 0x40000000 and (lo & (step * 4 - 1)) == 0 and lo + step * 4 <= hi:
            step *= 4
        out.append((lo, lo + step))
        lo += step
    return out


def key31(v):
    """Sort key matching Uint31_Index::operator< : low 31 bits first, then the compound flag bit."""
    return ((v & 0x7FFFFFFF) << 1) | (v >> 31)


def _inc31(v):
    return (v & 0x7FFFFFFF) + 1  # Uint31_Index::inc, used by push_back_interval


def merge31(ranges):
    """Merge [lo, hi) ranges of Uint31 indexes under Overpass ordering; returns ranges in key31 space."""
    return _merge([(key31(lo), key31(hi)) for lo, hi in ranges])


def _blur(distance, bitmask, lower_idx, upper_idx, out):
    sbm = 0x10000 - distance
    min_lat = upper_ilat(lower_idx) & sbm
    min_lon = upper_ilon(lower_idx) & sbm
    max_lat = upper_ilat(upper_idx) & sbm
    max_lon = upper_ilon(upper_idx) & sbm
    for lat in range(min_lat - distance, max_lat + 1, distance):
        v = ll_upper((lat & 0xFFFF) << 16, ((min_lon - distance) & 0xFFFF) << 16) | bitmask
        out.append((v, _inc31(v)))
    for lon in range(min_lon, max_lon + 1, distance):
        v = ll_upper(((min_lat - distance) & 0xFFFF) << 16, lon << 16) | bitmask
        out.append((v, _inc31(v)))
    v = ll_upper(max_lat << 16, max_lon << 16) | bitmask
    out.append((v, _inc31(v)))


_BLUR = [(0x7FFFFFFC, 2, 0x80000001), (0x7FFFFFC0, 8, 0x80000002), (0x7FFFFC00, 0x20, 0x80000004),
         (0x7FFFC000, 0x80, 0x80000008), (0x7FFC0000, 0x200, 0x80000010), (0x7FC00000, 0x800, 0x80000020),
         (0x7C000000, 0x2000, 0x80000040)]


def calc_parents(node_ranges):
    """Way/relation index ranges that may contain objects touching the given node ranges.
    Returned as merged [lo, hi) ranges over key31(idx)."""
    out = [(0x80000080, 0x80000081)]
    for lo, hi in node_ranges:
        for a, b in _decomp(lo, hi):
            out.append((a, b))
            for mask, dist, bm in _BLUR:
                _blur(dist, bm, a & mask, (b - 1) & mask, out)
    return merge31(out)
