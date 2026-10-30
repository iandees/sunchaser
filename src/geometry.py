"""Overpass's geometry primitives, in one place (core/geometry.cc, core/basic_types.h).

Python is the reference; the DuckDB macros in engine.SQL_MACROS (ov_lat, ov_seg) repeat the same
formulas in SQL and must change together with this file.
"""
import math
from dataclasses import dataclass

NO_COORD = -2147483648  # a node missing from the extract, in a way's nd_lat / nd_lon

DEG_TO_ARC = math.acos(0) / 90.0
ARC_TO_METER = 10 * 1000 * 1000 / math.acos(0)  # Overpass's earth: a 10,000 km quarter meridian


def ov_lat(lat_e7):
    """Latitude as Overpass's ::lat() computes it (ilat / 1e7 - 91.0): on bbox edges the rounding
    differs from lat_e7 / 1e7 and decides membership."""
    return (lat_e7 + 910000000) / 1e7 - 91.0


@dataclass
class Bbox:
    s: float
    w: float
    n: float
    e: float

    def ints(self):
        """Integer bounds the way filter_by_bbox compares node coordinates (ilat_/ilon_ rounding)."""
        to7 = lambda v: int(v * 10000000 + (0.5 if v > 0 else -0.5))
        return (int(round((self.s + 91.0) * 10000000 + 0.5)) - 910000000, to7(self.w),
                int(round((self.n + 91.0) * 10000000 + 0.5)) - 910000000, to7(self.e))

    def intersect(self, other):
        """The overlap with other (empty when s > n or w > e)."""
        return Bbox(max(self.s, other.s), max(self.w, other.w), min(self.n, other.n), min(self.e, other.e))


def segment_intersects(flat, flon, tlat, tlon, box):
    """Bbox_Double::intersects (core/geometry.cc) for the segment (flat, flon)-(tlat, tlon) and a box
    with west <= east, no antimeridian handling. Latitudes are already ov_lat-rounded."""
    s, n, w, e = box.s, box.n, box.w, box.e
    if (flat < s and tlat < s) or (flat > n and tlat > n):
        return False
    df = (tlon - flon) * (s - flat) / (tlat - flat) if flat < s else (tlon - flon) * (n - flat) / (tlat - flat) if flat > n else 0
    dt = (flon - tlon) * (s - tlat) / (flat - tlat) if tlat < s else (flon - tlon) * (n - tlat) / (flat - tlat) if tlat > n else 0
    lo, hi = sorted((flon + df, tlon + dt))
    return lo <= e and hi >= w


def great_circle_dist(lat1, lon1, lat2, lon2):
    """core/geometry.cc great_circle_dist, exactly."""
    if lat1 == lat2 and lon1 == lon2:
        return 0.0
    sp = (math.cos((lat2 - lat1) * DEG_TO_ARC)
          + math.cos(lat1 * DEG_TO_ARC) * math.cos(lat2 * DEG_TO_ARC) * (math.cos((lon2 - lon1) * DEG_TO_ARC) - 1))
    return math.acos(min(1.0, sp)) * ARC_TO_METER
