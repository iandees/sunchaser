"""Spike query corpus: Overpass QL (kept verbatim so it can be replayed against a real Overpass;
queries/ql_corpus.py wraps each in a full request). Bboxes stay inside the BBBike Berlin extract.
"""
from dataclasses import dataclass


@dataclass
class Bbox:
    s: float
    w: float
    n: float
    e: float

MITTE = Bbox(52.510, 13.380, 52.525, 13.410)          # ~1.6 x 2.0 km
KREUZBERG = Bbox(52.490, 13.390, 52.505, 13.430)
TINY = Bbox(52.5160, 13.3770, 52.5175, 13.3800)        # Pariser Platz, ~170 x 200 m
CITY = Bbox(52.45, 13.30, 52.56, 13.48)                # inner city, ~12 x 12 km
WIDE = Bbox(52.35, 13.10, 52.68, 13.75)                # most of Berlin


def b(x):
    return f"({x.s},{x.w},{x.n},{x.e})"


CORPUS = [
    ("cafes_mitte", f'node[amenity=cafe]{b(MITTE)};'),
    ("highways_mitte", f'way[highway]{b(MITTE)};'),
    ("bakeries_nwr", f'nwr[shop=bakery]{b(KREUZBERG)};'),
    ("all_nodes_tiny", f'node{b(TINY)};'),
    ("all_nwr_tiny", f'nwr{b(TINY)};'),
    ("buildings_city", f'way[building]{b(CITY)};'),
    ("primary_secondary_regex", f'way[highway~"^(primary|secondary)$"]{b(CITY)};'),
    ("name_regex_icase", f'node[name~"bäcker",i]{b(CITY)};'),
    ("drinking_water_global", 'node[amenity=drinking_water];'),
    ("cafes_global", 'node[amenity=cafe];'),
    ("subway_routes_down", 'rel[route=subway];(._;>;);'),
    ("rail_not_main", f'way[railway=rail][usage!=main]{b(WIDE)};'),
    ("node_ids", 'node(id:30353086,253846340,322796388,2812237303,13254184468);'),
    ("way_ids_down", 'way(id:50637691,518071791,556435241);(._;>;);'),
    ("crossings_up", f'node[highway=crossing]{b(MITTE)};<;'),
    ("admin10_rels_bbox", f'rel[boundary=administrative][admin_level=10]{b(MITTE)};'),
    ("named_street_down", 'way[name="Unter den Linden"];(._;>;);'),
    ("park_multipolygons_downrel", f'rel[type=multipolygon][leisure=park]{b(CITY)};>>;'),
    ("signals_uprel", f'node[highway=traffic_signals]{b(TINY)};<<;'),
    ("highway_minus_footway", f'(way[highway]{b(MITTE)}; - way[highway=footway]{b(MITTE)};);'),
    ("unnamed_residential", f'way[highway=residential][!name]{b(CITY)};'),
    ("addr_key_regex", f'nwr[~"^addr:"~"."]{b(TINY)};'),
    ("route_master_downrel", 'rel(id:51942,58423);>>;'),
    ("ring_rel_up", 'rel(14981);<<;'),
]
