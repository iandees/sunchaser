"""Spike query corpus: Overpass QL (kept verbatim so it can be replayed against a real Overpass)
plus the equivalent program against engine.Engine. Bboxes stay inside the BBBike Berlin extract.
"""
from engine import Bbox, Filter as F

MITTE = Bbox(52.510, 13.380, 52.525, 13.410)          # ~1.6 x 2.0 km
KREUZBERG = Bbox(52.490, 13.390, 52.505, 13.430)
TINY = Bbox(52.5160, 13.3770, 52.5175, 13.3800)        # Pariser Platz, ~170 x 200 m
CITY = Bbox(52.45, 13.30, 52.56, 13.48)                # inner city, ~12 x 12 km
WIDE = Bbox(52.35, 13.10, 52.68, 13.75)                # most of Berlin


def b(x):
    return f"({x.s},{x.w},{x.n},{x.e})"


CORPUS = [
    ("cafes_mitte", f'node[amenity=cafe]{b(MITTE)};',
     lambda E: E.query("n", MITTE, [F("=", "amenity", "cafe")])),
    ("highways_mitte", f'way[highway]{b(MITTE)};',
     lambda E: E.query("w", MITTE, [F("exists", "highway")])),
    ("bakeries_nwr", f'nwr[shop=bakery]{b(KREUZBERG)};',
     lambda E: E.query("nwr", KREUZBERG, [F("=", "shop", "bakery")])),
    ("all_nodes_tiny", f'node{b(TINY)};',
     lambda E: E.query("n", TINY)),
    ("all_nwr_tiny", f'nwr{b(TINY)};',
     lambda E: E.query("nwr", TINY)),
    ("buildings_city", f'way[building]{b(CITY)};',
     lambda E: E.query("w", CITY, [F("exists", "building")])),
    ("primary_secondary_regex", f'way[highway~"^(primary|secondary)$"]{b(CITY)};',
     lambda E: E.query("w", CITY, [F("~", "highway", "^(primary|secondary)$")])),
    ("name_regex_icase", f'node[name~"bäcker",i]{b(CITY)};',
     lambda E: E.query("n", CITY, [F("~", "name", "bäcker", icase=True)])),
    ("drinking_water_global", 'node[amenity=drinking_water];',
     lambda E: E.query("n", None, [F("=", "amenity", "drinking_water")])),
    ("cafes_global", 'node[amenity=cafe];',
     lambda E: E.query("n", None, [F("=", "amenity", "cafe")])),
    ("subway_routes_down", 'rel[route=subway];(._;>;);',
     lambda E: (lambda r: E.union(r, E.recurse(r, ">")))(E.query("r", None, [F("=", "route", "subway")]))),
    ("rail_not_main", f'way[railway=rail][usage!=main]{b(WIDE)};',
     lambda E: E.query("w", WIDE, [F("=", "railway", "rail"), F("!=", "usage", "main")])),
    ("node_ids", 'node(id:30353086,253846340,322796388,2812237303,13254184468);',
     lambda E: E.query("n", ids=[30353086, 253846340, 322796388, 2812237303, 13254184468])),
    ("way_ids_down", 'way(id:50637691,518071791,556435241);(._;>;);',
     lambda E: (lambda w: E.union(w, E.recurse(w, ">")))(E.query("w", ids=[50637691, 518071791, 556435241]))),
    ("crossings_up", f'node[highway=crossing]{b(MITTE)};<;',
     lambda E: E.recurse(E.query("n", MITTE, [F("=", "highway", "crossing")]), "<")),
    ("admin10_rels_bbox", f'rel[boundary=administrative][admin_level=10]{b(MITTE)};',
     lambda E: E.query("r", MITTE, [F("=", "boundary", "administrative"), F("=", "admin_level", "10")])),
    ("named_street_down", 'way[name="Unter den Linden"];(._;>;);',
     lambda E: (lambda w: E.union(w, E.recurse(w, ">")))(E.query("w", None, [F("=", "name", "Unter den Linden")]))),
    ("park_multipolygons_downrel", f'rel[type=multipolygon][leisure=park]{b(CITY)};>>;',
     lambda E: E.recurse(E.query("r", CITY, [F("=", "type", "multipolygon"), F("=", "leisure", "park")]), ">>")),
    ("signals_uprel", f'node[highway=traffic_signals]{b(TINY)};<<;',
     lambda E: E.recurse(E.query("n", TINY, [F("=", "highway", "traffic_signals")]), "<<")),
    ("highway_minus_footway", f'(way[highway]{b(MITTE)}; - way[highway=footway]{b(MITTE)};);',
     lambda E: E.difference(E.query("w", MITTE, [F("exists", "highway")]),
                            E.query("w", MITTE, [F("=", "highway", "footway")]))),
    ("unnamed_residential", f'way[highway=residential][!name]{b(CITY)};',
     lambda E: E.query("w", CITY, [F("=", "highway", "residential"), F("!exists", "name")])),
    ("addr_key_regex", f'nwr[~"^addr:"~"."]{b(TINY)};',
     lambda E: E.query("nwr", TINY, [F("k~v", "^addr:", ".")])),
    ("route_master_downrel", 'rel(id:51942,58423);>>;',
     lambda E: E.recurse(E.query("r", ids=[51942, 58423]), ">>")),
    ("ring_rel_up", 'rel(14981);<<;',
     lambda E: E.recurse(E.query("r", ids=[14981]), "<<")),
]
