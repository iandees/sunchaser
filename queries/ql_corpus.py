"""Full Overpass QL requests for differential testing against a reference Overpass.

The first block reuses the engine corpus (queries/corpus.py) with `out meta;`, which exercises ids,
coordinates, tags, node lists, members and metadata. The second block targets output modes, formats
and statements the engine corpus does not cover.
"""
from corpus import CORPUS, CITY, KREUZBERG, MITTE, TINY, b

QL = [(f"meta:{name}", f"[out:json];{ql} out meta;") for name, ql in CORPUS]

QL += [
    ("turbo_default",
     f'[out:json][timeout:25];(node["amenity"="drinking_water"]{b(MITTE)};way["amenity"="drinking_water"]{b(MITTE)};'
     f'relation["amenity"="drinking_water"]{b(MITTE)};);out body;>;out skel qt;'),
    ("xml_default", f"node[amenity=cafe]{b(MITTE)};out;"),
    ("out_geom_ways", f"[out:json];way[highway=primary]{b(MITTE)};out geom;"),
    ("out_geom_rel", "[out:json];rel(7643526);out geom;"),
    ("out_bb_rels", f"[out:json];rel[route=bus]{b(TINY)};out bb;"),
    ("out_center_nwr", f"[out:json];nwr[amenity=restaurant]{b(TINY)};out center;"),
    ("out_ids", f"[out:json];way[building]{b(TINY)};out ids;"),
    ("out_skel", f"[out:json];way[building]{b(TINY)};out skel;"),
    ("out_tags", f"[out:json];nwr[tourism]{b(TINY)};out tags;"),
    ("out_count", f"[out:json];nwr[amenity]{b(MITTE)};out count;"),
    ("out_limit_qt", f"[out:json];node[amenity=bench]{b(CITY)};out body qt 25;"),
    ("out_noids", f"[out:json];node[amenity=cafe]{b(TINY)};out noids;"),
    ("csv_fields", f'[out:csv(::id,::type,::lat,::lon,name,"addr:street";true;",")];nwr[amenity=cafe]{b(MITTE)};out center;'),
    ("csv_meta", f"[out:csv(::id,::version,::timestamp,::user,::uid,::changeset)];node[shop=bakery]{b(KREUZBERG)};out meta;"),
    ("xml_geom", f"way[highway=primary]{b(TINY)};out geom;"),
    ("xml_meta_rel", "rel(14981);out meta;"),
    ("global_bbox", f"[out:json][bbox:{MITTE.s},{MITTE.w},{MITTE.n},{MITTE.e}];node[amenity=cafe];out ids;"),
    ("named_sets", f"[out:json];way[highway=primary]{b(MITTE)}->.p;node(w.p)->.n;.n out ids;.p out ids;"),
    ("recurse_filter_r", "[out:json];rel(14981);node(r)->.stops;.stops out ids;way(r);out ids;"),
    ("recurse_filter_role", '[out:json];rel(7643526);way(r:"outer");out ids;'),
    ("recurse_filter_bn", f"[out:json];node[highway=traffic_signals]{b(TINY)};way(bn);out ids;"),
    ("recurse_filter_bw", f"[out:json];way[highway=primary]{b(TINY)};rel(bw);out ids;"),
    ("recurse_filter_br", "[out:json];rel(14981);rel(br);out ids;"),
    ("input_set_filter", f"[out:json];nwr{b(TINY)}->.all;node.all[amenity];out ids;"),
    ("around_point", "[out:json];node(around:100,52.5163,13.3777)[tourism];out ids;"),
    ("around_point_ways", "[out:json];way(around:30,52.5163,13.3777)[highway];out ids;"),
    ("around_set", f"[out:json];node[amenity=cafe]{b(TINY)}->.c;node(around.c:50)[amenity=bench];out ids;"),
    ("difference_block", f"[out:json];(way[highway]{b(TINY)}; - way[highway=footway]{b(TINY)};);out ids;"),
    ("union_named", f"[out:json];(node[amenity=cafe]{b(TINY)};node[amenity=restaurant]{b(TINY)};)->.food;.food out ids;"),
    ("recurse_up_down", f"[out:json];node[highway=crossing]{b(TINY)};<;>;out ids;"),
    ("regex_value_icase", f'[out:json];node[name~"^berlin",i]{b(MITTE)};out ids;'),
    ("key_regex", f'[out:json];way[~"^name:"~"."]{b(TINY)};out ids;'),
    ("not_equal_missing", f"[out:json];way[highway][bridge!=yes]{b(TINY)};out ids;"),
    ("poly_nodes", '[out:json];node(poly:"52.515 13.375 52.520 13.380 52.518 13.390")[amenity];out ids;'),
    ("poly_ways", '[out:json];way(poly:"52.515 13.375 52.520 13.380 52.518 13.390")[highway];out ids;'),
    ("poly_rels", '[out:json];rel(poly:"52.515 13.375 52.520 13.380 52.518 13.390")[type=route];out ids;'),
    ("multiple_outs", f"[out:json];node[amenity=cafe]{b(TINY)};out ids;out count;out tags;"),
]

# Areas: the reference needs its area rules run first (osm3s_query --rules < rules/areas.osm3s).
QL_AREAS = [
    ("area_by_tags", '[out:json];area[name="Mitte"][admin_level];out;'),
    ("area_nodes", '[out:json];area[name="Mitte"][admin_level=10]->.a;node(area.a)[amenity=cafe];out ids;'),
    ("area_ways", '[out:json];area[name="Mitte"][admin_level=10]->.a;way(area.a)[highway=primary];out ids;'),
    ("area_nwr_turbo", '[out:json][timeout:25];area(id:3600016566)->.searchArea;'
                       'nwr["amenity"="drinking_water"](area.searchArea);out geom;'),
    ("area_rels", '[out:json];area[name="Kreuzberg"][admin_level=10]->.a;rel(area.a)[type=multipolygon];out ids;'),
    ("map_to_area", "[out:json];rel(16566);map_to_area;out ids;"),
    ("is_in_point", "[out:json];is_in(52.5163,13.3777);out ids;"),
    ("is_in_set", "[out:json];node(253846340);is_in;out ids;"),
    ("pivot", '[out:json];area[name="Mitte"][admin_level=10];rel(pivot);out ids;'),
    ("area_count", '[out:json];area[admin_level=10];out count;'),
]
QL += QL_AREAS

QL_EVAL = [
    ("if_length", "[out:json];way[highway=primary](52.51,13.38,52.525,13.41)(if: length() > 100);out ids;"),
    ("if_count_tags", '[out:json];node[amenity](52.515,13.385,52.52,13.395)(if: count_tags() > 10 && t["amenity"] == "cafe");out ids;'),
    ("if_closed_levels", '[out:json];way[building](52.516,13.377,52.5175,13.38)(if: is_closed() && number(t["building:levels"]) >= 5);out ids;'),
    ("if_count_members", "[out:json];rel[type=route](52.51,13.38,52.525,13.41)(if: count_members() > 100);out ids;"),
    ("foreach_around", "[out:json];node[amenity=cafe](52.5160,13.3770,52.5175,13.3800);foreach->.c(node(around.c:30)[amenity=bench];out ids;);"),
]
QL += QL_EVAL

QL += [
    ("out_geom_clip_ways", "[out:json];way[highway=primary](52.515,13.385,52.52,13.395);out geom(52.516,13.387,52.519,13.392);"),
    ("out_geom_clip_rel", "[out:json];rel(7643526);out geom(52.512,13.35,52.516,13.36);"),
    ("xml_geom_clip", "way[highway=primary](52.515,13.385,52.52,13.395);out geom(52.516,13.387,52.519,13.392);"),
]

# History (attic) queries. They need a reference loaded with --keep-attic and both sides replayed over
# the same diffs (tools/compare_overpass.py --history). Times assume the extract snapshot of
# 2026-10-24T00:00:00Z replayed to 2026-10-25T00:00:00Z.
T_MID, T_LATE, T_OLD = "2026-10-24T12:00:00Z", "2026-10-24T20:00:00Z", "2026-09-01T00:00:00Z"
# Overpass 0.7.62's attic queries report an elapsed time of hundreds of hours and time out at once;
# a huge [timeout] sidesteps that (it changes nothing else).
QL_HISTORY = [
    ("date_nodes_mid", f'[out:json][timeout:1000000000][date:"{T_MID}"];node[amenity]{b(MITTE)};out meta;'),
    ("date_ways_mid", f'[out:json][timeout:1000000000][date:"{T_MID}"];way[highway]{b(MITTE)};out meta;'),
    ("date_ways_geom", f'[out:json][timeout:1000000000][date:"{T_LATE}"];way[building]{b(TINY)};out geom;'),
    ("date_rels_mid", f'[out:json][timeout:1000000000][date:"{T_MID}"];rel[type=route]{b(KREUZBERG)};out ids;'),
    ("date_before_snapshot", f'[out:json][timeout:1000000000][date:"{T_OLD}"];nwr[shop]{b(MITTE)};out ids;'),
    ("date_recurse", f'[out:json][timeout:1000000000][date:"{T_MID}"];way[highway=primary]{b(MITTE)};>;out ids;'),
    ("date_xml_meta", f'[timeout:1000000000][date:"{T_LATE}"];node[shop]{b(KREUZBERG)};out meta;'),
    ("newer_nodes", f'[out:json];node(newer:"{T_MID}"){b(CITY)};out ids;'),
    ("newer_ways_tagged", f'[out:json];way[highway](newer:"{T_MID}"){b(CITY)};out ids;'),
    ("newer_rels", f'[out:json];rel(newer:"{T_MID}"){b(CITY)};out ids;'),
    ("changed_nodes", f'[out:json];node(changed:"{T_MID}","{T_LATE}"){b(CITY)};out ids;'),
    ("changed_ways", f'[out:json];way(changed:"2026-10-24T00:00:00Z","2026-10-25T00:00:00Z"){b(CITY)};out ids;'),
    ("changed_date", f'[out:json][timeout:1000000000][date:"{T_LATE}"];nwr(changed){b(CITY)};out ids;'),
]
