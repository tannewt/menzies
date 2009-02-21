// based on http://wiki.openstreetmap.org/wiki/OSM_Protocol_Version_0.6/DTD

struct Home {
	1: double lat,
	2: double lon,
	3: i16 zoom
}

struct User {
	1: string display_name,
	2: i64 account_created,
	3: optional Home home
}

struct Preferences {
	1: map<string,string> prefs
}

struct GPX {
	1: i64 id,
	2: string name,
	3: double lat,
	4: double lon,
	5: string user,
	6: i32 uid,
	7: bool _public,
	8: bool pending,
	9: i64 timestamp
}

struct API {
	1: double ver_min,
	2: double ver_max,
	3: double area_max,
	4: i32 tp_page,
	5: i32 waynodes_max
}

struct Node {
	1: i64 id,
	2: double lat,
	3: double lon,
	4: i64 changeset,
	5: bool visible,
	6: string user,
	7: i32 uid,
	8: i32 version,
	9: i64 timestamp,
	10:map<string,string> tags
}

struct Way {
	1: i64 id,
	2: i64 changeset,
	3: bool visible,
	4: string user,
	5: i64 uid,
	6: i32 version,
	7: i64 timestamp,
	8: map<string,string> tags,
	9: list<i64> nodes
}

struct Member {
	1: string role,
	2: optional i64 node,
	3: optional i64 way,
	4: optional i64 relation
}

struct Relation {
	1: i64 id,
	2: i64 changeset,
	3: bool visible,
	4: string user,
	5: i64 uid,
	6: i32 version,
	7: i64 timestamp,
	8: set<Member> members
	9: map<string,string> tags,
}

struct BBox {
	1: double min_lat,
	2: double max_lat,
	3: double min_lon,
	4: double max_lon
}

struct Changeset {
	1: i64 id,
	2: string user,
	3: i64 uid,
	4: i64 created,
	5: i64 closed,
	6: bool open,
	7: BBox bounds,
	8:map<string,string> tags
}

struct TimeRange {
	1: optional i64 start,
	2: optional i64 end
}

struct Osm {
	1: optional list<Node> nodes,
	2: optional list<Way> ways,
	3: optional list<Relation> relations
}

enum Op {
	CREATE,
	MODIFY,
	DELETE
}

struct Change {
	1: Op op,
	2: optional Node node,
	3: optional Way way,
	4: optional Relation relation
}

struct OsmChange {
	1: set<Change> changes
}
