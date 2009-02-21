include "data.thrift"

service WayServer {
	data.Way getWay(1: i64 id),
	data.Way getWayVersion(1: i64 id, 2: i32 version)
	list<data.Way> getWays(1: list<i64> ids),
	list<data.Way> getWaysFromNode(1: i64 node),
	i32 editWay(1: data.Way way),
	i32 deleteWay(1: i64 id),
	i64 createWay(1: data.Way way),
	list<data.Way> getWayHistory(1: i64 id)
}

