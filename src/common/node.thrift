include "data.thrift"

service NodeServer {
	void ping()
	list<data.Node> getNodesInBounds(1: data.BBox bounds)
	data.Node getNode(1: i64 id),
	data.Node getNodeVersion(1: i64 id, 2: i32 version)
	list<data.Node> getNodes(1: list<i64> ids),
	i32 editNode(1: data.Node Node),
	i32 deleteNode(1: i64 id),
	i64 createNode(1: data.Node Node),
	list<data.Node> getNodeHistory(1: i64 id)
}

