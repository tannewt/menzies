include "data.thrift"

service NodeServer {
  data.Node getNode(1: i64 id),
	data.Node getNodeVersion(1: i64 id, 2: i32 version)
	data.Osm getFullNode(1: i64 id),
	list<data.Node> getNodes(1: list<i64> ids),
	list<data.Node> getNodesFromNode(1: i64 node),
	list<data.Node> getNodesFromRelation(1: i64 relation),
	i32 editNode(1: data.Node Node),
	i32 deleteNode(1: data.Node Node),
	i64 createNode(1: data.Node Node),
	list<data.Node> NodeHistory(1: i64 id)
}

