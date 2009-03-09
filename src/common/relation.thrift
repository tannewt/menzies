include "data.thrift"

service RelationServer {
	void ping()
	data.Relation getRelation(1: i64 id),
	data.Relation getRelationVersion(1: i64 id, 2: i32 version)
	list<data.Relation> getRelations(1: list<i64> ids),
	list<data.Relation> getRelationsFromNode(1: i64 node),
	list<data.Relation> getRelationsFromWay(1: i64 way),
	list<data.Relation> getRelationsFromRelation(1: i64 relation),
	i32 editRelation(1: data.Relation relation),
	i32 deleteRelation(1: data.Relation relation),
	i64 createRelation(1: data.Relation relation),
	list<data.Relation> getRelationHistory(1: i64 id)
}

