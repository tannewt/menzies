include "data.thrift"

service OpenStreetMap {
  data.Node getNode(1: i64 id, 2: optional i32 version),
  i32 editNode(1: data.Node node),
  i32 deleteNode(1:data.Node node),
  i64 createNode(1:data.Node node),
  list<data.Node> nodeHistory(1: i64 id),
  
  data.Way getWay(1: i64 id, 2: optional i32 version, 3: optional bool full),
  list<data.Way> getWays(1: optional list<i64> ids, 2: optional i64 node, 3: optional i64 relation),
  i32 editWay(1: data.Way way),
  i32 deleteWay(1: data.Way way),
  i64 createWay(1: data.Way way),
  list<data.Way> wayHistory(1: i64 id),
  
  data.Relation getRelation(1: i64 id, 2: optional i32 version),
  data.Osm getFullRelation(1: i64 id, 2: optional bool full),
  list<data.Relation> getRelations(1: optional list<i64> ids, 2: optional i64 node, 3: optional i64 way),
  i32 editRelation(1: data.Relation rel),
  i32 deleteRelation(1: data.Relation rel),
  i64 createRelation(1: data.Relation rel),
  list<data.Relation> relationHistory(1: i64 id),
  
  data.Changeset getChangeset(1: i64 id),
  list<data.Changeset> getChangesets(1: optional data.BBox box, 2: optional i64 uid, 3: optional data.TimeRange range),
  void editChangeset(1: data.Changeset cs),
  i64 createChangeset(1: data.Changeset cs),
  data.OsmChange putChanges(1: i64 id, 2: data.OsmChange changes),
  data.OsmChange getChanges(1: i64 id),
  
  data.Osm getAll(1: data.BBox bounds)//,
  
  //getTrackpoints
  //createGpx
  //gpxDetails
  //gpxData
}
