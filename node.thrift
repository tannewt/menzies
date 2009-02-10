service NodeServer {
  getNode(i64 id, optional version),
  editNode(Node node),
  deleteNode(None node),
  createNode(Node node),
  nodeHistory(i64 id),
}
