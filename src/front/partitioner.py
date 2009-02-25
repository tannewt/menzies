class NodePartitioner:
	def __init__(self, servers):
		"""Setup the partition over these servers."""
		self._servers = servers
	
	def from_node_id(self, id):
		"""Return the servers to look for node on."""
		# look in them all
		return self._servers
	
	def from_node(self, node):
		"""Return the servers to look for node on."""
		# look in them all
		return self._servers
	
	def from_box(self, box):
		"""Return the servers to look for nodes in the box."""
		# look in them all
		return self._servers

class IdHashPartitioner (NodePartitioner):
	def from_node_id(self, id):
		return self._servers[id % len(self._servers)]
	
	def from_node(self, node):
		return self.from_node_id(node.id)

class StaticLatPartitioner (NodePartitioner):
	def __init__(self, servers):
		NodePartitioner.__init__(self, servers)
		self._bounds = [0, 30, 60, -45][:len(servers)-1]
		self._bounds.sort()
		print self._bounds

	def from_node(self, node):
		i = 0
		while i < len(self._bounds) and node.lat > self._bounds[i]:
			i += 1
		return [self._servers[i]]
