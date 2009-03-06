# -*- coding: utf-8 -*-
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

	def get_node_id_sets(self, node_ids):
		sets = dict.fromkeys(self._servers, [])
		for id in node_ids:
			sets[self.from_node_id(id)].append(id)
		return sets

class IdHashPartitioner (NodePartitioner):
	def from_node_id(self, id):
		return self._servers[id % len(self._servers)]
	
	def from_node(self, node):
		return self.from_node_id(node.id)

class StaticLatPartitioner (NodePartitioner):
	def __init__(self, servers):
		NodePartitioner.__init__(self, servers)
		self._bounds = [25, 49][:len(servers)-1]
		self._bounds.sort()
		print self._bounds

	def from_node(self, node):
		i = 0
		while i < len(self._bounds) and node.lat > self._bounds[i]:
			i += 1
		return [self._servers[i]]
	
	def from_box(self, box):
		i = 0
		while i < len(self._bounds) and box.min_lat >= self._bounds[i]:
			i += 1
		
		start = i
		while i < len(self._bounds) and box.max_lat > self._bounds[i]:
			i += 1
		end = i
		
		
		return self._servers[start:end+1]

if __name__=="__main__":
	part = StaticLatPartitioner(range(3))
	class o:
		pass
	o.min_lat = -10
	o.max_lat = 10
	print "[0]",part.from_box(o)
	
	class o:
		pass
	o.min_lat = 24
	o.max_lat = 45
	print "[0, 1]",part.from_box(o)
	
	class o:
		pass
	o.min_lat = 25
	o.max_lat = 45
	print "[1]",part.from_box(o)
	
	class o:
		pass
	o.min_lat = -90
	o.max_lat = 90
	print "[0, 1, 2]",part.from_box(o)
	
	class o:
		pass
	o.min_lat = 50
	o.max_lat = 90
	print "[2]",part.from_box(o)
