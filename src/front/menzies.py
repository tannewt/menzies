from node import NodeServer
from way import WayServer
from relation import RelationServer

from data.ttypes import *

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

class Menzies:
	SERVERS = {"node": [("localhost", 9091)]}
	
	def __init__(self):
		self.servers = {"node":[]}
		for s in self.SERVERS["node"]:
			transport = TSocket.TSocket('localhost', 9091)
			transport = TTransport.TBufferedTransport(transport)
			protocol = TBinaryProtocol.TBinaryProtocol(transport)
			client = NodeServer.Client(protocol)
			transport.open()
			self.servers["node"].append(client)

		transport = TSocket.TSocket('localhost', 9090)
		transport = TTransport.TBufferedTransport(transport)
		protocol = TBinaryProtocol.TBinaryProtocol(transport)
		client = WayServer.Client(protocol)
		transport.open()
		self.servers["way"] = client

		transport = TSocket.TSocket('localhost', 9092)
		transport = TTransport.TBufferedTransport(transport)
		protocol = TBinaryProtocol.TBinaryProtocol(transport)
		client = RelationServer.Client(protocol)
		transport.open()
		self.servers["relation"] = client

		self.next_node_id = 0
	
	def getNode(self,id):
		for s in self.servers["node"]:
			try:
				n = s.getNode(id)
				return n
			except:
				pass
		return None
	
	def getNodeVersion(self, id, version):
		for s in self.servers["node"]:
			try:
				n = s.getNodeVersion(id, version)
				return n
			except:
				pass
		return None
	
	def editNode(self, node):
		for s in self.servers["node"]:
			try:
				n = s.editNode(node)
				return n
			except:
				pass
		return None
	
	def deleteNode(self, node):
		for s in self.servers["node"]:
			try:
				n = s.deleteNode(node)
				return n
			except:
				pass
		return None
	
	def createNode(self, node):
		node.id = self.next_node_id
		self.next_node_id+=1
		
		self.servers["node"][0].createNode(node)
		return node.id
	
	def getNodeHistory(self, id):
		for s in self.servers["node"]:
			try:
				n = s.getNodeHistory(id)
				return n
			except:
				pass
		return None

	def getWaysFromNode(self, id):
		try:
			ways = self.servers["way"].getWaysFromNode(id)
			return ways
		except:
			pass
		return None

	def getWay(self, id):
		try:
			way = self.servers["way"].getWay(id)
			return way
		except:
			pass
		return None

	def getWayVersion(self, id, version):
		try:
			w = self.servers["way"].getWayVersion(id, version)
			return w
		except:
			pass
		return None
	
	def editWay(self, way):
		try:
			w = self.servers["way"].editNode(way)
			return w
		except:
			pass
		return None
	
	def deleteWay(self, way):
		try:
			w = self.servers["way"].deleteWay(way)
			return w
		except:
			pass
		return None
	
	def createWay(self, way):
		way.id = self.next_way_id
		self.next_way_id+=1
		
		self.servers["way"].createNode(way)
		return way.id
	
	def getWayHistory(self, id):
		try:
			w = self.servers["way"].getWayHistory(id)
			return w
		except:
			pass
		return None

	def getWayFull(self, id):
		try:
			osm = Osm()
			osm.ways = [self.servers["way"].getWay(id)]
			osm.nodes = []
			for node_id in osm.ways[0].nodes:
				osm.nodes.append(self.servers["node"][0].getNode(node_id))
			return osm
		except TApplicationException:
			pass
		return None
	def getRelationsFromNode(self, id):
		try:
			relations = self.servers["relation"].getRelationsFromNode(id)
			return relations
		except TApplicationException:
			pass
		return None

	def getRelationsFromWay(self, id):
		try:
			relations = self.servers["relation"].getRelationsFromWay(id)
			return relations
		except TApplicationException:
			pass
		return None

	def getRelation(self, id):
		try:
			relation = self.servers["relation"].getRelation(id)
			return relation
		except TApplicationException:
			pass
		return None

