from node import NodeServer
from way import WayServer
from relation import RelationServer
from partitioner import *

from data.ttypes import *

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

import sys

class Menzies:
	
	def __init__(self, servers={"node": [("localhost", 9091)],
															"way":('localhost','9090'),
															"relation":('localhost','9092')}):
		self.servers = {"node":[]}
		for s,p in servers["node"]:
			def makeNodeClient():
				print "makeNodeClient(",s,p,")"
				sys.stdout.flush()
				transport = TSocket.TSocket(s,p)
				transport = TTransport.TBufferedTransport(transport)
				protocol = TBinaryProtocol.TBinaryProtocol(transport)
				client = NodeServer.Client(protocol)
				transport.open()
				return client
			self.servers["node"].append(makeNodeClient)
		
		self.node_partitioner = StaticLatPartitioner(self.servers["node"])

		def makeWayClient():
			transport = TSocket.TSocket(servers["way"][0],servers["way"][1])
			transport = TTransport.TBufferedTransport(transport)
			protocol = TBinaryProtocol.TBinaryProtocol(transport)
			client = WayServer.Client(protocol)
			transport.open()
			return client
		self.servers["way"] = makeWayClient

		def makeRelationClient():
			transport = TSocket.TSocket(servers["relation"][0],servers["relation"][1])
			transport = TTransport.TBufferedTransport(transport)
			protocol = TBinaryProtocol.TBinaryProtocol(transport)
			client = RelationServer.Client(protocol)
			transport.open()
			return client
		self.servers["relation"] = makeRelationClient

		self.next_node_id = 0
		self.next_changeset_id = 0

	def getAllInBounds(self, box):
		osm = Osm()

		osm.nodes = []
		osm.ways = []
		osm.relations = []

		relation_set = set()
		way_set = set()

		try:
			for server in self.node_partitioner.from_box(box):
				for node in server().getNodesInBounds(box):
					osm.nodes.append(node)
					for way in self.servers["way"]().getWaysFromNode(node.id):
						if way.id not in way_set:
							osm.ways.append(way)
							way_set.add(way.id)

						for relation in self.servers["relation"]().getRelationsFromWay(way.id):
							if relation.id not in relation_set:
								osm.relations.append(relation)
								relation_set.add(relation.id)
					for relation in self.servers["relation"]().getRelationsFromNode(node.id):
						if relation.id not in relation_set:
							osm.relations.append(relation)
							relation_set.add(relation.id)

						for relation_in_relation in self.servers["relation"]().getRelationsFromRelation(relation.id):
							if relation_in_relation.id not in relation_set:
								osm.relations.append(relation_in_relation)
								relation_set.add(relation_in_relation.id)
		except TApplicationException, e:
			if e.type != TApplicationException.MISSING_RESULT:
				raise e

		return osm

	def getNode(self,id):
		for s in self.node_partitioner.from_node_id(id):
			try:
				n = s().getNode(id)
				return n
			except TApplicationException, e:
				if e.type != TApplicationException.MISSING_RESULT:
					raise e
				else:
					print e.message

		return None
	
	def getNodeVersion(self, id, version):
		for s in self.node_partitioner.from_node_id(id):
			try:
				n = s().getNodeVersion(id, version)
				return n
			except TApplicationException, e:
				if e.type != TApplicationException.MISSING_RESULT:
					raise e

		return None
	
	def editNode(self, node):
		for s in self.node_partitioner.from_node(node):
			try:
				n = s().editNode(node)
				return n
			except TApplicationException, e:
				if e.type != TApplicationException.MISSING_RESULT:
					raise e

		return None
	
	def deleteNode(self, node_id):
		for s in self.node_partitioner.from_node_id(node_id):
			try:
				n = s().deleteNode(node_id)
				return n
			except TApplicationException, e:
				if e.type != TApplicationException.MISSING_RESULT:
					raise e

		return None
	
	def createNode(self, node):
		node.id = self.next_node_id
		self.next_node_id+=1
		
		self.node_partitioner.from_node(node)[0]().createNode(node)
		return node.id
	
	def getNodeHistory(self, id):
		for s in self.node_partitioner.from_node_id(id):
			try:
				n = s().getNodeHistory(id)
				return n
			except TApplicationException, e:
				if e.type != TApplicationException.MISSING_RESULT:
					raise e	
		
		return None

	def getWaysFromNode(self, id):
		try:
			ways = self.servers["way"]().getWaysFromNode(id)
			return ways
		except TApplicationException, e:
			if e.type != TApplicationException.MISSING_RESULT:
				raise e

		return None

	def getWay(self, id):
		try:
			way = self.servers["way"]().getWay(id)
			return way
		except TApplicationException, e:
			if e.type != TApplicationException.MISSING_RESULT:
				raise e

		return None

	def getWayVersion(self, id, version):
		try:
			w = self.servers["way"]().getWayVersion(id, version)
			return w
		except TApplicationException, e:
			if e.type != TApplicationException.MISSING_RESULT:
				raise e

		return None
	
	def editWay(self, way):
		try:
			w = self.servers["way"]().editWay(way)
			return w
		except TApplicationException, e:
			if e.type != TApplicationException.MISSING_RESULT:
				raise e

		return None
	
	def deleteWay(self, way):
		try:
			w = self.servers["way"]().deleteWay(way)
			return w
		except TApplicationException, e:
			if e.type != TApplicationException.MISSING_RESULT:
				raise e

		return None
	
	def createWay(self, way):
		return self.servers["way"]().createWay(way)

	def getWayHistory(self, id):
		try:
			w = self.servers["way"]().getWayHistory(id)
			return w
		except TApplicationException, e:
			if e.type != TApplicationException.MISSING_RESULT:
				raise e

		return None

	def getWayFull(self, id):
		try:
			osm = Osm()
			osm.ways = [self.servers["way"]().getWay(id)]
			osm.nodes = []
			for node_id in osm.ways[0].nodes:
				osm.nodes.append(self.getNode(node_id))
			return osm
		except TApplicationException, e:
			if e.type != TApplicationException.MISSING_RESULT:
				raise e

		return None

	def getRelationsFromNode(self, id):
		try:
			relations = self.servers["relation"]().getRelationsFromNode(id)
			return relations
		except TApplicationException, e:
			if e.type != TApplicationException.MISSING_RESULT:
				raise e

		return None

	def getRelationsFromWay(self, id):
		try:
			relations = self.servers["relation"]().getRelationsFromWay(id)
			return relations
		except TApplicationException, e:
			if e.type != TApplicationException.MISSING_RESULT:
				raise e

		return None

	def getRelationsFromRelation(self, id):
		try:
			relations = self.servers["relation"]().getRelationsFromRelation(id)
			return relations
		except TApplicationException, e:
			if e.type != TApplicationException.MISSING_RESULT:
				raise e

		return None

	def getRelation(self, id):
		try:
			relation = self.servers["relation"]().getRelation(id)
			return relation
		except TApplicationException, e:
			if e.type != TApplicationException.MISSING_RESULT:
				raise e

		return None

	def getRelationFull(self, id):
		try:
			osm = Osm()
			osm.relations = [self.servers["relation"]().getRelation(id)]

			osm.nodes = []
			osm.ways = []

			for member in osm.relations[0].members:
				if member.node != None:
					osm.nodes.append(self.getNode(member.node))
				elif member.way != None:
					osm.ways.append(self.servers["way"]().getWay(member.way))
				elif member.relation != None:
					osm.relations.append(self.servers["relation"]().getRelation(member.relation))

			return osm
		except TApplicationException, e:
			if e.type != TApplicationException.MISSING_RESULT:
				raise e

		return None

	def createRelation(self, relation):
		return self.servers["relation"]().createRelation(relation)
	
	def createChangeset(self, changeset):
		i = self.next_changeset_id
		self.next_changeset_id += 1
		return i

