# -*- coding: utf-8 -*-
from node import NodeServer
from way import WayServer
from relation import RelationServer
from partitioner import *
from future import Future

from data.ttypes import *

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

import threading
from Queue import *

import sys, os
from bsddb import db as bdb

class curry:
    def __init__(self, fun, *args, **kwargs):
        self.fun = fun
        self.pending = args[:]
        self.kwargs = kwargs.copy()

    def __call__(self, *args, **kwargs):
        if kwargs and self.kwargs:
            kw = self.kwargs.copy()
            kw.update(kwargs)
        else:
            kw = kwargs or self.kwargs

        return self.fun(*(self.pending + args), **kw)

class ClientPool:
	def __init__(self, servers):
		self.server_info = servers

		self.pool_size = 1

		self.free_servers = {}
		self.used_servers = {}

		for server, make_it in servers.items():
			self.free_servers[server] = Queue(0)
			for i in range(self.pool_size):
				self.free_servers[server].put(None)

	# Block until we are able to acquire clients open to the given servers
	def acquire(self, servers):
		clients = {}

		for server in servers:
			print "acquiring %s" % str(server)
			wrapper = self.free_servers[server].get(True)
			if not wrapper or wrapper.needs_new_connection():
				wrapper = self.server_info[server]()

			print "got %s - %s" % (str(server), str(wrapper.client))

			clients[server] = wrapper.client
			self.used_servers[wrapper.client] = wrapper

		return clients

	def release(self, clients):
		for info, client in clients.items():
			wrapper = self.used_servers.pop(client)
			self.free_servers[info].put(wrapper)

class ClientWrapper:
	def __init__(self, transport, client):
		self.transport = transport
		self.client = client
		self.force_new = False

	def needs_new_connection(self):
		return self.force_new or not self.transport.isOpen()

class Menzies:
	
	def __init__(self, servers={"node": [("localhost", 9091)],
															"way":('localhost',9090),
															"relation":('localhost',9092)}):
		
		self.servers = servers

		def makeNodeClient(server, port):
			print "makeNodeClient(",server,port,")"

			transport = TSocket.TSocket(server,port)
			transport = TTransport.TBufferedTransport(transport)
			protocol = TBinaryProtocol.TBinaryProtocol(transport)
			client = NodeServer.Client(protocol)
			transport.open()
			return ClientWrapper(transport, client)

		def makeWayClient(server, port):
			print "makeWayClient()"

			transport = TSocket.TSocket(server,port)
			transport = TTransport.TBufferedTransport(transport)
			protocol = TBinaryProtocol.TBinaryProtocol(transport)
			client = WayServer.Client(protocol)
			transport.open()
			return ClientWrapper(transport, client)

		def makeRelationClient(server,port):
			print "makeRelationClient()"

			transport = TSocket.TSocket(server,port)
			transport = TTransport.TBufferedTransport(transport)
			protocol = TBinaryProtocol.TBinaryProtocol(transport)
			client = RelationServer.Client(protocol)
			transport.open()
			return ClientWrapper(transport, client)

		server_info = {}
		server_info[servers["way"]] = curry(makeWayClient,*servers["way"])
		server_info[servers["relation"]] = curry(makeRelationClient,*servers["relation"])
		for info in servers["node"]:
			server_info[info] = curry(makeNodeClient,*info)
		self.client_pool = ClientPool(server_info)

		self.node_partitioner = StaticLatPartitioner(servers["node"])

		self.db = bdb.DB()
		#self.db.set_flags(bdb.DB_DUP)
		self.db.open(os.path.join("","frontend.db"),"Frontend Data", bdb.DB_BTREE, bdb.DB_CREATE)
		if not self.db.get("next_node_id"):
			print "Initializing next_node_id to 0"
			self.db.put("next_node_id", "0")
		self.increment_lock = threading.Lock()

		self.next_changeset_id = 0

	def cleanup(self):
		self.db.close()

	def getAllInBounds(self, box):
		servers = self.client_pool.acquire([self.servers["way"]] + [self.servers["relation"]] + self.servers["node"])

		try:
			for x in self._getAllInBounds(servers, box): yield x
		finally:
			self.client_pool.release(servers)

	def _getAllInBounds(self, servers, box):

		relation_set = set()
		way_set = set()
		node_set = set()

		nodes_in_ways = set()

		for server_info in self.node_partitioner.from_box(box):
			server = servers[server_info]

			# get all nodes in the box
			try:
				nodes = server.getNodesInBounds(box)
			except TApplicationException, e:
				if e.type != TApplicationException.MISSING_RESULT:
					raise e
				nodes = []
			
			# for each of those nodes
			for node in nodes:
				node_set.add(node.id)
				yield (0, node)
				
				#get all the ways this node is in
				try:
					ways = servers[self.servers["way"]].getWaysFromNode(node.id)
				except TApplicationException, e:
					if e.type != TApplicationException.MISSING_RESULT:
						raise e
					ways = []
				
				# for each of these ways
				for way in ways:
					for node_id in way.nodes: nodes_in_ways.add(node_id)

					if way.id not in way_set:
						way_set.add(way.id)
						yield (1, way)
				
					# get all relations this way is in
					try:
						relations = servers[self.servers["relation"]].getRelationsFromWay(way.id)
					except TApplicationException, e:
						if e.type != TApplicationException.MISSING_RESULT:
							raise e
						relations = []
					
					for relation in relations:
						if relation.id not in relation_set:
							relation_set.add(relation.id)
							yield (2, relation)
				
				# get all relations this node is in
				try:
					relations = servers[self.servers["relation"]].getRelationsFromNode(node.id)
				except TApplicationException, e:
					if e.type != TApplicationException.MISSING_RESULT:
						raise e
					relations = []
				
				for relation in relations:
					if relation.id not in relation_set:
						relation_set.add(relation.id)
						yield (2, relation)

					# get all relations this relation is in
					try:
						relations2 = servers[self.servers["relation"]].getRelationsFromRelation(relation.id)
					except TApplicationException, e:
						if e.type != TApplicationException.MISSING_RESULT:
							raise e
						relations2 = []
					
					for relation_in_relation in relations2:
						if relation_in_relation.id not in relation_set:
							relation_set.add(relation_in_relation.id)
							yield (2, relation_in_relation)

			print "Fetching nodes outside the bounding box"
			# Fetch nodes outside the bounding box that are part of ways within the bounding box
			for node_id in nodes_in_ways:
				if node_id not in node_set:
					for server_info in self.node_partitioner.from_node_id(node_id):
						server = servers[server_info]
						try:
							node = server.getNode(node_id)
						except TApplicationException, e:
							if e.type != TApplicationException.MISSING_RESULT:
								raise e
							node = None
						if node:
							node_set.add(node_id)
							yield (0, node)
							break

	def getNode(self, id):
		servers = self.client_pool.acquire(self.servers["node"])
		try:
			return self._getNode(servers, id)
		finally:
			self.client_pool.release(servers)

	def _getNode(self, servers, id):
		for s_info in self.node_partitioner.from_node_id(id):
			s = servers[s_info]
			try:
				n = s.getNode(id)
				return n
			except TApplicationException, e:
				if e.type != TApplicationException.MISSING_RESULT:
					raise e
				else:
					print e.message

		return None
	
	def getNodeVersion(self, id, version):
		servers = self.client_pool.acquire(self.servers["node"])
		try:
			return self._getNodeVersion(servers, id, version)
		finally:
			self.client_pool.release(servers)

	def _getNodeVersion(self, servers, id, version):
		for s_info in self.node_partitioner.from_node_id(id):
			s = servers[s_info]
			try:
				n = s.getNodeVersion(id, version)
				return n
			except TApplicationException, e:
				if e.type != TApplicationException.MISSING_RESULT:
					raise e

		return None
	
	def editNode(self, node):
		servers = self.client_pool.acquire(self.servers["node"])
		try:
			return self._editNode(servers, node)
		finally:
			self.client_pool.release(servers)

	def _editNode(self, servers, node):
		for s_info in self.node_partitioner.from_node(node):
			s = servers[s_info]
			try:
				n = s.editNode(node)
				return n
			except TApplicationException, e:
				if e.type != TApplicationException.MISSING_RESULT:
					raise e

		return None
	
	def deleteNode(self, node_id):
		servers = self.client_pool.acquire(self.servers["node"])
		try:
			return self._deleteNode(servers, node_id)
		finally:
			self.client_pool.release(servers)


	def _deleteNode(self, servers, node_id):
		for s_info in self.node_partitioner.from_node_id(node_id):
			s = servers[s_info]
			try:
				n = s.deleteNode(node_id)
				return n
			except TApplicationException, e:
				if e.type != TApplicationException.MISSING_RESULT:
					raise e

		return None
	
	def createNode(self, node):
		servers = self.client_pool.acquire(self.servers["node"])
		try:
			return self._createNode(servers, node)
		finally:
			self.client_pool.release(servers)

	def _createNode(self, servers, node):
		self.increment_lock.acquire()
		next_id = long(self.db.get("next_node_id"))
		self.db.delete("next_node_id")
		self.db.put("next_node_id", "%d"%(next_id+1))
		self.increment_lock.release()

		node.id = next_id
		print "next_node_id: ", self.db.get("next_node_id")
		
		servers[self.node_partitioner.from_node(node)[0]].createNode(node)
		return node.id
	
	def getNodeHistory(self, id):
		servers = self.client_pool.acquire(self.servers["node"])
		try:
			return self._getNodeHistory(servers, id)
		finally:
			self.client_pool.release(servers)


	def _getNodeHistory(self, servers, id):
		for s_info in self.node_partitioner.from_node_id(id):
			s = servers[s_info]
			try:
				n = s.getNodeHistory(id)
				return n
			except TApplicationException, e:
				if e.type != TApplicationException.MISSING_RESULT:
					raise e	
		
		return None

	def getWaysFromNode(self, id):
		servers = self.client_pool.acquire([self.servers["way"]])
		try:
			return self._getWaysFromNode(servers, id)
		finally:
			self.client_pool.release(servers)

	def _getWaysFromNode(self, servers, id):
		try:
			ways = servers[self.servers["way"]].getWaysFromNode(id)
			return ways
		except TApplicationException, e:
			if e.type != TApplicationException.MISSING_RESULT:
				raise e

		return None

	def getWay(self, id):
		servers = self.client_pool.acquire([self.servers["way"]])
		try:
			return self._getWay(servers, id)
		finally:
			self.client_pool.release(servers)

	def _getWay(self, servers, id):
		try:
			way = servers[self.servers["way"]].getWay(id)
			return way
		except TApplicationException, e:
			if e.type != TApplicationException.MISSING_RESULT:
				raise e

		return None

	def getWayVersion(self, id, version):
		servers = self.client_pool.acquire([self.servers["way"]])
		try:
			return self._getWayVersion(servers, id, version)
		finally:
			self.client_pool.release(servers)


	def _getWayVersion(self, servers, id, version):
		try:
			w = servers[self.servers["way"]].getWayVersion(id, version)
			return w
		except TApplicationException, e:
			if e.type != TApplicationException.MISSING_RESULT:
				raise e

		return None
	
	def editWay(self, way):
		servers = self.client_pool.acquire([self.servers["way"]])
		try:
			return self._editWay(servers, way)
		finally:
			self.client_pool.release(servers)

	def _editWay(self, servers, way):
		try:
			w = servers[self.servers["way"]].editWay(way)
			return w
		except TApplicationException, e:
			if e.type != TApplicationException.MISSING_RESULT:
				raise e

		return None
	
	def deleteWay(self, way):
		servers = self.client_pool.acquire([self.servers["way"]])
		try:
			return self._deleteWay(servers, way)
		finally:
			self.client_pool.release(servers)

	def _deleteWay(self, servers, way):
		try:
			w = servers[self.servers["way"]].deleteWay(way)
			return w
		except TApplicationException, e:
			if e.type != TApplicationException.MISSING_RESULT:
				raise e

		return None
	
	def createWay(self, way):
		servers = self.client_pool.acquire([self.servers["way"]])
		try:
			return self._createWay(servers, way)
		finally:
			self.client_pool.release(servers)

	def _createWay(self, servers, way):
		return servers[self.servers["way"]].createWay(way)

	def getWayHistory(self, id):
		servers = self.client_pool.acquire([self.servers["way"]])
		try:
			return self._getWayHistory(servers, id)
		finally:
			self.client_pool.release(servers)

	def _getWayHistory(self, servers, id):
		try:
			w = servers[self.servers["way"]].getWayHistory(id)
			return w
		except TApplicationException, e:
			if e.type != TApplicationException.MISSING_RESULT:
				raise e

		return None

	def getWayFull(self, id):
		servers = self.client_pool.acquire([self.servers["way"]])
		try:
			return self._getWayFull(servers, id)
		finally:
			self.client_pool.release(servers)

	def _getWayFull(self, servers, id):
		try:
			osm = Osm()
			osm.ways = [servers[self.servers["way"]].getWay(id)]
			osm.nodes = []
			for node_id in osm.ways[0].nodes:
				osm.nodes.append(self.getNode(node_id))
			return osm
		except TApplicationException, e:
			if e.type != TApplicationException.MISSING_RESULT:
				raise e

		return None

	def getRelationsFromNode(self, id):
		servers = self.client_pool.acquire([self.servers["relation"]])
		try:
			return self._getRelationsFromNode(servers, id)
		finally:
			self.client_pool.release(servers)

	def _getRelationsFromNode(self, servers, id):
		try:
			relations = servers[self.servers["relation"]].getRelationsFromNode(id)
			return relations
		except TApplicationException, e:
			if e.type != TApplicationException.MISSING_RESULT:
				raise e

		return None

	def getRelationsFromWay(self, id):
		servers = self.client_pool.acquire([self.servers["relation"]])
		try:
			return self._getRelationsFromWay(servers, id)
		finally:
			self.client_pool.release(servers)

	def _getRelationsFromWay(self, servers, id):
		try:
			relations = servers[self.servers["relation"]].getRelationsFromWay(id)
			return relations
		except TApplicationException, e:
			if e.type != TApplicationException.MISSING_RESULT:
				raise e

		return None

	def getRelationsFromRelation(self, id):
		servers = self.client_pool.acquire([self.servers["relation"]])
		try:
			return self._getRelationsFromRelation(servers, id)
		finally:
			self.client_pool.release(servers)

	def _getRelationsFromRelation(self, servers, id):
		try:
			relations = servers[self.servers["relation"]].getRelationsFromRelation(id)
			return relations
		except TApplicationException, e:
			if e.type != TApplicationException.MISSING_RESULT:
				raise e

		return None

	def getRelation(self, id):
		servers = self.client_pool.acquire([self.servers["relation"]])
		try:
			return self._getRelation(servers, id)
		finally:
			self.client_pool.release(servers)

	def _getRelation(self, servers, id):
		try:
			relation = servers[self.servers["relation"]].getRelation(id)
			return relation
		except TApplicationException, e:
			if e.type != TApplicationException.MISSING_RESULT:
				raise e

		return None

	def getRelationFull(self, id):
		servers = self.client_pool.acquire([self.servers["relation"], self.servers["way"]])
		try:
			return self._getRelationFull(servers, id)
		finally:
			self.client_pool.release(servers)

	def _getRelationFull(self, servers, id):
		try:
			osm = Osm()
			osm.relations = [servers[self.servers["relation"]].getRelation(id)]

			osm.nodes = []
			osm.ways = []

			for member in osm.relations[0].members:
				if member.node != None:
					osm.nodes.append(self.getNode(member.node))
				elif member.way != None:
					osm.ways.append(servers[self.servers["way"]].getWay(member.way))
				elif member.relation != None:
					osm.relations.append(servers[self.servers["relation"]].getRelation(member.relation))

			return osm
		except TApplicationException, e:
			if e.type != TApplicationException.MISSING_RESULT:
				raise e

		return None

	def createRelation(self, relation):
		servers = self.client_pool.acquire([self.servers["relation"]])
		try:
			return self._createRelation(servers, relation)
		finally:
			self.client_pool.release(servers)

	def _createRelation(self, servers, relation):
		return servers[self.servers["relation"]].createRelation(relation)
	
	def createChangeset(self, changeset):
		i = self.next_changeset_id
		self.next_changeset_id += 1
		return i

