from bsddb import db as bdb

import sys
sys.path.append('../common/gen-py')
sys.path.append('../common')
from data.ttypes import *

import thrift_wrapper

try:
	from rtree import Rtree
except:
	Rtree = None


class NodeRequestHandler:
	def __init__(self):
		self.db = bdb.DB()
		self.db.set_flags(bdb.DB_DUP)
		self.db.open("nodes.db","Nodes", bdb.DB_BTREE, bdb.DB_CREATE)

		#self.spatial_index = Rtree("spatial", pagesize=8) # page holds 64 bit node ids

	def getNode(self, id):
		node = Node()
		data = self.db.get("%d"%id)
		if data:
			thrift_wrapper.from_string(node, data)
			return node
		else:
			return None

	def getNodes(self, ids):
		nodes = []
		for id in ids:
			node = self.getNode(id)
			if node: nodes.append(node)
			else:
				return None
		return nodes

	def getNodeVersion(self, id, version):
		node = Node()

		cursor = self.db.cursor()
		data_pair = cursor.get("%d"%id, bdb.DB_SET)
		while data_pair:
			thrift_wrapper.from_string(node, data_pair[1])
		
			if node.version == version:
				return node

			data_pair = cursor.get("%d"%id, bdb.DB_NEXT_DUP)

		cursor.close()
		return None

	def createNode(self, node):
		node.version = 1

		data = thrift_wrapper.to_string(node)
		# Note to self: The bulk import process was 100's times faster not creating a new cursor
		# Maybe unnecessary locking issues?
		cursor = self.db.cursor()
		cursor.put("%d"%node.id, data, bdb.DB_KEYFIRST)
		#if Rtree:
		#	self.spatial_index.add(node.id, (node.lat, node.lon))
		cursor.close()
		return node.id

	def createNodes(self, nodes):
		cursor = self.db.cursor()
		for node in nodes:
			data = thrift_wrapper.to_string(node)
			cursor.put("%d"%node.id, data, bdb.DB_KEYFIRST)
			#if Rtree:
			#	self.spatial_index.add(node.id, (node.lat, node.lon))
			return node.id
		cursor.close()

	def getNodeHistory(self, id):
		nodes = []

		cursor = self.db.cursor()
		data_pair = cursor.get("%d"%id, bdb.DB_SET)
		while data_pair:
			node = Node()
			thrift_wrapper.from_string(node, data_pair[1])
			nodes.append(node)

			data_pair = cursor.get("%d"%id, bdb.DB_NEXT_DUP)

		cursor.close()
		return nodes

	def deleteNode(self, node_id):
		node_id_str = "%d"%node_id

		# Get the last version
		cursor = self.db.cursor()
		data_pair = cursor.get(node_id_str, bdb.DB_SET)
		if data_pair:
			old_node = Node()
			thrift_wrapper.from_string(old_node, data_pair[1])

			node = self.getNode(node_id)
			node.visible = False
			node.version = old_node.version + 1 # This is bound to have concurrency issues
			data = thrift_wrapper.to_string(node)
			cursor.put(node_id_str, data, bdb.DB_KEYFIRST)

			cursor.close()
			return node.id
		else:
			# There was no previous version!
			cursor.close()


	# FIXME: Bad things happen when there's duplicate node ids referenced by a node
	def editNode(self, node):
		node_id_str = "%d"%node.id

		# Get the last version
		cursor = self.db.cursor()
		data_pair = cursor.get(node_id_str, bdb.DB_SET)
		if data_pair:
			old_node = Node()
			thrift_wrapper.from_string(old_node, data_pair[1])

			node.version = old_node.version + 1 # This is bound to have concurrency issues
			data = thrift_wrapper.to_string(node)
			cursor.put(node_id_str, data, bdb.DB_KEYFIRST)

			cursor.close()
			return node.version
		else:
			# There was no previous version!
			cursor.close()

	def getNodesInBounds(self, bounds):
		if Rtree:
			node_ids = self.spatial_index.intersection((bounds.min_lat, bounds.min_lon, bounds.max_lat, bounds.max_lon))
			return self.getNodes(node_ids)
		else:		
			def contains(bounds, node):
				return (node.lat > bounds.min_lat and node.lat < bounds.max_lat) and (node.lon > bounds.min_lon and node.lon < bounds.max_lon)

			# Do the stupid search
			nodes = []
			cursor = self.db.cursor()
			data_pair = cursor.get(None, bdb.DB_FIRST)
			while data_pair:
				node = Node()
				thrift_wrapper.from_string(node, data_pair[1])
				if contains(bounds, node):
					nodes.append(node)
				data_pair = cursor.get(None, bdb.DB_NEXT_NODUP)

			return nodes

