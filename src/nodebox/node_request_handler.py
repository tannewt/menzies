# -*- coding: utf-8 -*-
from bsddb import db as bdb

import sys, os
sys.path.append('../common/gen-py')
sys.path.append('../common')
from data.ttypes import *

from berkeley_db_rtree import RTree
import thrift_wrapper

DB_ENV = bdb.DBEnv()
DB_ENV.open(None, bdb.DB_CREATE | bdb.DB_INIT_LOCK | bdb.DB_INIT_MPOOL | bdb.DB_THREAD)

class NodeRequestHandler:
	def __init__(self, data_dir=None):
		if data_dir == None:
			data_dir = ""
			if os.environ.has_key("DATA_DIR"):
				data_dir = os.environ["DATA_DIR"]

		self.db = bdb.DB(DB_ENV)
		self.db.set_flags(bdb.DB_DUP)
		self.db.open(os.path.join(data_dir,"nodes.db"),"Nodes", bdb.DB_BTREE, bdb.DB_CREATE)

		self.spatial_index = RTree(os.path.join(data_dir,"spatial_index.db"))

	def cleanup(self):
		self.db.close()
		self.spatial_index.cleanup()

	def ping(self):
		pass

	def getNode(self, id):
		node = Node()
		data = self.db.get("%d"%id)
		if data:
			thrift_wrapper.from_string(node, data)
			return node
		else:
			print "No node found, %d" % id
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
		node.visible = True

		data = thrift_wrapper.to_string(node)
		# Note to self: The bulk import process was 100's times faster not creating a new cursor
		# Maybe unnecessary locking issues?
		cursor = self.db.cursor()
		cursor.put("%d"%node.id, data, bdb.DB_KEYFIRST)
		cursor.close()

		if self.spatial_index:
			self.spatial_index.insert(node)

		return node.id

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

		if len(nodes) > 0:
			return nodes
		else:
			return None

	def deleteNode(self, node_id):
		node_id_str = "%d"%node_id

		# Get the last version
		cursor = self.db.cursor()
		data_pair = cursor.get(node_id_str, bdb.DB_SET)
		if data_pair:
			old_node = Node()
			thrift_wrapper.from_string(old_node, data_pair[1])

			old_node.visible = False
			old_node.version += 1
			data = thrift_wrapper.to_string(old_node)
			cursor.put(node_id_str, data, bdb.DB_KEYFIRST)

			cursor.close()

			self.spatial_index.delete(old_node)

			return old_node.version
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

			if node.lat != old_node.lat or node.lon != old_node.lon:
				self.spatial_index.delete(old_node)
				self.spatial_index.insert(node)

			node.version = old_node.version + 1 # This is bound to have concurrency issues
			data = thrift_wrapper.to_string(node)
			cursor.put(node_id_str, data, bdb.DB_KEYFIRST)

			cursor.close()
			return node.version
		else:
			# There was no previous version!
			cursor.close()

	def getNodesInBounds(self, bounds):
		def contains(bounds, node):
			return (node.lat > bounds.min_lat and node.lat < bounds.max_lat) and (node.lon > bounds.min_lon and node.lon < bounds.max_lon)

		if self.spatial_index:
			node_ids = self.spatial_index.likely_intersection(bounds)

			# We'll get some nodes that are outside the requested bounds, so filter them out
			return [node for node in self.getNodes(node_ids) if contains(bounds,node)]
		else:
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

