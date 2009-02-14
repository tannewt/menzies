from bsddb import db as bdb

import sys
sys.path.append('../common/gen-py')
sys.path.append('../common')
from data.ttypes import *

import thrift_wrapper

class NodeRequestHandler:
	def __init__(self):
		self.db = bdb.DB()
		self.db.set_flags(bdb.DB_DUP)
		self.db.open("nodes.db","Nodes", bdb.DB_BTREE, bdb.DB_CREATE)

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
			node = getNode(id)
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

		return None

	def createNode(self, node):
		data = thrift_wrapper.to_string(node)
		cursor = self.db.cursor()
		cursor.put("%d"%node.id, data, bdb.DB_KEYFIRST)
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

		return nodes

	def deleteNode(self, node):
		node_id_str = "%d"%node.id

		# Get the last version
		cursor = self.db.cursor()
		data_pair = cursor.get(node_id_str, bdb.DB_SET)
		if data_pair:
			old_node = Node()
			thrift_wrapper.from_string(old_node, data_pair[1])

			node.visible = False
			node.version = old_way.version + 1 # This is bound to have concurrency issues
			data = thrift_wrapper.to_string(node)
			cursor.put(node_id_str, data, bdb.DB_KEYFIRST)

			return node.id
		else:
			# There was no previous version!
			pass


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

			return node.id
		else:
			# There was no previous version!
			pass

