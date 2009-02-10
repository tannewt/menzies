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

		self.cursor = self.db.cursor()

	def getNode(self, id):
		node = Node()
		data = self.db.get("%d"%id)
		if data:
			thrift_wrapper.from_string(node, data)
			return node
		else:
			return None

	def getNodeVersion(self, id, version):
		node = Node()

		data = self.cursor.get(id, bdb.DB_SET)
		while data:
			thrift_wrapper.from_string(node, data)
		
			if node.version == version:
				return node

			data = self.cursor.get(id, bdb.DB_NEXT_DUP)

		# raise an exception

	def createNode(self, node):
		data = thrift_wrapper.to_string(node)
		self.cursor.put("%d"%node.id, data, bdb.DB_KEYFIRST)
		return node.id

