from bsddb import db as bdb

import sys
sys.path.append('../common')
sys.path.append('../common/gen-py')
from data.ttypes import *

import thrift_wrapper

class WayRequestHandler:
	def __init__(self):
		self.db = bdb.DB()
		self.db.set_flags(bdb.DB_DUP)
		self.db.open("ways.db","Ways", bdb.DB_BTREE, bdb.DB_CREATE)

		self.cursor = self.db.cursor()

	def getWay(self, id):
		way = Way()
		data = self.db.get("%d"%id)
		if data:
			thrift_wrapper.from_string(way, data)
			return way
		else:
			return None

	def getWayVersion(self, id, version):
		way = Way()

		data = self.cursor.get(id, bdb.DB_SET)
		while data:
			thrift_wrapper.from_string(way, data)
		
			if way.version == version:
				return way

			data = self.cursor.get(id, bdb.DB_NEXT_DUP)

		# raise an exception

	def createWay(self, way):
		data = thrift_wrapper.to_string(way)
		self.cursor.put("%d"%way.id, data, bdb.DB_KEYFIRST)
		return way.id


