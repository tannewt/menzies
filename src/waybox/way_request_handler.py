
import data

from bsddb import db as bdb

import thrift_wrapper

'''
  data.Way getWay(1: i64 id, 2: optional i32 version, 3: optional bool full),
  list<data.Way> getWays(1: optional list<i64> ids, 2: optional i64 node, 3: optional i64 relation),
  i32 editWay(1: data.Way way),
  i32 deleteWay(1: data.Way way),
  i64 createWay(1: data.Way way),
  list<data.Way> wayHistory(1: i64 id),
'''

class WayRequestHandler:
	def __init__(self):
		self.db = bdb.DB()
		self.db.set_flags(bdb.DB_DUP)
		self.db.open("ways.db","Ways", bdb.DB_BTREE, bdb.DB_CREATE)

		self.cursor = self.db.cursor()

	def getWay(self, id):
		way = Way()
		thrift_wrapper.from_string(way, self.db.get(id))
		return way

	def getWay(self, id, version):
		way = Way()

		data = self.cursor.get(id, bdb.DB_SET)
		while data:
			thrift_wrapper.from_string(way, data)
		
			if way.version == version:
				return way

			data = self.cursor.get(id, bdb.DB_NEXT)

		# raise an exception

	def createWay(self, way):
		data = thrift_wrapper.to_string(way)
		self.cursor.put("%d"%way.id, data, bdb.DB_KEYFIRST)
		return way.id


