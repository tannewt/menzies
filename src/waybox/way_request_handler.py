#
# Implements the methods provided by the service defined in 'way.thrift'
#

from bsddb import db as bdb

import threading
import sys, os
sys.path.append('../common')
sys.path.append('../common/gen-py')
from data.ttypes import *

import thrift_wrapper

class WayRequestHandler:
	def __init__(self):
		data_dir = ""
		if os.environ.has_key("DATA_DIR"):
			data_dir = os.environ["DATA_DIR"]

		self.db = bdb.DB()
		self.db.set_flags(bdb.DB_DUP)
		self.db.open(os.path.join(data_dir,"ways.db"),"Ways", bdb.DB_BTREE, bdb.DB_CREATE)

		if not self.db.get("next_id"):
			print "Initializing next_id to 0"
			self.db.put("next_id", "0")

		self.reverse_node_index = bdb.DB()
		self.reverse_node_index.set_flags(bdb.DB_DUP)
		self.reverse_node_index.open(os.path.join(data_dir,"ways_reverse_node_index.db"),"Reverse Node Index", bdb.DB_BTREE, bdb.DB_CREATE)

		self.increment_lock = threading.Lock()

		#self.debug_print_db()
		#self.debug_print_reverse_node_index()

	def debug_print_db(self):
		way = Way()

		print "Way db:"

		cursor = self.db.cursor()
		data_pair = cursor.get(None, bdb.DB_FIRST)
		while data_pair:
			thrift_wrapper.from_string(way, data_pair[1])
			print "%s -> %s" % (data_pair[0],way)
			data_pair = cursor.get(None, bdb.DB_NEXT)


	def debug_print_reverse_node_index(self):
		print "Reverse node index (node -> way):"

		cursor = self.reverse_node_index.cursor()
		data_pair = cursor.get(None, bdb.DB_FIRST)
		while data_pair:
			print "%s -> %s" % data_pair
			data_pair = cursor.get(None, bdb.DB_NEXT)

	def getWay(self, id):
		print "getWay(%d)" % id

		way = Way()
		data = self.db.get("%d"%id)
		if data:
			thrift_wrapper.from_string(way, data)
			return way
		else:
			return None

	def getWays(self, ids):
		ways = []
		for id in ids:
			way = getWay(id)
			if way: ways.append(way)
			else:
				return None
		return ways

	def getWayVersion(self, id, version):
		way = Way()

		cursor = self.db.cursor()
		data_pair = cursor.get("%d"%id, bdb.DB_SET)
		while data_pair:
			thrift_wrapper.from_string(way, data_pair[1])
		
			if way.version == version:
				return way

			data_pair = cursor.get("%d"%id, bdb.DB_NEXT_DUP)

		return None

	def getWayHistory(self, id):
		ways = []

		cursor = self.db.cursor()
		data_pair = cursor.get("%d"%id, bdb.DB_SET)
		while data_pair:
			way = Way()
			thrift_wrapper.from_string(way, data_pair[1])
			ways.append(way)

			data_pair = cursor.get("%d"%id, bdb.DB_NEXT_DUP)

		return ways

	def getWaysFromNode(self, node_id):
		cursor = self.reverse_node_index.cursor()
		way_ids = set()
		way_id_str_pair = cursor.get("%d"%node_id, bdb.DB_SET)
		while way_id_str_pair:
			way_ids.add(way_id_str_pair[1])
			way_id_str_pair = cursor.get("%d"%node_id, bdb.DB_NEXT_DUP)

		ways = []
		for way_id_str in way_ids:
			data = self.db.get(way_id_str)
			if data:
				way = Way()
				thrift_wrapper.from_string(way, data)
				ways.append(way)
		return ways

	def createWay(self, way):
		increment_lock.acquire()
		next_id = long(db.get("next_id"))
		db.put("next_id", "%d"%(next_id+1))
		increment_lock.release()	

		way.id = next_id
		way.version = 1

		way_id_str = "%d"%way.id

		data = thrift_wrapper.to_string(way)
		cursor = self.db.cursor()
		cursor.put(way_id_str, data, bdb.DB_KEYFIRST)
		cursor.close()
	
		# Update indexes
		for node_id in way.nodes:
			self.reverse_node_index.put("%d"%node_id, way_id_str)

		return way.id

	def createWays(self, ways):
		cursor = self.db.cursor()
		for way in ways:
			way.version = 1

			way_id_str = "%d"%way.id

			data = thrift_wrapper.to_string(way)
			cursor.put(way_id_str, data, bdb.DB_KEYFIRST)

			# Update indexes
			for node_id in way.nodes:
				self.reverse_node_index.put("%d"%node_id, way_id_str)

		cursor.close()

	def deleteWay(self, id):
		way_id_str = "%d"%id

		# Get the last version
		cursor = self.db.cursor()
		data_pair = cursor.get(way_id_str, bdb.DB_SET)
		if data_pair:
			old_way = Way()
			thrift_wrapper.from_string(old_way, data_pair[1])
			
			# Indexes to remove
			reverse_node_cursor = self.reverse_node_index.cursor()
			for node_id in set(old_way.nodes):
				way_id_search_str_pair = reverse_node_cursor.get("%d"%node_id, bdb.DB_SET)
				while way_id_search_str_pair:
					if way_id_str == way_id_search_str_pair[1]:
						reverse_node_cursor.delete()
						break
					way_id_search_str_pair = reverse_node_cursor.get("%d"%node_id, bdb.DB_NEXT_DUP)

			old_way.visible = False
			old_way.version += 1
			data = thrift_wrapper.to_string(old_way)
			cursor.put(way_id_str, data, bdb.DB_KEYFIRST)

			return old_way.version
		else:
			# There was no previous version!
			pass


	# FIXME: Bad things happen when there's duplicate node ids referenced by a node
	def editWay(self, way):
		way_id_str = "%d"%way.id

		# Get the last version
		cursor = self.db.cursor()
		data_pair = cursor.get(way_id_str, bdb.DB_SET)
		if data_pair:
			old_way = Way()
			thrift_wrapper.from_string(old_way, data_pair[1])
			
			# Indexes to add to
			for node_id in set(way.nodes) - set(old_way.nodes):
				self.reverse_node_index.put("%d"%node_id, way_id_str)

			# Indexes to remove
			reverse_node_cursor = self.reverse_node_index.cursor()
			for node_id in set(old_way.nodes) - set(way.nodes):
				way_id_search_str_pair = reverse_node_cursor.get("%d"%node_id, bdb.DB_SET)
				while way_id_search_str_pair:
					if way_id_str == way_id_search_str_pair[1]:
						reverse_node_cursor.delete()
						break
					way_id_search_str_pair = reverse_node_cursor.get("%d"%node_id, bdb.DB_NEXT_DUP)

			way.version = old_way.version + 1
			data = thrift_wrapper.to_string(way)
			cursor.put(way_id_str, data, bdb.DB_KEYFIRST)

			return way.id
		else:
			# There was no previous version!
			pass

