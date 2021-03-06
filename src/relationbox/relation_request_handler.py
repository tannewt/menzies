# -*- coding: utf-8 -*-
#
# Implements the methods provided by the service defined in 'relation.thrift'
#

from bsddb import db as bdb

import sys, os
sys.path.append('../common')
sys.path.append('../common/gen-py')
from data.ttypes import *

import thrift_wrapper

DB_ENV = bdb.DBEnv()
DB_ENV.set_cachesize(0, 512*(1<<20))
DB_ENV.open(None, bdb.DB_CREATE | bdb.DB_INIT_LOCK | bdb.DB_INIT_MPOOL | bdb.DB_THREAD)

class RelationRequestHandler:
	def __init__(self):
		data_dir = ""
		if os.environ.has_key("DATA_DIR"):
			data_dir = os.environ["DATA_DIR"]

		self.db = bdb.DB(DB_ENV)
		self.db.set_flags(bdb.DB_DUP)
		self.db.open(os.path.join(data_dir,"relations.db"),"Relations", bdb.DB_BTREE, bdb.DB_CREATE)

		if not self.db.get("next_id"):
			print "Initializing next_id to 0"
			self.db.put("next_id", "0")

		self.reverse_node_index = bdb.DB(DB_ENV)
		self.reverse_node_index.set_flags(bdb.DB_DUP)
		self.reverse_node_index.open(os.path.join(data_dir,"relations_reverse_node_index.db"),"Reverse Node Index", bdb.DB_BTREE, bdb.DB_CREATE)

		self.reverse_relation_index = bdb.DB(DB_ENV)
		self.reverse_relation_index.set_flags(bdb.DB_DUP)
		self.reverse_relation_index.open(os.path.join(data_dir,"relations_reverse_relation_index.db"),"Reverse Relation Index", bdb.DB_BTREE, bdb.DB_CREATE)

		self.reverse_way_index = bdb.DB(DB_ENV)
		self.reverse_way_index.set_flags(bdb.DB_DUP)
		self.reverse_way_index.open(os.path.join(data_dir,"relations_reverse_way_index.db"),"Reverse Relation Index", bdb.DB_BTREE, bdb.DB_CREATE)

		#self.debug_print_db()

	def cleanup(self):
		self.db.close()
		self.reverse_relation_index.close()
		self.reverse_way_index.close()

	def debug_print_db(self):
		relation = Relation()

		print "Relation db:"

		cursor = self.db.cursor()
		data_pair = cursor.get(None, bdb.DB_FIRST)
		while data_pair:
			thrift_wrapper.from_string(relation, data_pair[1])
			print "%s -> %s" % (data_pair[0],relation)
			data_pair = cursor.get(None, bdb.DB_NEXT)
		cursor.close()

	def ping(self):
		pass

	def getRelation(self, id):
		relation = Relation()
		data = self.db.get("%d"%id)
		if data:
			thrift_wrapper.from_string(relation, data)
			return relation
		else:
			return None

	def getRelations(self, ids):
		relations = []
		for id in ids:
			relation = getRelation(id)
			if relation: relations.append(relation)
			else:
				return None
		return relations

	def getRelationVersion(self, id, version):
		relation = Relation()

		cursor = self.db.cursor()
		data_pair = cursor.get("%d"%id, bdb.DB_SET)
		while data_pair:
			thrift_wrapper.from_string(relation, data_pair[1])
		
			if relation.version == version:
				return relation

			data_pair = cursor.get("%d"%id, bdb.DB_NEXT_DUP)

		return None

	def getRelationHistory(self, id):
		relations = []

		cursor = self.db.cursor()
		data_pair = cursor.get("%d"%id, bdb.DB_SET)
		while data_pair:
			relation = Relation()
			thrift_wrapper.from_string(relation, data_pair[1])
			relations.append(relation)

			data_pair = cursor.get("%d"%id, bdb.DB_NEXT_DUP)

		return relations

	def getRelationsFromNode(self, node_id):
		cursor = self.reverse_node_index.cursor()
		relation_ids = set()
		relation_id_str_pair = cursor.get("%d"%node_id, bdb.DB_SET)
		while relation_id_str_pair:
			relation_ids.add(relation_id_str_pair[1])
			relation_id_str_pair = cursor.get("%d"%node_id, bdb.DB_NEXT_DUP)

		relations = []
		for relation_id_str in relation_ids:
			data = self.db.get(relation_id_str)
			if data:
				relation = Relation()
				thrift_wrapper.from_string(relation, data)
				relations.append(relation)
		return relations
	
	def getRelationsFromNodes(self, nodes):
		cursor = self.reverse_node_index.cursor()
		relation_ids = set()
		for node_id in nodes:
			relation_id_str_pair = cursor.get("%d"%node_id, bdb.DB_SET)
			while relation_id_str_pair:
				relation_ids.add(relation_id_str_pair[1])
				relation_id_str_pair = cursor.get("%d"%node_id, bdb.DB_NEXT_DUP)

		relations = []
		for relation_id_str in relation_ids:
			data = self.db.get(relation_id_str)
			if data:
				relation = Relation()
				thrift_wrapper.from_string(relation, data)
				relations.append(relation)
		return relations

	def getRelationsFromWay(self, way_id):
		cursor = self.reverse_way_index.cursor()
		relation_ids = set()
		relation_id_str_pair = cursor.get("%d"%way_id, bdb.DB_SET)
		while relation_id_str_pair:
			relation_ids.add(relation_id_str_pair[1])
			relation_id_str_pair = cursor.get("%d"%way_id, bdb.DB_NEXT_DUP)

		relations = []
		for relation_id_str in relation_ids:
			data = self.db.get(relation_id_str)
			if data:
				relation = Relation()
				thrift_wrapper.from_string(relation, data)
				relations.append(relation)
		return relations

	def getRelationsFromWays(self, ways):
		cursor = self.reverse_way_index.cursor()
		relation_ids = set()
		for way_id in ways:
			relation_id_str_pair = cursor.get("%d"%way_id, bdb.DB_SET)
			while relation_id_str_pair:
				relation_ids.add(relation_id_str_pair[1])
				relation_id_str_pair = cursor.get("%d"%way_id, bdb.DB_NEXT_DUP)

		relations = []
		for relation_id_str in relation_ids:
			data = self.db.get(relation_id_str)
			if data:
				relation = Relation()
				thrift_wrapper.from_string(relation, data)
				relations.append(relation)
		return relations

	def getRelationsFromRelation(self, relation_id):
		cursor = self.reverse_relation_index.cursor()
		relation_ids = set()
		relation_id_str_pair = cursor.get("%d"%relation_id, bdb.DB_SET)
		while relation_id_str_pair:
			relation_ids.add(relation_id_str_pair[1])
			relation_id_str_pair = cursor.get("%d"%relation_id, bdb.DB_NEXT_DUP)

		relations = []
		for relation_id_str in relation_ids:
			data = self.db.get(relation_id_str)
			if data:
				relation = Relation()
				thrift_wrapper.from_string(relation, data)
				relations.append(relation)
		return relations

	def createRelation(self, relation):
		lock = DB_ENV.lock_get(DB_ENV.lock_id(), "next_id_increment", bdb.DB_LOCK_WRITE)
		next_id = long(self.db.get("next_id"))
		self.db.delete("next_id")
		self.db.put("next_id", "%d"%(next_id+1))
		DB_ENV.lock_put(lock)

		relation.id = next_id
		relation.version = 1

		relation_id_str = "%d"%relation.id

		data = thrift_wrapper.to_string(relation)
		cursor = self.db.cursor()
		cursor.put(relation_id_str, data, bdb.DB_KEYFIRST)

		# Update indexes
		for member in relation.members:
			if member.node != None:
				self.reverse_node_index.put("%d"%member.node, relation_id_str)
			elif member.way != None:
				self.reverse_way_index.put("%d"%member.way, relation_id_str)
			elif member.relation != None:
				self.reverse_relation_index.put("%d"%member.relation, relation_id_str)

		return relation.id

	def deleteRelation(self, relation):
		relation_id_str = "%d"%relation.id

		# Get the last version
		cursor = self.db.cursor()
		data_pair = cursor.get(relation_id_str, bdb.DB_SET)
		if data_pair:
			old_relation = Relation()
			thrift_wrapper.from_string(old_relation, data_pair[1])
			
			# Indexes to remove
			reverse_node_cursor = self.reverse_node_index.cursor()
			for node_id in set(old_relation.nodes):
				print "Going to remove node %d from the index" % node_id
				relation_id_search_str_pair = reverse_node_cursor.get("%d"%node_id, bdb.DB_SET)
				while relation_id_search_str_pair:
					if relation_id_str == relation_id_search_str_pair[1]:
						print "...calling delete for %s -> %s" % relation_id_search_str_pair
						reverse_node_cursor.delete()
						break
					relation_id_search_str_pair = reverse_node_cursor.get("%d"%node_id, bdb.DB_NEXT_DUP)

			relation.visible = False
			relation.version = old_relation.version + 1 # This is bound to have concurrency issues
			data = thrift_wrapper.to_string(relation)
			cursor.put(relation_id_str, data, bdb.DB_KEYFIRST)

			return relation.id
		else:
			# There was no previous version!
			pass


	# FIXME: Bad things happen when there's duplicate node ids referenced by a node
	def editRelation(self, relation):
		relation_id_str = "%d"%relation.id

		# Get the last version
		cursor = self.db.cursor()
		data_pair = cursor.get(relation_id_str, bdb.DB_SET)
		if data_pair:
			old_relation = Relation()
			thrift_wrapper.from_string(old_relation, data_pair[1])
			
			# Indexes to add to
			for node_id in set(relation.nodes) - set(old_relation.nodes):
				self.reverse_node_index.put("%d"%node_id, relation_id_str)

			# Indexes to remove
			reverse_node_cursor = self.reverse_node_index.cursor()
			for node_id in set(old_relation.nodes) - set(relation.nodes):
				print "Going to remove node %d from the index" % node_id
				relation_id_search_str_pair = reverse_node_cursor.get("%d"%node_id, bdb.DB_SET)
				while relation_id_search_str_pair:
					if relation_id_str == relation_id_search_str_pair[1]:
						print "...calling delete for %s -> %s" % relation_id_search_str_pair
						reverse_node_cursor.delete()
						break
					relation_id_search_str_pair = reverse_node_cursor.get("%d"%node_id, bdb.DB_NEXT_DUP)

			relation.version = old_relation.version + 1 # This is bound to have concurrency issues
			data = thrift_wrapper.to_string(relation)
			cursor.put(relation_id_str, data, bdb.DB_KEYFIRST)

			return relation.id
		else:
			# There was no previous version!
			pass

