#!/usr/bin/env python

import sys, time

import bz2

from xml.sax import make_parser, handler

THIS_SERVER = 2 # less than 0 for a way or relation server
LOAD_WAYS = False
LOAD_RELATIONS = False

sys.path.append('common')
sys.path.append('common/gen-py')
from data.ttypes import *

sys.path.append('front')
from partitioner import *
node_partitioner = StaticLatPartitioner(range(4))
raw_spatial_data = open("spatial_data.raw", "w")

sys.path.append('nodebox')
from node_request_handler import NodeRequestHandler
node_handler = NodeRequestHandler()

sys.path.append('waybox')
from way_request_handler import WayRequestHandler
way_handler = WayRequestHandler()

sys.path.append('relationbox')
from relation_request_handler import RelationRequestHandler
relation_handler = RelationRequestHandler()

class FancyCounter(handler.ContentHandler):

	def __init__(self):
		self._elems = 0
		self._attrs = 0
		self._elem_types = {}
		self._attr_types = {}

		self._object = None

	def startElement(self, name, attrs):
		self._elems += 1
		self._attrs += len(attrs)
		self._elem_types[name] = self._elem_types.get(name, 0) + 1

		for n in attrs.keys():
			self._attr_types[n] = self._attr_types.get(n, 0) + 1

		if name == "node":
			self._object = None
			if THIS_SERVER < 0: return

			self._object = Node()
			self._object.tags = {}
			self._object.id = long(attrs["id"])
			self._object.lat = float(attrs["lat"])
			self._object.lon = float(attrs["lon"])

			if attrs.has_key("changeset"): self._object.changeset = int(attrs["changeset"])
			if attrs.has_key("visible"): self._object.visible = attrs["visible"] == "true"
			if attrs.has_key("user"): self._object.user = attrs["user"].encode("utf-8")
			if attrs.has_key("uid"): self._object.uid = int(attrs["uid"])
			if attrs.has_key("version"): self._object.version = int(attrs["version"])
			if attrs.has_key("timestamp"): self._object.timestamp = time.mktime(time.strptime(attrs["timestamp"], "%Y-%m-%dT%H:%M:%SZ"))
		elif name == "way":
			self._object = None
			if not LOAD_WAYS: return

			self._object = Way()
			self._object.tags = {}
			self._object.nodes = []
			self._object.id = long(attrs["id"])
			if attrs.has_key("changeset"): self._object.changeset = int(attrs["changeset"])
			if attrs.has_key("visible"): self._object.visible = attrs["visible"] == "true"
			if attrs.has_key("user"): self._object.user = attrs["user"].encode("utf-8")
			if attrs.has_key("uid"): self._object.uid = int(attrs["uid"])
			if attrs.has_key("version"): self._object.version = int(attrs["version"])
			if attrs.has_key("timestamp"): self._object.timestamp = time.mktime(time.strptime(attrs["timestamp"], "%Y-%m-%dT%H:%M:%SZ"))
		elif name == "relation":
			self._object = None
			if not LOAD_RELATIONS: return

			self._object = Relation()
			self._object.tags = {}
			self._object.members = set()
			self._object.id = long(attrs["id"])
			if attrs.has_key("changeset"): self._object.changeset = int(attrs["changeset"])
			if attrs.has_key("visible"): self._object.visible = attrs["visible"] == "true"
			if attrs.has_key("user"): self._object.user = attrs["user"].encode("utf-8")
			if attrs.has_key("uid"): self._object.uid = int(attrs["uid"])
			if attrs.has_key("version"): self._object.version = int(attrs["version"])
			if attrs.has_key("timestamp"): self._object.timestamp = time.mktime(time.strptime(attrs["timestamp"], "%Y-%m-%dT%H:%M:%SZ"))
		elif name == "tag":
			if self._object == None: return
			self._object.tags[attrs["k"].encode("utf-8")] = attrs["v"].encode("utf-8")
		elif name == "nd":
			if self._object == None: return
			self._object.nodes.append(int(attrs["ref"]))
		elif name == "member":
			if self._object == None: return

			member = Member()
			if attrs.has_key("role"): member.role = attrs["role"]
			if attrs["type"] == "way":
				member.way = int(attrs["ref"])
			elif attrs["type"] == "relation":
				member.relation = int(attrs["ref"])
			elif attrs["type"] == "node":
				member.node = int(attrs["ref"])
			else:
				print "Unknown type in member element:", attrs["type"]
			self._object.members.add(member)
		else:
			print "Unknown element:", name

	def endElement(self, name):
		if self._elems % 20000 == 0:
			print "Processed %d elements" % self._elems

		# Store in Berkeley DB
		if name == "node":
			if self._object == None: return

			if node_partitioner.from_node(self._object)[0] == THIS_SERVER:
				node_id = node_handler.createNode(self._object)
				raw_spatial_data.write("%f:%f:%d\n"%(self._object.lat, self._object.lon, node_id))
		elif name == "way":
			if self._object == None: return
			way_handler.createWay(self._object)
		elif name == "relation":
			if self._object == None: return
			relation_handler.createRelation(self._object)



	def endDocument(self):
		print "There were", self._elems, "elements."
		print "There were", self._attrs, "attributes."

		print "---ELEMENT TYPES"
		for pair in  self._elem_types.items():
		    print "%20s %d" % pair

		print "---ATTRIBUTE TYPES"
		for pair in  self._attr_types.items():
		    print "%20s %d" % pair          

parser = make_parser()
parser.setContentHandler(FancyCounter())

if sys.argv[1][-4:]==".bz2":
	parser.parse(bz2.BZ2File(sys.argv[1]))
else:
	parser.parse(sys.argv[1])

raw_spatial_data.close()


