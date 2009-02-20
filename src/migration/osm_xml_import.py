#!/usr/bin/env python

import sys, time

from xml.sax import make_parser, handler

sys.path.append('../common')
sys.path.append('../common/gen-py')
from data.ttypes import *

sys.path.append('../nodebox')
from node_request_handler import NodeRequestHandler
node_handler = NodeRequestHandler()

sys.path.append('../waybox')
from way_request_handler import WayRequestHandler
way_handler = WayRequestHandler()

sys.path.append('../relationbox')
from relation_request_handler import RelationRequestHandler
relation_handler = RelationRequestHandler()

#import rtree
#index = rtree.Rtree("spatial", pagesize=8)

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
			self._object = Node()
			self._object.tags = {}
			self._object.id = int(attrs["id"])
			self._object.lat = float(attrs["lat"])
			self._object.lon = float(attrs["lon"])

			#index.add(self._object.id, (self._object.lat, self._object.lon))

			if attrs.has_key("changeset"): self._object.changeset = int(attrs["changeset"])
			if attrs.has_key("visible"): self._object.visible = attrs["visible"] == "true"
			if attrs.has_key("user"): self._object.user = attrs["user"].encode("utf-8")
			if attrs.has_key("uid"): self._object.uid = int(attrs["uid"])
			if attrs.has_key("version"): self._object.version = int(attrs["version"])
			if attrs.has_key("timestamp"): self._object.timestamp = time.mktime(time.strptime(attrs["timestamp"], "%Y-%m-%dT%H:%M:%SZ"))
		elif name == "way":
			self._object = Way()
			self._object.tags = {}
			self._object.nodes = []
			self._object.id = int(attrs["id"])
			if attrs.has_key("changeset"): self._object.changeset = int(attrs["changeset"])
			if attrs.has_key("visible"): self._object.visible = attrs["visible"] == "true"
			if attrs.has_key("user"): self._object.user = attrs["user"].encode("utf-8")
			if attrs.has_key("uid"): self._object.uid = int(attrs["uid"])
			if attrs.has_key("version"): self._object.version = int(attrs["version"])
			if attrs.has_key("timestamp"): self._object.timestamp = time.mktime(time.strptime(attrs["timestamp"], "%Y-%m-%dT%H:%M:%SZ"))
		elif name == "relation":
			self._object = Relation()
			self._object.tags = {}
			self._object.members = set()
			self._object.id = int(attrs["id"])
			if attrs.has_key("changeset"): self._object.changeset = int(attrs["changeset"])
			if attrs.has_key("visible"): self._object.visible = attrs["visible"] == "true"
			if attrs.has_key("user"): self._object.user = attrs["user"].encode("utf-8")
			if attrs.has_key("uid"): self._object.uid = int(attrs["uid"])
			if attrs.has_key("version"): self._object.version = int(attrs["version"])
			if attrs.has_key("timestamp"): self._object.timestamp = time.mktime(time.strptime(attrs["timestamp"], "%Y-%m-%dT%H:%M:%SZ"))
		elif name == "tag":
			self._object.tags[attrs["k"].encode("utf-8")] = attrs["v"].encode("utf-8")
		elif name == "nd":
			self._object.nodes.append(int(attrs["ref"]))
		elif name == "member":
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
		# Store in Berkeley DB
		if name == "node":
			node_handler.createNode(self._object)
		elif name == "way":
			way_handler.createWay(self._object)
		elif name == "relation":
			relation_handler.createRelation(self._object)

		if self._elems % 20000 == 0:
			print "Processed %d elements" % self._elems


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
parser.parse(sys.argv[1])

