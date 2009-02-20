#!/usr/bin/env python

from BaseHTTPServer import *
import sys

sys.path.append("common/gen-py/")
import data.ttypes as data
import menzies

from xml.dom.minidom import parseString,getDOMImplementation
from xml.dom import Node

menzies = menzies.Menzies()
impl = getDOMImplementation()

class OpenStreetMapHandler (BaseHTTPRequestHandler):
	API_PREFIX="/api/0.6/"

	def parse_node_xml(self, e):
		assert e.tagName == "node"

		node = data.Node()
		node.id = long(e.getAttribute("id"))
		node.lat = float(e.getAttribute("lat"))
		node.lon = float(e.getAttribute("lon"))
		node.visible = e.getAttribute("visible")=="true"
		#node.timestamp = e.getAttribute("timestamp")
		node.timestamp = 9999
		node.tags = {}
		for c in e.childNodes:
			if c.nodeType == Node.TEXT_NODE:
				continue
			assert c.tagName == "tag"
			node.tags[c.getAttribute("k")] = c.getAttribute("v")
		return node		

	def parse_way_xml(self, e):
		assert e.tagName == "way"

		way = data.Way()
		way.id = long(e.getAttribute("id"))
		way.visible = e.getAttribute("visible")=="true"
		#way.timestamp = e.getAttribute("timestamp")
		way.timestamp = 9999
		way.tags = {}
		way.nodes = []
		for c in e.childNodes:
			if c.nodeType == Node.TEXT_NODE:
				continue
			if c.tagName == "tag":
				way.tags[c.getAttribute("k")] = c.getAttribute("v")
			elif c.tagName == "nd":
				way.nodes.append(long(c.getAttribute("ref")))
		return way

	def parse_relation_xml(self, e):
		assert e.tagName == "relation"

		relation = data.Relation()
		relation.id = long(e.getAttribute("id"))
		relation.visible = e.getAttribute("visible")=="true"
		#relation.timestamp = e.getAttribute("timestamp")
		relation.timestamp = 9999
		relation.tags = {}
		relation.members = []
		for c in e.childNodes:
			if c.nodeType == Node.TEXT_NODE:
				continue
			if c.tagName == "tag":
				relation.tags[c.getAttribute("k")] = c.getAttribute("v")
			elif c.tagName == "member":
				relations.members.append(parse_member_xml(c))
		return relation

	def parse_member_xml(self, e):
		assert e.tagName == "member"

		member = data.Member()
		member.role = e.getAttribute("role")
		member_type = e.getAttribute("type")
		if member_type == "node":
			member.node = long(e.getAttribute("ref"))
		elif member_type == "way":
			member.way = long(e.getAttribute("ref"))
		elif member_type == "relation":
			member.relation = long(e.getAttribute("ref"))		

		return member
	
	def node_to_xml(self, doc, o):
		node = doc.createElement("node")
		#node.tagName = "node"
		node.setAttribute("id",str(o.id))
		node.setAttribute("lat",str(o.lat))
		node.setAttribute("lon",str(o.lon))
		if o.visible:
			node.setAttribute("visible","true")
		else:
			node.setAttribute("visible","false")
		for tag in o.tags:
			t = doc.createElement("tag")
			t.setAttribute("k",tag)
			t.setAttribute("v",o.tags[tag])
			node.appendChild(t)
		return node
	
	def node_from_xml(self, s):
		dom = parseString(s)
		assert dom.documentElement.tagName == "osm"
		return parse_node_xml(dom.documentElement.firstChild)

	def way_to_xml(self, doc, o):
		way = doc.createElement("way")
		way.setAttribute("id",str(o.id))
		way.setAttribute("user",o.user)
		if o.visible:
			way.setAttribute("visible","true")
		else:
			way.setAttribute("visible","false")
		for n in o.nodes:
			nd = doc.createElement("nd")
			nd.setAttribute("ref",str(n.id))
			way.appendChild(nd)
		for tag in o.tags:
			t = doc.createElement("tag")
			t.setAttribute("k",tag)
			t.setAttribute("v",o.tags[tag])
			way.appendChild(t)
		return way

	def way_from_xml(self, s):
		dom = parseString(s)
		assert dom.documentElement.tagName == "osm"
		return parse_way_xml(dom.documentElement.firstChild)


	def relation_to_xml(self, doc, o):
		relation = doc.createElement("relation")
		relation.setAttribute("id",str(o.id))
		relation.setAttribute("user",o.user)
		if o.visible:
			relation.setAttribute("visible","true")
		else:
			relation.setAttribute("visible","false")
		for member in o.members:
			m = doc.createElement("member")
			for n in member.nodes:
				m.setAttribute("type", "node")
				m.setAttribute("ref", str(n.id))
			for w in member.ways:
				m.setAttribute("type", "way")
				m.setAttribute("ref", str(w.id))
			for r in member.relations:
				m.setAttribute("type", "relation")
				m.setAttribute("ref", str(r.id))
			m.setAttribute("role", m.role)
			relation.appendChild(m)
		for tag in o.tags:
			t = doc.createElement("tag")
			t.setAttribute("k",tag)
			t.setAttribute("v",o.tags[tag])
			relation.appendChild(t)
		return relation	
	
	def relation_from_xml(self, s):
		dom = parseString(s)
		assert dom.documentElement.tagName == "osm"
		return parse_relation_xml(dom.documentElement.firstChild)

	def parse_path(self):
		path = self.path[len(self.API_PREFIX):]
		path = path.strip("/")
		bits = path.split("/")
		args = {}
		
		# parse args
		if "?" in bits[-1]:
			bits[-1],tmp_args = bits[-1].split("?")
			tmp_args = map(lambda x: x.split("=",1),tmp_args.split("&"))
			for k,v in tmp_args:
				if "," in v:
					args[k] = v.split(",")
				else:
					args[k] = v
		return (bits,args)
	
	def do_HEAD(self):
		print "HEAD"
	
	def do_POST(self):
		print "POST"
	
	def do_PUT(self):
		global menzies
		print self.command, self.path,self.headers
		xml_in = self.rfile.read(int(self.headers["content-length"]))
		print xml_in
		bits,args = self.parse_path()
		if bits[0] == "node" and bits[1]=="create":
			node = self.node_from_xml(xml_in)
			print "createNode(",node,")"
			id = menzies.createNode(node)
			print "id",id
		self.send_response(200)
		self.end_headers()
		self.wfile.write(str(id))
	
	def do_GET(self):
		bits,args = self.parse_path()
		if bits[0]=="node":
			print bits
			if len(bits)==2:
				id = long(bits[1])
				print 2,"getNode(",id,")"
				n = menzies.getNode(id)
				doc = impl.createDocument(None, "osm", None)
				root = doc.documentElement
				print n
				root.appendChild(self.node_to_xml(doc,n))
				self.send_response(200)
				self.end_headers()
				self.wfile.write(doc.toxml())
			elif bits[2]=="history":
				id = long(bits[1])
				print 6,"getNodeHistory(",id,")"
				n = menzies.getNodeHistory(id)
				doc = impl.createDocument(None, "osm", None)
				root = doc.documentElement
				print n
				for node in n:
					root.appendChild(self.node_to_xml(doc,node))
				self.send_response(200)
				self.end_headers()
				self.wfile.write(doc.toxml())
			elif bits[2]=="ways":
				id = long(bits[1])
				print 8, "ways"
				ways = menzies.getWaysFromNode(id)
				doc = impl.createDocument(None, "osm", None)
				root = doc.documentElement
				print ways
				for way in ways:
					root.appendChild(self.way_to_xml(doc, way))
				self.send_response(200)
				self.end_headers()
				self.wfile.write(doc.toxml())
			elif bits[2]=="relations":
				id = long(bits[1])
				print 9,"relations"
				relations = menzies.getRelationsFromNode(id)
				doc = impl.createDocument(None, "osm", None)
				root = doc.documentElement
				print relations
				for relation in relations:
					root.appendChild(self.relation_to_xml(doc, relation))
				self.send_response(200)
				self.end_headers()
				self.wfile.write(doc.toxml())
			else:
				id = long(bits[1])
				version = int(bits[2])
				print 7,"version",bits[2]
				n = menzies.getNodeVersion(id, version)
				doc = impl.createDocument(None, "osm", None)
				root = doc.documentElement
				print n
				root.appendChild(self.node_to_xml(doc, n))
				self.send_response(200)
				self.end_headers()
				self.wfile.write(doc.toxml())
		elif bits[0]=="way":
			if len(bits)==2:
				print 11,"getWay(",long(bits[1]),")"
				w = menzies.getWay(id)
				doc = impl.createDocument(None, "osm", None)
				root = doc.documentElement
				print w
				root.appendChild(self.way_to_xml(doc,w))
				self.send_response(200)
				self.end_headers()
				self.wfile.write(doc.toxml())
			elif bits[2]=="history":
				print 15,"history"
			elif bits[2]=="full":
				print 18,"full"
			elif bits[2]=="relations":
				print 17,"relations"
			else:
				print 16,"version",bits[3]
		elif bits[0]=="relation":
			print "relation"
		elif bits[0]=="changeset":
			print "changeset"
		elif bits[0]=="map":
			args = args["bbox"]
			box = data.BBox(min_lat=args[1],max_lat=args[3],min_lon=args[0],max_lon=args[2])
			print "getAll(",box,")"
	
	def do_DELETE(self):
		pass

if __name__=="__main__":
	httpd = HTTPServer(("",8001),OpenStreetMapHandler)
	httpd.serve_forever()
