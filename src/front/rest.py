#!/usr/bin/env python

import BaseHTTPServer, SocketServer
import sys
import base64

sys.path.append("common/gen-py/")
import data.ttypes as data
import menzies

from xml.dom.minidom import parseString,getDOMImplementation
from xml.dom import Node

impl = getDOMImplementation()
log = open("rest.log", "a")

class ThreadingHTTPServer(SocketServer.ThreadingTCPServer,BaseHTTPServer.HTTPServer):
	pass

class OpenStreetMapHandler (BaseHTTPServer.BaseHTTPRequestHandler):
	API_PREFIX="/api/0.6/"

	def log_message(self, format, *args):
		"""Log an arbitrary message.

		This is used by all other logging functions.  Override
		it if you have specific logging wishes.

		The first argument, FORMAT, is a format string for the
		message to be logged.  If the format string contains
		any % escapes requiring parameters, they should be
		specified as subsequent arguments (it's just like
		printf!).

		The client host and current date/time are prefixed to
		every message.
		"""

		log.write("%s - - [%s] %s\n" %
				         (self.address_string(),
				          self.log_date_time_string(),
				          format%args))
		log.flush()


	def parse_node_xml(self, e):
		assert e.tagName == "node"

		node = data.Node()
		node.id = long(e.getAttribute("id"))
		node.lat = float(e.getAttribute("lat"))
		node.lon = float(e.getAttribute("lon"))
		node.visible = e.getAttribute("visible")=="true"
		node.user = e.getAttribute("user").encode("utf-8")
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
		way.user = e.getAttribute("user").encode("utf-8")
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
		relation.user = e.getAttribute("user").encode("utf-8")
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
				relation.members.append(self.parse_member_xml(c))
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
		return self.parse_node_xml(dom.documentElement.firstChild)

	def way_to_xml(self, doc, o):
		way = doc.createElement("way")
		way.setAttribute("id",str(o.id))
		if o.user:
			way.setAttribute("user",o.user)
		if o.visible:
			way.setAttribute("visible","true")
		else:
			way.setAttribute("visible","false")
		for node_id in o.nodes:
			nd = doc.createElement("nd")
			nd.setAttribute("ref",str(node_id))
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
		return self.parse_way_xml(dom.documentElement.firstChild)


	def relation_to_xml(self, doc, o):
		print o
		relation = doc.createElement("relation")
		relation.setAttribute("id",str(o.id))
		relation.setAttribute("user",o.user)
		if o.visible:
			relation.setAttribute("visible","true")
		else:
			relation.setAttribute("visible","false")
		for member in o.members:
			m = doc.createElement("member")
			if member.node != None:
				m.setAttribute("type", "node")
				m.setAttribute("ref", str(member.node))
			elif member.way != None:
				m.setAttribute("type", "way")
				m.setAttribute("ref", str(member.way))
			elif member.relation != None:
				m.setAttribute("type", "relation")
				m.setAttribute("ref", str(member.relation))
			m.setAttribute("role", member.role)
			relation.appendChild(m)
		if o.tags:
			for tag in o.tags:
				t = doc.createElement("tag")
				t.setAttribute("k",tag)
				t.setAttribute("v",o.tags[tag])
				relation.appendChild(t)
		return relation	
	
	def relation_from_xml(self, s):
		dom = parseString(s)
		assert dom.documentElement.tagName == "osm"
		return self.parse_relation_xml(dom.documentElement.firstChild)

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
		
	#class MessageClass:
	#	def __init__(self, fp, seekable=0):
	#		line = fp.readline().strip()
	#		while line.strip()!="":
	#			print "\"",line,"\""
	#			line = fp.readline()
	#		sys.stdout.flush()
	#		self.dict = {}
	
	def do_PUT(self):
		global menzies
		xml_in = self.rfile.read(int(self.headers["content-length"])).strip('\0')
		if "authorization" in self.headers.dict:
			auth = base64.b64decode(self.headers.dict["authorization"].split()[1])
			user,passwd = auth.split(":")
			print "user",user,"passwd",passwd
			sys.stdout.flush()
		else:
			self.send_response(401)
			self.send_header("WWW-Authenticate", "Basic realm=\"menzies\"")
			self.end_headers()
			return
		bits,args = self.parse_path()
		if bits[0] == "node":
			if bits[1]=="create":
				node = self.node_from_xml(xml_in)
				print "createNode(",node,")"
				id = menzies.createNode(node)
			else:
				id = long(bits[1])
				print "editNode(",id,")"
				node = self.node_from_xml(xml_in)
				version = menzies.editNode(node)
		elif bits[0] == "way":
			if bits[1]=="create":
				way = self.way_from_xml(xml_in)
				print "createWay(",way,")"
				id = menzies.createWay(way)
			else:
				id = long(bits[1])
				print "editWay(",id,")"
				way = self.way_from_xml(xml_in)
				version = menzies.editWay(way)
		elif bits[0] == "relation" and bits[1]=="create":
			relation = self.relation_from_xml(xml_in)
			print "createRelation(",relation,")"
			id = menzies.createRelation(relation)
		elif bits[0] == "changeset":
			if bits[1]=="create":
				#way = self.way_from_xml(xml_in)
				print "createChangeset(",")"
				id = menzies.createChangeset(xml_in)
		else:
			# should send error code
			return

		print "id",id

		self.send_response(200)
		self.send_header("Content-type", "text/html")
		self.send_header("Content-length", len(str(id)))
		self.end_headers()
		self.wfile.write(str(id))

	def do_GET(self):
		bits,args = self.parse_path()
		print "headers",self.headers.headers,"path",self.path
		sys.stdout.flush()
		if bits[0]=="node":
			print bits
			if len(bits)==2:
				id = long(bits[1])
				print 2,"getNode(",id,")"
				n = menzies.getNode(id)
				if n:
					doc = impl.createDocument(None, "osm", None)
					root = doc.documentElement
					print n
					root.appendChild(self.node_to_xml(doc,n))

					xml_str = doc.toxml()
					self.send_response(200)
					self.send_header("Content-type", "text/xml")
					self.send_header("Content-length", str(len(xml_str)))
					self.end_headers()
					self.wfile.write(xml_str)
				else:					
					self.send_response(410)
					self.end_headers()
			elif bits[2]=="history":
				id = long(bits[1])
				print 6,"getNodeHistory(",id,")"
				n = menzies.getNodeHistory(id)
				if n:
					doc = impl.createDocument(None, "osm", None)
					root = doc.documentElement
					print n
					for node in n:
						root.appendChild(self.node_to_xml(doc,node))

					xml_str = doc.toxml()
					self.send_response(200)
					self.send_header("Content-type", "text/xml")
					self.send_header("Content-length", str(len(xml_str)))
					self.end_headers()
					self.wfile.write(xml_str)
				else:
					self.send_response(410)
					self.end_headers()					
			elif bits[2]=="ways":
				id = long(bits[1])
				print 8, "ways"
				ways = menzies.getWaysFromNode(id)
				if ways:
					doc = impl.createDocument(None, "osm", None)
					root = doc.documentElement
					print ways
					for way in ways:
						root.appendChild(self.way_to_xml(doc, way))
					self.send_response(200)
					self.send_header("Content-type", "text/xml")
					self.end_headers()
					self.wfile.write(doc.toxml())
				else:
					self.send_response(410)
					self.end_headers()					
			elif bits[2]=="relations":
				id = long(bits[1])
				print 9,"relations from node"
				relations = menzies.getRelationsFromNode(id)
				if relations:
					doc = impl.createDocument(None, "osm", None)
					root = doc.documentElement
					print relations
					for relation in relations:
						root.appendChild(self.relation_to_xml(doc, relation))

					xml_str = doc.toxml()
					self.send_response(200)
					self.send_header("Content-type", "text/xml")
					self.send_header("Content-length", str(len(xml_str)))
					self.end_headers()
					self.wfile.write(xml_str)
				else:
					self.send_response(410)
					self.end_headers()
			else:
				id = long(bits[1])
				version = int(bits[2])
				print 7,"version",bits[2]
				n = menzies.getNodeVersion(id, version)
				if n:
					doc = impl.createDocument(None, "osm", None)
					root = doc.documentElement
					print n
					root.appendChild(self.node_to_xml(doc, n))

					xml_str = doc.toxml()
					self.send_response(200)
					self.send_header("Content-type", "text/xml")
					self.send_header("Content-length", str(len(xml_str)))
					self.end_headers()
					self.wfile.write(xml_str)
				else:
					self.send_response(410)
					self.end_headers()
		elif bits[0]=="way":
			if len(bits)==2:
				id = long(bits[1])
				print 11,"getWay(",id,")"
				w = menzies.getWay(id)
				if w:
					doc = impl.createDocument(None, "osm", None)
					root = doc.documentElement
					print w
					root.appendChild(self.way_to_xml(doc,w))

					xml_str = doc.toxml()
					self.send_response(200)
					self.send_header("Content-type", "text/xml")
					self.send_header("Content-length", str(len(xml_str)))
					self.end_headers()
					self.wfile.write(xml_str)
				else:
					self.send_response(410)
					self.end_headers()
			elif bits[2]=="history":
				id = long(bits[1])
				print 15,"history"
				ways = menzies.getWayHistory(id)
				if ways:
					doc = impl.createDocument(None, "osm", None)
					root = doc.documentElement
					print ways
					for way in ways:
						root.appendChild(self.way_to_xml(doc, way))

					xml_str = doc.toxml()
					self.send_response(200)
					self.send_header("Content-type", "text/xml")
					self.send_header("Content-length", str(len(xml_str)))
					self.end_headers()
					self.wfile.write(xml_str)
				else:
					self.send_response(410)
					self.end_headers()	
			elif bits[2]=="full":
				id = long(bits[1])
				print 18,"full"
				osm = menzies.getWayFull(id)
				if osm:
					doc = impl.createDocument(None, "osm", None)
					root = doc.documentElement
					print osm
					for way in osm.ways:
						root.appendChild(self.way_to_xml(doc, way))
					for node in osm.nodes:
						root.appendChild(self.node_to_xml(doc, node))

					xml_str = doc.toxml()
					self.send_response(200)
					self.send_header("Content-type", "text/xml")
					self.send_header("Content-length", str(len(xml_str)))
					self.end_headers()
					self.wfile.write(xml_str)
				else:
					self.send_response(410)
					self.end_headers()
			elif bits[2]=="relations":
				print 17,"relations from way"
				id = long(bits[1])
				relations = menzies.getRelationsFromWay(id)
				if relations:
					doc = impl.createDocument(None, "osm", None)
					root = doc.documentElement
					print relations
					for relation in relations:
						root.appendChild(self.relation_to_xml(doc, relation))
					self.send_response(200)
					self.send_header("Content-type", "text/xml")
					self.end_headers()
					self.wfile.write(doc.toxml())
				else:
					self.send_response(410)
					self.end_headers()	
			else:
				id = long(bits[1])
				version = int(bits[2])
				print 16,"version", version
				way = menzies.getWayVersion(id, version)
				if way:
					doc = impl.createDocument(None, "osm", None)
					root = doc.documentElement
					print way
					root.appendChild(self.way_to_xml(doc, way))

					xml_str = doc.toxml()
					self.send_response(200)
					self.send_header("Content-type", "text/xml")
					self.send_header("Content-length", str(len(xml_str)))
					self.end_headers()
					self.wfile.write(xml_str)
				else:
					self.send_response(410)
					self.end_headers()	
		elif bits[0]=="relation":
			print bits
			if len(bits)==2:
				id = long(bits[1])
				print 2,"getRelation(",id,")"
				relation = menzies.getRelation(id)
				if relation:
					doc = impl.createDocument(None, "osm", None)
					root = doc.documentElement
					print relation
					root.appendChild(self.relation_to_xml(doc, relation))

					xml_str = doc.toxml()
					self.send_response(200)
					self.send_header("Content-type", "text/xml")
					self.send_header("Content-length", str(len(xml_str)))
					self.end_headers()
					self.wfile.write(xml_str)
				else:
					self.send_response(410)
					self.end_headers()
			elif bits[2]=="relations":
				id = long(bits[1])
				print "relations from relation"
				relations = menzies.getRelationsFromRelation(id)
				if relations:
					doc = impl.createDocument(None, "osm", None)
					root = doc.documentElement
					print relations
					for relation in relations:
						root.appendChild(self.relation_to_xml(doc, relation))

					xml_str = doc.toxml()
					self.send_response(200)
					self.send_header("Content-type", "text/xml")
					self.send_header("Content-length", str(len(xml_str)))
					self.end_headers()
					self.wfile.write(xml_str)
				else:
					self.send_response(410)
					self.end_headers()
			elif bits[2]=="full":
				id = long(bits[1])
				print "getRelationFull"
				osm = menzies.getRelationFull(id)
				if osm:
					doc = impl.createDocument(None, "osm", None)
					root = doc.documentElement
					print osm
					for relation in osm.relations:
						root.appendChild(self.relation_to_xml(doc, relation))
					for way in osm.ways:
						root.appendChild(self.way_to_xml(doc, way))
					for node in osm.nodes:
						root.appendChild(self.node_to_xml(doc, node))

					xml_str = doc.toxml()
					self.send_response(200)
					self.send_header("Content-type", "text/xml")
					self.send_header("Content-length", str(len(xml_str)))
					self.end_headers()
					self.wfile.write(xml_str)
				else:
					self.send_response(410)
					self.end_headers()
		elif bits[0]=="changeset":
			print "changeset"
		elif bits[0]=="map":
			args = args["bbox"]
			box = data.BBox(min_lat=float(args[1]),max_lat=float(args[3]),min_lon=float(args[0]),max_lon=float(args[2]))

			if box.max_lat - box.min_lat > 0.25 or box.max_lon - box.min_lon > 0.25:
				self.send_response(405) # FIXME: What's the right response?
				self.end_headers()
				return

			osm = menzies.getAllInBounds(box)
			if osm:
				doc = impl.createDocument(None, "osm", None)
				root = doc.documentElement
				print str(osm)[:100], "..."
				print "Nodes: %d" % len(osm.nodes)
				print "Ways: %d" % len(osm.ways)
				print "Relations: %d" % len(osm.relations)
				for relation in osm.relations:
					root.appendChild(self.relation_to_xml(doc, relation))
				for way in osm.ways:
					root.appendChild(self.way_to_xml(doc, way))
				for node in osm.nodes:
					root.appendChild(self.node_to_xml(doc, node))

				try:
					xml_str = doc.toxml()
				except:
					print "Failed to convert document to XML"
					print str(osm)
				else:
					self.send_response(200)
					self.send_header("Content-type", "text/xml")
					self.send_header("Content-length", str(len(xml_str)))
					self.end_headers()
					self.wfile.write(xml_str)
			else:
				self.send_response(410)
				self.end_headers()
			print "getAll(",box,")"
	
	def do_DELETE(self):
		print self.command, self.path, self.headers
		bits, args = self.parse_path()
		if bits[0] == "node":
			id = long(bits[1])
			print "deleteNode(",id,")"

			version = menzies.deleteNode(id)

			self.send_response(200)
			self.send_header("Content-type", "text/html")
			self.send_header("Content-length", len(str(version)))
			self.end_headers()
			self.wfile.write(str(version))
		elif bits[0] == "way":
			id = long(bits[1])
			print "deleteWay(",id,")"

			version = menzies.deleteWay(id)

			self.send_response(200)
			self.send_header("Content-type", "text/html")
			self.send_header("Content-length", len(str(version)))
			self.end_headers()
			self.wfile.write(str(version))

if __name__=="__main__":
	# Don't buffer stdout
	# sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

	if len(sys.argv) > 1:
		server_conf = open(sys.argv[1])

		servers = {}
		servers["node"] = []

		for line in server_conf.xreadlines():
			line = line.strip()
			if line == "":
				continue

			if line in ("[Nodes]", "[Way]", "[Relation]"):
				section = line
			elif section:
				info = line.split(":")
				info[1] = int(info[1])
				if section == "[Nodes]":
					servers["node"].append(info)
				elif section == "[Way]":
					servers["way"] = info
				elif section == "[Relation]":
					servers["relation"] = info
	else:
		num_nodeservers = 1
		node_servers = map(lambda x: ("localhost", 9100+x), range(num_nodeservers))
		servers={"node": node_servers,"way":('localhost','9090'),"relation":('localhost','9092')}

	print "Read server configuration:"
	print servers

	menzies = menzies.Menzies(servers)
	httpd = ThreadingHTTPServer(("",8001),OpenStreetMapHandler)
	try:
		httpd.serve_forever()
	except Exception, e:
		print "Cleaning up"
		menzies.cleanup()
		raise e

