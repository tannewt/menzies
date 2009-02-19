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
		assert dom.documentElement.firstChild.tagName == "node"
		node = data.Node()
		e = dom.documentElement.firstChild
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

	def way_to_xml(self, doc, o):
		way = doc.createElement("way")
		way.setAttribute("id",str(o.id))
		way.setAttribute("user",o.user)
		if o.visible:
			way.setAttribute("visible","true")
		else:
			way.setAttribute("visible","false")
		for tag in o.tags:
			t = doc.createElement("tag")
			t.setAttribute("k",tag)
			t.setAttribute("v",o.tags[tag])
			way.appendChild(t)
		return way

	def relation_to_xml(self, doc, o):
		relation = doc.createElement("relation")
		relation.setAttribute("id",str(o.id))
		relation.setAttribute("user",o.user)
		if o.visible:
			relation.setAttribute("visible","true")
		else:
			relation.setAttribute("visible","false")
		for member in o.members:
			pass
		for tag in o.tags:
			t = doc.createElement("tag")
			t.setAttribute("k",tag)
			t.setAttribute("v",o.tags[tag])
			relation.appendChild(t)
		return relation
		
	def tags_to_xml(self, doc, o):
		
	
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
				print 8,"ways"
			elif bits[2]=="relations":
				print 9,"relations"
			else:
				print 7,"version",bits[3]
		elif bits[0]=="way":
			if len(bits)==2:
				print 11,"getWay(",long(bits[1]),")"
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
