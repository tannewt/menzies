from BaseHTTPServer import *

class OpenStreetMapHandler (BaseHTTPRequestHandler):
	API_PREFIX="/api/0.6/"
	
	def to_xml(self, o):
		return ""
	
	def from_xml(self, s):
		return
	
	def do_PUT(self):
		pass
	
	def do_GET(self):
		path = self.path[len(self.API_PREFIX):]
		bits = path.split("/")
		bits[-1],tmp_args = bits[-1].split("?")
		args = {}
		tmp_args = map(lambda x: x.split("=",1),tmp_args.split("&"))
		for k,v in tmp_args:
			if "," in v:
				args[k] = v.split(",")
			else:
				args[k] = v
		print self.command,bits,args
		if bits[0]=="node":
			if len(bits)==2:
				print 2,"getNode(",long(bits[1]),")"
			elif bits[2]=="history":
				print 6,"history"
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
	
	def do_PUT(self):
		pass

if __name__=="__main__":
	httpd = HTTPServer(("",8001),OpenStreetMapHandler)
	httpd.serve_forever()
