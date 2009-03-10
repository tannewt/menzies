import sys
import httplib
import time

OSM = "--osm" in sys.argv
if OSM:
  sys.argv.remove("--osm")

if not OSM and len(sys.argv)<3:
  print sys.argv[0],"<server>","<port>"

if not OSM:
  SERVER = sys.argv[1]
  PORT = sys.argv[2]

# test create
if not OSM:
	c = httplib.HTTPConnection(SERVER, PORT)
	api = "0.6"
else:
	c = httplib.HTTPConnection("api.openstreetmap.org",80)
	api = "0.5"

#node="""<osm><node id="-1" lat="61.8083953857422" lon="10.8497076034546" visible="true" timestamp="2005-07-30T14:27:12+01:00">
#   <tag k="tourism" v="hotel" />
#   <tag k="name" v="Cockroach Inn" />
#</node></osm>"""
#c.request("PUT", "/api/0.6/node/create", node, {"Authorization":"Basic lkajslgjls"})
#r = c.getresponse()
#id = r.read()
#print "node/create",r.status, r.reason,"id",id

url = "/api/"+api+"/node/29972850"
start = time.time()
c.request("GET", url)
r = c.getresponse()
node = r.read()
duration = time.time() - start
print "Get node:",duration,"secs"
print r.status, r.reason
print node

url = "/api/"+api+"/map?bbox=-122.5867,47.8938,-122.5481,47.9165"
print "start"
start = time.time()
c.request("GET", url)
r = c.getresponse()
node = r.read()
duration = time.time() - start
print "Get hansville bbox:",duration,"secs"
print r.status, r.reason
print

url = "/api/"+api+"/map?bbox=-122.34363,47.59828,-122.32432,47.60968"
print "start"
start = time.time()
c.request("GET", url)
r = c.getresponse()
node = r.read()
duration = time.time() - start
print "Get seattle bbox:",duration,"secs"
print r.status, r.reason
