import sys
import httplib

OSM = "--osm" in sys.argv
if OSM:
  sys.argv.remove("--osm")

if len(sys.argv)<3:
  print sys.argv[0],"<server>","<port>"

SERVER = sys.argv[1]
PORT = sys.argv[2]

# test create
c = httplib.HTTPConnection(SERVER, PORT)
node="""<osm><node id="-1" lat="61.8083953857422" lon="10.8497076034546" visible="true" timestamp="2005-07-30T14:27:12+01:00">
   <tag k="tourism" v="hotel" />
   <tag k="name" v="Cockroach Inn" />
</node></osm>"""
c.request("PUT", "/api/0.6/node/create", node, {"Authorization":"Basic lkajslgjls"})
r = c.getresponse()
id = r.read()
print "node/create",r.status, r.reason,"id",id

c.request("GET", "/api/0.6/node/"+str(id))
r = c.getresponse()
node = r.read()
print "node/"+id,r.status, r.reason
print node
