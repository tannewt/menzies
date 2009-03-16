import sys
import urllib2
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
	base_url = "http://"+SERVER+":"+PORT+"/api/0.6/"
else:
	base_url = "http://api.openstreetmap.org/api/0.5/"

print "using",base_url
print

def fetch(url):
	start = time.time()
	urllib2.urlopen(base_url+url)
	duration = time.time() - start
	print url,duration,"secs"

url = "node/29972850"
fetch(url)

url = "node/39972850"
fetch(url)

#hansvile
url = "map?bbox=-122.5867,47.8938,-122.5481,47.9165"
fetch(url)

#downtown seattle
url = "map?bbox=-122.34363,47.59828,-122.32432,47.60968"
fetch(url)
