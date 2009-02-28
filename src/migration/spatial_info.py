#!/usr/bin/env python

from bsddb import db as bdb

import pprint
import pickle

#########################################?
#45.550551, -124.733215
#49.002797, -116.913571


db = bdb.DB()
db.open("spatial_index_tmp.db","Spatial Index", bdb.DB_BTREE, 0)

print "spatial_index_tmp.db:"
stat = db.stat()
print "keys:", stat["nkeys"]
print "data:", stat["ndata"]

levels = int(db.get("levels"))+1
print "levels: %d" % levels

total_nodes = 0
for i in range(levels):
	splits = int(db.get("splits%d" % i))
	print "splits%d: %s" % (i, splits)

	for x in range(splits):
		for y in range(splits):
			data = db.get("%d-%d-%d" % (i,x,y))
			if data:
				min_lat, min_lon, max_lat, max_lon, pickled_ids = data.split(":")

				min_lat = float(min_lat)
				min_lon = float(min_lon)
				max_lat = float(max_lat)
				max_lon = float(max_lon)

				node_ids = pickle.loads(pickled_ids)
				if i == 0:
					total_nodes += len(node_ids)
				if splits < 10:
					print "(%f, %f, %f, %f) ids: %s" % (min_lat, min_lon, max_lat, max_lon, node_ids)

#########################################?

print "total nodes covered: %d" % total_nodes
