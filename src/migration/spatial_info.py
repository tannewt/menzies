#!/usr/bin/env python

from bsddb import db as bdb

import pprint

#########################################?

db = bdb.DB()
db.open("spatial_index_tmp.db","Spatial Index", bdb.DB_BTREE, 0)

print "spatial_index_tmp.db:"
stat = db.stat()
print "keys:", stat["nkeys"]
print "data:", stat["ndata"]

splits = int(float(db.get("splits")))
print "splits:", str(splits)

'''
for x in range(splits):
	for y in range(splits):
		print db.get("0-%d-%d" % (x,y))
'''
#########################################?
