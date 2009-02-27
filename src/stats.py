#!/usr/bin/env python

from bsddb import db as bdb

import pprint

#########################################?
'''
db = bdb.DB()
db.set_flags(bdb.DB_DUP)
db.open("nodes.db","Nodes", bdb.DB_BTREE, 0)

print "nodes.db:"
pprint.pprint(db.stat())
'''
#########################################?

db = bdb.DB()
db.open("frontend.db","Frontend Data", bdb.DB_BTREE, 0)

print "frontend.db:"
stat = db.stat()
print "keys:", stat["nkeys"]
print "data:", stat["ndata"]
print "next node id:", db.get("next_node_id")

#########################################?

db = bdb.DB()
db.set_flags(bdb.DB_DUP)
db.open("relations.db","Relations", bdb.DB_BTREE, 0)

print "relations.db:"
stat = db.stat()
print "keys:", stat["nkeys"]
print "data:", stat["ndata"]
print "next id:", db.get("next_id")

#########################################?

db = bdb.DB()
db.set_flags(bdb.DB_DUP)
db.open("ways.db","Ways", bdb.DB_BTREE, 0)

print "ways.db:"
stat = db.stat()
print "keys:", stat["nkeys"]
print "data:", stat["ndata"]
print "next id:", db.get("next_id")

#########################################?
