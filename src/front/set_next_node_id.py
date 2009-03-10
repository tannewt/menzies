#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

from bsddb import db as bdb
import sys, os

db = bdb.DB()
db.open("frontend.db","Frontend Data", bdb.DB_BTREE, 0)

old_id = db.get("next_node_id")
if old_id != None:
	print "Old next id was", old_id

db.put("next_node_id", "%d" % int(sys.argv[1]))
