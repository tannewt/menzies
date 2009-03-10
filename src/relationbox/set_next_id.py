#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

from bsddb import db as bdb
import sys, os

db = bdb.DB()
db.set_flags(bdb.DB_DUP)
db.open("relations.db","Relations", bdb.DB_BTREE, 0)

old_id = db.get("next_id")
if old_id != None:
	print "Old next id was", old_id
	db.delete("next_id")

db.put("next_id", "%d" % int(sys.argv[1]))
