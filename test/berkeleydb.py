#!/usr/bin/env python

import bsddb

db = bsddb.btopen(None, 'c')
for i in range(10):
	db['%d'%i] = '%d'% (i*i)

for k, v in db.iteritems():
	print k, v

