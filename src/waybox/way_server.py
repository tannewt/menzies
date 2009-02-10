#!/usr/bin/env python

from bsddb import db as bdb

from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport

import sys
sys.path.append('gen-py')
from tutorial import ttypes

from cStringIO import StringIO
import pprint as pp


binary = StringIO()
transport = TTransport.TFileObjectTransport(binary)
protocol = TBinaryProtocol.TBinaryProtocol(transport)
work = ttypes.Work(5,10,ttypes.Operation.ADD)
work.write(protocol)
str_work = binary.getvalue()
binary.close()

db = bdb.DB()
db.set_flags(bdb.DB_DUP)
db.open("test.db","test", bdb.DB_BTREE, bdb.DB_CREATE)

db.put("1","1")
db.put("1","11")
db.put("3",str_work)

for k, v in db.items():
	pp.pprint(k+", "+v)

binary = StringIO(db.get("3"))
transport = TTransport.TFileObjectTransport(binary)
protocol = TBinaryProtocol.TBinaryProtocol(transport)
work = ttypes.Work()
work.read(protocol)
binary.close()

print "num1: ", work.num1
print "num2: ", work.num2
print "op: ", work.op

'''
c = db.cursor()
c.put("2","2", bdb.DB_KEYFIRST)
c.put("2","22", bdb.DB_KEYFIRST)

print db.get("2") # Now we get the last value inserted for this key (i.e., the latest version is always first)

db.sync()
'''

