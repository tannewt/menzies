#!/usr/bin/env python

import sys
sys.path.append('gen-py')
sys.path.append('../common/gen-py')

from relation import RelationServer
from relation.ttypes import *
from data.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

try:

	# Make socket
	transport = TSocket.TSocket('localhost', 9092)

	# Buffering is critical. Raw sockets are very slow
	transport = TTransport.TBufferedTransport(transport)

	# Wrap in a protocol
	protocol = TBinaryProtocol.TBinaryProtocol(transport)

	# Create a client to use the protocol encoder
	client = RelationServer.Client(protocol)

	# Connect!
	transport.open()

	try:
		client.getRelation(1)
	except:
		relation = Relation()
		relation.id = 1
		relation.user = "jason"
		relation.ways = [1,1,2,3,5,8,13]
		print "created relation:", client.createRelation(relation)

	print "calling server for relation '1'"
	relation = client.getRelation(1)

	print "result: ", relation

	# Close!
	transport.close()

except Thrift.TException, tx:
	print '%s' % (tx.message)

