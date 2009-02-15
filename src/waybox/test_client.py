#!/usr/bin/env python

import sys
sys.path.append('gen-py')
sys.path.append('../common/gen-py')

from way import WayServer
from way.ttypes import *
from data.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

try:

	# Make socket
	transport = TSocket.TSocket('localhost', 9090)

	# Buffering is critical. Raw sockets are very slow
	transport = TTransport.TBufferedTransport(transport)

	# Wrap in a protocol
	protocol = TBinaryProtocol.TBinaryProtocol(transport)

	# Create a client to use the protocol encoder
	client = WayServer.Client(protocol)

	# Connect!
	transport.open()

	try:
		client.getWay(1)
	except:
		way = Way()
		way.id = 1
		way.user = "jason"
		way.nodes = [1,1,2,3,5,8,13]
		print "created way:", client.createWay(way)

	print "calling server for way '1'"
	way = client.getWay(1)

	print "result: ", way

	# Close!
	transport.close()

except Thrift.TException, tx:
	print '%s' % (tx.message)
