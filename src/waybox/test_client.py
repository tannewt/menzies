#!/usr/bin/env python

import sys
sys.path.append('gen-py')

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

	way = client.getWay(1)
	if way == None:
		way = Way()
		way.id = 1
		way.user = "jason"
		print "created way:", client.createWay(way)

	way = client.getWay(1)
	print way

	# Close!
	transport.close()

except Thrift.TException, tx:
	print '%s' % (tx.message)
