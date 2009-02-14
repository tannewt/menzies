#!/usr/bin/env python

import sys
sys.path.append('gen-py')
sys.path.append('../common/gen-py')

from node import NodeServer
from node.ttypes import *
from data.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

try:

	# Make socket
	transport = TSocket.TSocket('localhost', 9091)

	# Buffering is critical. Raw sockets are very slow
	transport = TTransport.TBufferedTransport(transport)

	# Wrap in a protocol
	protocol = TBinaryProtocol.TBinaryProtocol(transport)

	# Create a client to use the protocol encoder
	client = NodeServer.Client(protocol)

	# Connect!
	transport.open()

	try:
		client.getNode(2)
	except:
		node = Node()
		node.id = 2
		node.user = "jason"
		print "created node:", client.createNode(node)

	print "calling server for node '2'"
	node = client.getNode(2)

	print "result: ", node

	# Close!
	transport.close()

except Thrift.TException, tx:
	print '%s' % (tx.message)
