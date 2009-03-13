#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
sys.path.append('common/gen-py')
sys.path.append('common/')

from node import NodeServer

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from node_request_handler import NodeRequestHandler

# Don't buffer stdout
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

if len(sys.argv) > 1:
	port = int(sys.argv[1])
else:
	port = 9091
handler = NodeRequestHandler()
processor = NodeServer.Processor(handler)
transport = TSocket.TServerSocket(port)
tfactory = TTransport.TBufferedTransportFactory()
pfactory = TBinaryProtocol.TBinaryProtocolAcceleratedFactory()

#server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

# You could do one of these for a multithreaded server
#server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
#server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory)
server = TServer.TForkingServer(processor, transport, tfactory, pfactory)

print 'Starting the server...'
try:
	server.serve()
except Exception, e:
	handler.cleanup()
	raise e
print 'done.'

