#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
sys.path.append('common/gen-py')
sys.path.append('common/')

from way import WayServer

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from way_request_handler import WayRequestHandler

handler = WayRequestHandler()
processor = WayServer.Processor(handler)
transport = TSocket.TServerSocket(9090)
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

