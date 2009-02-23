#!/usr/bin/env python

import sys
sys.path.append('common/gen-py')
sys.path.append('common/')

from relation import RelationServer

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from relation_request_handler import RelationRequestHandler

handler = RelationRequestHandler()
processor = RelationServer.Processor(handler)
transport = TSocket.TServerSocket(9092)
tfactory = TTransport.TBufferedTransportFactory()
pfactory = TBinaryProtocol.TBinaryProtocolFactory()

#server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

# You could do one of these for a multithreaded server
#server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
#server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory)
server = TServer.TForkingServer(processor, transport, tfactory, pfactory)

print 'Starting the server...'
server.serve()
print 'done.'

