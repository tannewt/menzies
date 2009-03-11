# -*- coding: utf-8 -*-
from cStringIO import StringIO

from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport

protocol_factory = TBinaryProtocol.TBinaryProtocolAcceleratedFactory()

def to_string(thrift_obj):
	trans = TTransport.TMemoryBuffer()
	prot = protocol_factory.getProtocol(trans)
	thrift_obj.write(prot)
	return trans.getvalue()

def from_string(thrift_obj, data):
	prot = protocol_factory.getProtocol(TTransport.TMemoryBuffer(data))
	thrift_obj.read(prot)
