from cStringIO import StringIO

from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport

def to_string(thrift_obj):
	buf = StringIO()
	transport = TTransport.TFileObjectTransport(buf)
	protocol = TBinaryProtocol.TBinaryProtocol(transport)
	thrift_obj.write(buf)
	return buf.getvalue()

def from_string(thrift_obj, data):
	buf = StringIO(data)
	transport = TTransport.TFileObjectTransport(buf)
	protocol = TBinaryProtocol.TBinaryProtocol(transport)
	thrift_obj.write(protocol)

