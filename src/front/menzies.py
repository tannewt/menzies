from node import NodeServer

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

class Menzies:
	SERVERS = {"node": [("localhost", 9091)]}
	
	def __init__(self):
		self.servers = {"node":[]}
		for s in self.SERVERS["node"]:
			transport = TSocket.TSocket('localhost', 9091)
			transport = TTransport.TBufferedTransport(transport)
			protocol = TBinaryProtocol.TBinaryProtocol(transport)
			client = NodeServer.Client(protocol)
			transport.open()
			self.servers["node"].append(client)
		self.next_node_id = 0
	
	def getNode(self,id):
		for s in self.servers["node"]:
			n = s.getNode(id)
			if n:
				return n
		return None
	
	def getNodeVersion(self, id, version):
		for s in self.servers["node"]:
			n = s.getNodeVersion(id, version)
			if n:
				return n
		return None
	
	def editNode(self, node):
		for s in self.servers["node"]:
			n = s.editNode(node)
			if n:
				return n
		return None
	
	def deleteNode(self, node):
		for s in self.servers["node"]:
			n = s.deleteNode(node)
			if n:
				return n
		return None
	
	def createNode(self, node):
		node.id = self.next_node_id
		self.next_node_id+=1
		
		self.servers["node"][0].createNode(node)
		return node.id
	
	def getNodeHistory(self, id):
		for s in self.servers["node"]:
			n = s.getNodeHistory(id)
			if n:
				return n
		return None
