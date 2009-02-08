#!/usr/bin/env python

import test_pb2

node = test_pb2.Node()
node.id = 1234
node.latitude = 45.0
node.longitude = 180.0

try:
	node.longitude = "hello"
except TypeError:
	pass 

print "node as string:", node.SerializeToString()

node2 = test_pb2.Node()
node2.ParseFromString(node.SerializeToString())
print "id:", node2.id
print "latitude:", node2.latitude
print "longitude:", node2.longitude

