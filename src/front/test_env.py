#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
sys.path.append('../common')
sys.path.append('../common/gen-py')

from node import NodeServer
from node.ttypes import *
from data.ttypes import *

from menzies import Menzies

server_conf = open('../servers.conf')

servers = {}
servers["node"] = []

for line in server_conf.xreadlines():
	line = line.strip()
	if line == "":
		continue

	if line in ("[Nodes]", "[Way]", "[Relation]"):
		section = line
	elif section:
		info = line.split(":")
		info[1] = int(info[1])
		if section == "[Nodes]":
			servers["node"].append(info)
		elif section == "[Way]":
			servers["way"] = info
		elif section == "[Relation]":
			servers["relation"] = info

print "Read server configuration:"
print servers

menzies = Menzies(servers)

bounds = BBox()

bounds.min_lat = 48.956538
bounds.max_lat = 48.959046

bounds.min_lon = -122.434816
bounds.max_lon = -122.424045

#menzies.getAll(bounds)
