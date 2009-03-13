#!/usr/bin/env python
# -*- coding: utf-8 -*-

from xml.sax import make_parser, handler

edit_heat_map = []
for i in range(360):
	edit_heat_map.append([0]*180)

class FancyCounter(handler.ContentHandler):
	def __init__(self):
		el = 0

	def startElement(self, name, attrs):
		el += 1
		if el%1000000:
			print "seen",el,"nodes"
		if name == "node":
			lat = 180-(int(float(attrs["lat"]))+90)
			lon = int(float(attrs["lon"]))+180
			edit_heat_map[lon][lat] += 1

	def endElement(self, name):
		pass

	def endDocument(self):
		pass         

parser = make_parser()

counter = FancyCounter()
parser.setContentHandler(counter)
parser.parse("planet.osm")

print "dumping to bin_counts.pickle"
f = open("bin_counts.pickle")

import cPickle as pickle
pickle.dump(edit_heat_map, f)
f.close()
