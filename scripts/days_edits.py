#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import math
import pickle

if len(sys.argv) < 2:
	print sys.argv[0],"<filename>"
	sys.exit(1)

f = open(sys.argv[1])
heat_map = pickle.load(f)
f.close()


print "drawing to node_heat_map.svg"
import cairo

surface = cairo.SVGSurface("node_heat_map.svg",360,180)
context = cairo.Context(surface)

context.set_line_width(0.1)
context.set_source_rgb(0.0,0.0,0.0)
context.rectangle(0,180,360,180)
context.stroke()

for x in range(360):
	for y in range(180):
		v = heat_map[x][y]
		if v!=0:
			context.set_source_rgb(0.0,1.0,0.0)
			context.rectangle(x,y,1,1)
			context.fill()
