#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import subprocess
import gzip
import os
import math

sys.path.append("src/front/")

from partitioner import StaticLatPartitioner

from xml.sax import make_parser, handler

if len(sys.argv) < 2:
	print sys.argv[0],"yyyymmdd"
	sys.exit(1)

date = int(sys.argv[1])

class Node:
	def __init__(self, lat, lon):
		self.lat = lat
		self.lon = lon

class FancyCounter(handler.ContentHandler):
	def __init__(self):
		self.points = []

	def startElement(self, name, attrs):
		if name == "node":
			self.points.append(Node(float(attrs["lat"]),float(attrs["lon"])))

	def endElement(self, name):
		pass

	def endDocument(self):
		pass         

parser = make_parser()

edits = []
for i in range(24):
	date1 = date*100+i
	if i<23:
		date2 = date*100+i+1
	else:
		date2 = (date+1)*100
	fn = str(date1)+"-"+str(date2)+".osc.gz"
	if not os.path.exists(fn):
		url = "http://planet.openstreetmap.org/hourly/"+fn
		print url
		subprocess.call(["wget",url])
	
	counter = FancyCounter()
	parser.setContentHandler(counter)
	parser.parse(gzip.GzipFile(fn))
	
	edits.append(counter.points)

hour_heat_map = []
for hour in range(24):
	hour_heat_map.append([])
	for i in range(360):
		hour_heat_map[hour].append([0]*180)

max_total = 0
for hour in range(24):
	for edit in edits[hour]:
		lat = 180-(int(edit.lat)+90)
		lon = int(edit.lon)+180
		hour_heat_map[hour][lon][lat] += 1
		max_total = max(max_total, hour_heat_map[hour][lon][lat])

print "max bin", max_total

#SERVERS = 3
#part = StaticLatPartitioner(range(SERVERS))
#server_per_hour = []
#for server in range(SERVERS):
#	server_per_hour.append([])
#	for hour in range(24):
#		server_per_hour[server].append(filter(lambda x: x[0]==server,map(part.from_node,edits[hour])))
	
hour_sum = map(len,edits)
#for hour in range(24):
#	print hour,"\t",hour_sum[hour],"\t",
#	for server in range(SERVERS):
#		print len(server_per_hour[server][hour]),"\t",
#	print
sum_all = sum(hour_sum)
print sum_all

print "drawing to edits.svg"
import cairo

max_total = math.log(max_total)
surface = cairo.SVGSurface("edits.svg",360,180*24)
context = cairo.Context(surface)

for hour in range(24):
	context.set_line_width(0.1)
	context.set_source_rgb(0.0,0.0,0.0)
	context.rectangle(0,180*hour,360,180)
	context.stroke()
	
	for timezone in range(24):
		this_time = abs(timezone+hour+12)%24
		if this_time < 6 or this_time > 18:
			context.set_source_rgba(0.5,0.5,0.5)
		else:
			context.set_source_rgba(1.0,1.0,1.0)
		
		context.rectangle(timezone*360/24-0.1,180*hour,360/24+0.2,180)
		context.fill()
	
	
	context.set_source_rgb(0.0,0.0,0.0)
	context.set_line_width(0.0)
	context.move_to(0,180*(hour+1))
	context.show_text(str(hour))
	
	for x in range(360):
		for y in range(180):
			v = hour_heat_map[hour][x][y]
			if v!=0:
				p = 1-math.log(float(v))/max_total
				context.set_source_rgb(1.0,p,p)
				context.rectangle(x,y+180*hour,1,1)
				context.fill()
	
	#context.set_source_rgb(0.0,0.0,0.0)
	#i = 0
	#while i < len(edits[hour]):
	#	x = edits[hour][i].lon+180
	#	y = 180-(edits[hour][i].lat+90)
	#	context.arc(x,y,0.5,0,360)
	#	#context.set_source_rgba(float(hour)/24,float(hour)/24,float(hour)/24)
	#	context.fill_preserve()
	#	context.stroke()
	#	i += 100
