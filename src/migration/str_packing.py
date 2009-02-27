#!/usr/bin/env python

import sys, os, math
import subprocess
import pickle
from bsddb import db as bdb

class Rectangle:
	def __init__(self):
		self.min_lat = None
		self.max_lat = None
		self.min_lat = None
		self.max_lon = None

		self.node_ids = []

	def include(self, lat, lon, node_id):
		if not self.min_lat:
			self.min_lat = lat
			self.min_lon = lon
		elif not self.max_lat:
			self.max_lat = lat
			self.max_lon = lon
		else:
			if lat < self.min_lat:
				self.min_lat = lat
			elif lat > self.max_lat:
				self.max_lat = lat

			if lon < self.min_lon:
				self.min_lon = lon
			elif lon > self.max_lon:
				self.max_lon = lon

		self.node_ids.append(node_id)

	def __str__(self):
		return "%f:%f:%f:%f" % (self.min_lat, self.min_lon, self.max_lat, self.max_lon)

def minimum_bounding_rectangle(data):
	rect = Rectangle()
	for point in data:
		lat, lon, node_id = point.split(":")

		lat = float(lat)
		lon = float(lon)
		node_id = long(node_id)

		rect.include(lat, lon, node_id)
	return rect

input_filename = "spatial_data.raw"
input_basename = os.path.splitext(input_filename)[0]
sorted_by_lon_filename = input_basename + "_sorted_by_lon"

print "sorting spatial data by longitude"
subprocess.call(["sort", "-k2", "-t:", "-n", input_filename, "-o", sorted_by_lon_filename])

total_entries = 3557197
entries_per_node = 20
grid_size = math.ceil(math.sqrt(float(total_entries) / entries_per_node))
entries_per_split = int(math.ceil(float(total_entries) / grid_size))
entries_per_page = int(math.ceil(float(entries_per_split) / grid_size))

splits_prefix = input_basename+"_split_"
print "splitting into chunks of %d points per chunk" % entries_per_split
subprocess.call(["split", sorted_by_lon_filename, "-d", "-a", "5", "-l", str(entries_per_split), splits_prefix])

db = bdb.DB()
db.open("spatial_index_tmp.db","Spatial Index", bdb.DB_BTREE, bdb.DB_CREATE)

print "grid size is %d" % grid_size
db.put("splits", str(grid_size))

print "creating rectangles from splits and storing in spatial_index_tmp.db"

level = 0
col = 0
for f in os.listdir("."):
	if f.startswith(splits_prefix):
		i = 0
		data = []
		for line in open(f,"r").xreadlines():
			data.append(line)
			i += 1
			if i % entries_per_page == 0:
				rectangle = minimum_bounding_rectangle(data)
				# print "got rectangle:", str(rectangle)
				db.put("%d-%d-%d" % (level, i / entries_per_page - 1, col), str(rectangle))
				row = 0
				data = []
		col += 1

db.close()

