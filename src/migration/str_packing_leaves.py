#!/usr/bin/env python

import sys, os, math
import subprocess
import pickle
from bsddb import db as bdb

from rectangle import minimum_bounding_rectangle

input_filename = "spatial_data.raw"
input_basename = os.path.splitext(input_filename)[0]
sorted_by_lon_filename = input_basename + "_sorted_by_lon"

level = 0

total_entries = 0
for i in open(input_filename, "r").xreadlines():
	total_entries += 1

print "sorting %d points of spatial data by longitude" % total_entries
subprocess.call(["sort", "-k2", "-t:", "-n", input_filename, "-o", sorted_by_lon_filename])

#total_entries = 3557197
entries_per_node = 20
grid_size = math.ceil(math.sqrt(float(total_entries) / entries_per_node))
entries_per_split = int(math.ceil(float(total_entries) / grid_size))
entries_per_page = int(math.ceil(float(entries_per_split) / grid_size))

curr_dir = "level%d" % level
try:
	os.mkdir(curr_dir)
except:
	pass

splits_prefix = input_basename+"_split_"
print "splitting into chunks of %d points per chunk" % entries_per_split
subprocess.call(["split", sorted_by_lon_filename, "-d", "-a", "5", "-l", str(entries_per_split), os.path.join(curr_dir,splits_prefix)])

db = bdb.DB()
db.open("spatial_index_tmp.db","Spatial Index", bdb.DB_BTREE, bdb.DB_CREATE)

print "grid size is %d" % grid_size
db.put("splits0", str(int(grid_size)))
db.put("levels", "0")

print "creating rectangles from splits and storing in spatial_index_tmp.db"

col = 0
for f in os.listdir(curr_dir):
	fullname = os.path.join(curr_dir,f)
	# Sort by latitude
	subprocess.call(["sort", "-k1", "-t:", "-n", fullname, "-o", fullname])
	i = 0
	data = []
	for line in open(fullname,"r").xreadlines():
		lat, lon, node_id = line.split(":")
		data.append((float(lat), float(lon), long(node_id)))
		i += 1
		if i % entries_per_page == 0:
			rectangle = minimum_bounding_rectangle(data)
			key = "%d-%d-%d" % (level, i / entries_per_page - 1, col)
			db.put(key, str(rectangle))
			row = 0
			data = []

	if len(data) > 0:
		# Now handle the page that includes the leftovers
		rectangle = minimum_bounding_rectangle(data)
		key = "%d-%d-%d" % (level, i / entries_per_page, col)
		db.put(key, str(rectangle))

	col += 1

db.close()

# Cleanup
os.unlink(sorted_by_lon_filename)
for f in os.listdir(curr_dir):
	fullname = os.path.join(curr_dir,f)
	os.unlink(fullname)
os.rmdir(curr_dir)

