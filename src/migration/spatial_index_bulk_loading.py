#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os, math
import subprocess
import pickle
from bsddb import db as bdb

sys.path.append("common")
from rectangle import minimum_bounding_rectangle

if len(sys.argv) < 3:
	print "Usage: python spatial_index_bulk_loading.py <spatial_data> <output.db>"
	print ""
	print "spatial_data should be a file in the form:"
	print "\tlatitude:longitude:node_id1"
	print "\tlatitude:longitude:node_id2"
	print "\t..."
	sys.exit(1)

input_filename = sys.argv[1]
input_basename = os.path.splitext(input_filename)[0]
sorted_by_lon_filename = input_basename + "_sorted_by_lon"

level = 0

total_entries = 0
for i in open(input_filename, "r").xreadlines():
	total_entries += 1

print "sorting %d points of spatial data by longitude" % total_entries
subprocess.call(["sort", "-k2", "-t:", "-n", input_filename, "-o", sorted_by_lon_filename])

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

os.unlink(sorted_by_lon_filename)

db = bdb.DB()
db.open(sys.argv[2],"Spatial Index", bdb.DB_BTREE, bdb.DB_CREATE)

print "grid size is %d" % grid_size
db.put("splits0", str(int(grid_size)))
db.put("levels", "0")

print "creating rectangles from splits and storing in db"

col = 0

files = [os.path.join(curr_dir,f) for f in os.listdir(curr_dir)]
files.sort()

next_level_input_filename = "spatial_index_level%d.raw" % (level+1)
next_level_input_basename = os.path.splitext(input_filename)[0]
next_level_file = open(next_level_input_filename, "w")

for fullname in files:
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

			min_lat, min_lon, max_lat, max_lon = rectangle.min_lat, rectangle.min_lon, rectangle.max_lat, rectangle.max_lon

			mid_lat = (min_lat + max_lat) / 2
			mid_lon = (min_lon + max_lon) / 2

			next_level_file.write("%.10f:%.10f:%.10f:%.10f:%.10f:%.10f:%s\n" % (mid_lat, mid_lon, min_lat, min_lon, max_lat, max_lon, key))

			row = 0
			data = []

	if len(data) > 0:
		# Now handle the page that includes the leftovers
		rectangle = minimum_bounding_rectangle(data)
		key = "%d-%d-%d" % (level, i / entries_per_page, col)
		db.put(key, str(rectangle))

	os.unlink(fullname)

	col += 1

os.rmdir(curr_dir)
next_level_file.close()

#########################################
# Now load all the nodes, up to the root, recursively
#########################################

print "writing data for next level up of the index"

def load(level):
	print "loading level %d" % level

	grid_size = int(db.get("splits%d" % (level-1)))
	print "grid_size:", str(grid_size)

	input_filename = "spatial_index_level%d.raw" % level
	input_basename = os.path.splitext(input_filename)[0]

	total_entries = 0
	for i in open(input_filename, "r").xreadlines():
		total_entries += 1

	sorted_by_lon_filename = input_basename + "_sorted_by_lon"

	print "sorting spatial data by longitude"
	subprocess.call(["sort", "-k2", "-t:", "-n", input_filename, "-o", sorted_by_lon_filename])

	print "total entries: %d" % total_entries
	entries_per_node = 20
	grid_size = math.ceil(math.sqrt(float(total_entries) / entries_per_node))
	entries_per_split = int(math.ceil(float(total_entries) / grid_size))
	entries_per_page = int(math.ceil(float(entries_per_split) / grid_size))

	splits_prefix = input_basename+"_split_"
	print "splitting into chunks of %d points per chunk" % entries_per_split

	curr_dir = "level%d" % level
	try:
		os.mkdir(curr_dir)
	except:
		pass
	subprocess.call(["split", sorted_by_lon_filename, "-d", "-a", "5", "-l", str(entries_per_split), os.path.join(curr_dir,splits_prefix)])

	print "storing in berkeley db"
	col = 0

	files = [os.path.join(curr_dir,f) for f in os.listdir(curr_dir)]
	files.sort()

	next_level_input_filename = "spatial_index_level%d.raw" % (level+1)
	next_level_input_basename = os.path.splitext(input_filename)[0]
	next_level_file = open(next_level_input_filename, "w")

	for fullname in files:

		# Sort by latitude
		subprocess.call(["sort", "-k1", "-t:", "-n", fullname, "-o", fullname])
		i = 0
		data = []
		for line in open(fullname,"r").xreadlines():
			mid_lat, mid_lon, min_lat, min_lon, max_lat, max_lon = [float(x) for x in line.split(":")[:6]]
			child_key = line.split(":")[-1].strip()

			data.append((min_lat, min_lon, child_key))
			data.append((max_lat, max_lon, None))

			i += 1
			if i % entries_per_page == 0:
				rectangle = minimum_bounding_rectangle(data)
				key = "%d-%d-%d" % (level, i / entries_per_page - 1, col)
				db.put(key, str(rectangle))

				min_lat, min_lon, max_lat, max_lon = rectangle.min_lat, rectangle.min_lon, rectangle.max_lat, rectangle.max_lon

				mid_lat = (min_lat + max_lat) / 2
				mid_lon = (min_lon + max_lon) / 2

				next_level_file.write("%.10f:%.10f:%.10f:%.10f:%.10f:%.10f:%s\n" % (mid_lat, mid_lon, min_lat, min_lon, max_lat, max_lon, key))

				row = 0
				data = []

		if len(data) > 0:
			rectangle = minimum_bounding_rectangle(data)
			key = "%d-%d-%d" % (level, i / entries_per_page, col)
			db.put(key, str(rectangle))

		col += 1

	db.put("levels", str(level))
	db.put("splits%d" % level, str(int(grid_size)))

	next_level_file.close()

	# Cleanup
	os.unlink(input_filename)
	os.unlink(sorted_by_lon_filename)
	for f in os.listdir(curr_dir):
		fullname = os.path.join(curr_dir,f)
		os.unlink(fullname)
	os.rmdir(curr_dir)

	if grid_size > 1:
		load(level+1)
	else:
		os.unlink(next_level_input_filename)

load(1)