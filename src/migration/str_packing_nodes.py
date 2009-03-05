#!/usr/bin/env python

from bsddb import db as bdb

import os, math, sys
import pprint
import pickle
import subprocess

from rectangle import minimum_bounding_rectangle

#########################################?

db = bdb.DB()
db.open("spatial_index_tmp.db","Spatial Index", bdb.DB_BTREE, 0)

print "writing data for next level up of the index"

def load(level):
	print "loading level %d" % level

	grid_size = int(db.get("splits%d" % (level-1)))
	print "grid_size:", str(grid_size)


	input_filename = "spatial_index_level%d.raw" % level
	input_basename = os.path.splitext(input_filename)[0]
	level2_file = open(input_filename, "w")

	total_entries = 0

	print "writing file to do external sort on"
	for x in range(grid_size):
		for y in range(grid_size):
			key = "%d-%d-%d" % (level - 1, x, y)
			data = db.get(key)
			if data:
				min_lat, min_lon, max_lat, max_lon = data.split(":")[:4]

				min_lat = float(min_lat)
				min_lon = float(min_lon)
				max_lat = float(max_lat)
				max_lon = float(max_lon)

				mid_lat = (min_lat + max_lat) / 2
				mid_lon = (min_lon + max_lon) / 2

				total_entries += 1
				level2_file.write("%f:%f:%f:%f:%f:%f:%s\n" % (mid_lat, mid_lon, min_lat, min_lon, max_lat, max_lon, key))
	level2_file.close()

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
				row = 0
				data = []

		if len(data) > 0:
			rectangle = minimum_bounding_rectangle(data)
			key = "%d-%d-%d" % (level, i / entries_per_page, col)
			db.put(key, str(rectangle))

		col += 1

	db.put("levels", str(level))
	db.put("splits%d" % level, str(int(grid_size)))

	# Cleanup
	os.unlink(input_filename)
	os.unlink(sorted_by_lon_filename)
	for f in os.listdir(curr_dir):
		fullname = os.path.join(curr_dir,f)
		os.unlink(fullname)
	os.rmdir(curr_dir)

	if grid_size > 1:
		load(level+1)

load(1)

