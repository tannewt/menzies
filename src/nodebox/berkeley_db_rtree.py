# -*- coding: utf-8 -*-
import sys
import os
import pickle

from bsddb import db as bdb

from data.ttypes import *
from rectangle import Rectangle

class RTree:
	def __init__(self, filename, bucket_size=25):
		self.bucket_size = bucket_size

		self.db = bdb.DB()
		self.db.open(filename, "Spatial Index", bdb.DB_BTREE, bdb.DB_CREATE)

	def cleanup(self):
		self.db.close()

	def likely_intersection(self, bounds):
		traversals = 0

		node_ids = []
		for level, data in self.traverse(bounds):
			traversals += 1
			if level == 0:
				node_ids += data[1]

		print "Took %d traversals of the spatial index" % traversals

		return node_ids

	#def traverse_if_contains(node)
	#def traverse_if_intersects(bounds)
	#def traverse_if_best_insert(node, children)

	def traverse(self, bounds):

		def contains(bounds, node):
			return (node.lat > bounds.min_lat and node.lat < bounds.max_lat) and (node.lon > bounds.min_lon and node.lon < bounds.max_lon)

		def intersects(r1, r2):
			return not (r2.min_lon > r1.max_lon
				or r2.max_lon < r1.min_lon
				or r2.max_lat < r1.min_lat
				or r2.min_lat > r1.max_lat)

		if isinstance(bounds, Node):
			within = contains
		elif isinstance(bounds, BBox):
			within = intersects
		else:
			raise TypeError, "Bad argument"

		def traverse_to(level, key):
			page = self.db.get(key)
			min_lat, min_lon, max_lat, max_lon, pickled_ids = page.split(":")
			page_bounds = BBox(min_lat=float(min_lat), min_lon=float(min_lon), max_lat=float(max_lat), max_lon=float(max_lon))

			if not within(page_bounds, bounds):
				return

			ids = pickle.loads(pickled_ids)

			if level == 0: # We're at a leaf that intersects, done
				yield (level, (page_bounds, ids))
			else: # We're at an intermediate node, keep going
				yield (level, page_bounds)
				for id in ids:
					for i in traverse_to(level - 1, id): yield i

		root_level = self.db.get("levels")
		if root_level:
			key = "%s-%d-%d" % (root_level, 0, 0)
			for i in traverse_to(int(root_level), key): yield i
		else:
			print "No nodes on this server"
			raise StopIteration

	def insert(self, node):

		def traverse_to(level, key):
			page = self.db.get(key)
			min_lat, min_lon, max_lat, max_lon, pickled_ids = page.split(":")
			page_bounds = BBox(min_lat=float(min_lat), min_lon=float(min_lon), max_lat=float(max_lat), max_lon=float(max_lon))

			ids = pickle.loads(pickled_ids)

			if level == 0: # We're at the best leaf, add it
				if node.id in ids:
					print "Warning: not adding duplicate id to spatial index"
					return

				# Get the potentially expanded bounds
				rect = Rectangle()
				rect.node_ids = ids
				rect.min_lat = float(min_lat)
				rect.min_lon = float(min_lon)
				rect.max_lat = float(max_lat)
				rect.max_lon = float(max_lon)
				rect.include(node.lat, node.lon, node.id)

				# Update the record
				self.db.put(key, str(rect))
			else: # We're at an intermediate node, keep going
				best = None
				for id in ids:
					page = self.db.get(id)
					min_lat, min_lon, max_lat, max_lon, pickled_ids = page.split(":")

					rect = Rectangle()
					rect.node_ids = pickle.loads(pickled_ids)
					rect.min_lat = float(min_lat)
					rect.min_lon = float(min_lon)
					rect.max_lat = float(max_lat)
					rect.max_lon = float(max_lon)
					area_before = rect.area()

					rect.include(node.lat, node.lon)
					area_after = rect.area()

					growth = area_after - area_before
					if not best or growth < best[1] or (growth == best[1] and area_after < best[2]):
						best = (id, growth, area_after, rect)

				if best[1] > 0: # This rectangle needs to grow
					self.db.put(id, str(best[3]))
				traverse_to(level - 1, best[0])

		root_level = self.db.get("levels")
		if root_level:
			key = "%s-%d-%d" % (root_level, 0, 0)
			traverse_to(int(root_level), key)
		else:
			print "No nodes on this server"
			raise StopIteration

	# FIXME: Reuse the above traverse code to implement delete
	def delete(self, node):
		def contains(bounds, node):
			return (node.lat > bounds.min_lat and node.lat < bounds.max_lat) and (node.lon > bounds.min_lon and node.lon < bounds.max_lon)
		within = contains

		def traverse_to(level, key):
			page = self.db.get(key)
			min_lat, min_lon, max_lat, max_lon, pickled_ids = page.split(":")
			page_bounds = BBox(min_lat=float(min_lat), min_lon=float(min_lon), max_lat=float(max_lat), max_lon=float(max_lon))

			if not within(page_bounds, bounds):
				return

			ids = pickle.loads(pickled_ids)

			if level == 0: # We're at a leaf that intersects, done
				ids.remove(node.id)

				rect = Rectangle()
				rect.node_ids = ids
				rect.min_lat = float(min_lat)
				rect.min_lon = float(min_lon)
				rect.max_lat = float(max_lat)
				rect.max_lon = float(max_lon)

				self.db.put(key, str(rect))
			else: # We're at an intermediate node, keep going
				for id in ids:
					traverse_to(level - 1, id)

		root_level = self.db.get("levels")
		if root_level:
			key = "%s-%d-%d" % (root_level, 0, 0)
			traverse_to(int(root_level), key)
		else:
			print "No nodes on this server"
			raise StopIteration

