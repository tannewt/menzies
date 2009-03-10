# -*- coding: utf-8 -*-
import pickle

class Rectangle:
	def __init__(self):
		self.min_lat = None
		self.max_lat = None
		self.min_lat = None
		self.max_lon = None

		self.node_ids = []

	def include(self, lat, lon, node_id = None):
		if not self.min_lat:
			self.min_lat = lat
			self.min_lon = lon
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

		if node_id:
			self.node_ids.append(node_id)

	def area(self):
		height = self.max_lat - self.min_lat
		width = self.max_lon - self.min_lon
		return height * width

	def __str__(self):
		return "%.10f:%.10f:%.10f:%.10f:%s" % (self.min_lat, self.min_lon, self.max_lat, self.max_lon, pickle.dumps(self.node_ids))

def minimum_bounding_rectangle(data):
	rect = Rectangle()
	for lat, lon, node_id in data:
		rect.include(lat, lon, node_id)
	return rect
