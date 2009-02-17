DEBUG = True
def debug(p):
	if DEBUG:
		print p

def bin(a,fill=8):
		s=[]
		t={'0':'0000','1':'0001','2':'0010','3':'0011',
			 '4':'0100','5':'0101','6':'0110','7':'0111',
			 '8':'1000','9':'1001','a':'1010','b':'1011',
			 'c':'1100','d':'1101','e':'1110','f':'1111'}
		o = hex(a)[2:].strip("L")
		for c in '0'*(fill-len(o))+o:
				    s.append(t[c])
		return "".join(s)
		

class GeoNode:
	def __init__(self,lat=None,lon=None,v=None):
		self.lat = lat
		self.lon = lon
		self.v = v
	
	def __str__(self):
		return self.v
	
	def __repr__(self):
		return "GeoNode("+",".join((str(self.lat),str(self.lon),str(self.v)))+")"
		
class BdbBin:
	MAX = 100
	def __init__(self,depth,num):
		self.depth = depth
		self.num = num
		self.count = 0
	
	def put(self, value):
		self.count += 1
	
	def get(self):
		return [0]*self.count
	
	def remove(self, v):
		self.count -= 1
	
	def split(self):
		return self.count>self.MAX
	
	def _str(self):
		return "BdbBin " + hex(self.num) + " " + str(self.children)
	
	def __str__(self):
		return " "*self.depth + self._str()
	
	def __repr__(self):
		return self._str()

class Branch:
	def __init__(self,depth,num,leaf,first):
		shift = (32 - depth - 1) * 2
		self.children = [first, leaf(depth+1,num+(1<<shift)), leaf(depth+1,num+(2<<shift)), leaf(depth+1,num+(3<<shift))]
		first.depth = depth + 1
		self.depth = depth
		self.num = num
		
		i = 0
		while i < len(first.children):
			c = first.children[i]
			bin = (c.num>>2*(31-depth))%4
			if bin!=0:
				first.children.pop(i)
				self.children[bin].children.append(c)
			else:
				i += 1
		
		# prevent this from spiralling out of control
		if depth < 31:
			i = 0
			for c in self.children:
				if c.split():
					self.children[i] = Branch(c.depth,c.num,leaf,c)
				i += 1
	
	def __repr__(self):
		return "Branch "+hex(self.num)
	
	def __str__(self):
		rows = [" "*self.depth+"Branch "+hex(self.num)]
		for c in self.children:
			rows.append(str(c))
		return "\n".join(rows)
		
class QuadPartition:
	def __init__(self, leaf):
		self.leaf = leaf
		self.root = leaf(0,0)
		self._total_nodes = 0
	
	def __str__(self):
		return str(self.root)
	
	def add(self, v):
		self._total_nodes += 1
		bnum = self.get_bin_num(v.lat, v.lon)
		v.num = bnum
		parent,i,box = self._get_node(bnum)
		box.children.append(v)
		if box.split():
			if i == None:
				self.root = Branch(0,0,self.leaf, box)
			else:
				parent.children[i] = Branch(box.depth,box.num,self.leaf, box)
	
	def get_box(self, left, bottom, right, top):
		top_left = self.get_bin_num(top, left)
		bottom_left = self.get_bin_num(bottom, left)
		bottom_right = self.get_bin_num(bottom, right)
		top_right = self.get_bin_num(top, right)
		
		debug(bin(top_left,16))
		debug(bin(bottom_left,16))
		debug(bin(bottom_right,16))
		debug(bin(top_right,16))
		
		depth,ancestor = self.get_ancestor([top_left, bottom_left, bottom_right, top_right])
		
		print "depth",depth
		print "ancestor",hex(ancestor)
		#could add more logic to make this finer grained
		return self._get_values(ancestor,depth)
	
	def _get_all(self, root):
		if isinstance(root, self.leaf):
			return root.children
		else:
			# this is probably slowwwwww
			result = []
			for c in root.children:
				result += self._get_all(c)
			return result
	
	def _get_values(self, bin, depth=32):
		node = self._get_node(bin, depth)[-1]
		return self._get_all(node)
	
	def _get_node(self, bin, depth=32):
		current = self.root
		parent = None
		i = None
		d = 0
		while d < 32 and d < depth:
			if isinstance(current,self.leaf):
				return (parent, i, current)
			else:
				i = (bin>>2*(31-d))%4
				parent = current
				current = current.children[i]
			d += 1
		return (parent, i, current)
		
	def all_equal(self, vs):
		v = vs[0]
		for o in vs[1:]:
			if v!=o:
				return False
		return True
	
	def get_ancestor(self, vs):
		d = 0
		while not self.all_equal(vs):
			vs = map(lambda v: v>>2, vs)
			d+=1
		return (32-d, vs[0]<<2*d)
	
	def get_value(lat, lon):
		pass
	
	def get_bin_num(self, lat, lon):
		x,y = self.get_xy(lat, lon)
		#debug("x " + bin(x))
		#debug("y " + bin(y))
		return self._interlace(x,y)
	
	def get_xy(self, lat, lon):
		lat -= 90
		lat *= -1
		y = 2**32 * (lat/180.0)
		
		lon += 180
		x = 2**32 * (lon/360.0)
		return (int(x),int(y))
	
	def get_lat(self, x):
		lat = float(x)/2**32 * -180
		lat += 90
		return lat
	
	def get_lon(self, y):
		lon = float(y)/2**32 * 360
		lon -= 180
		return lon
	
	def _interlace(self, x, y):
		out = 0
		mask = 1
		mask <<= 31
		for i in range(32):
			out <<= 2
			if x & mask:
				out += 2
			if y & mask:
				out += 1
			mask >>= 1
		return out
	
	def _deinterlace(self, bin):
		x = 0
		y = 0
		mask = 2**63
		for i in range(32):
			x <<= 1
			if bin & mask:
				x += 1
			mask >>= 1
			y <<= 1
			if bin & mask:
				y += 1
			mask >>= 1
		return (x,y)

if __name__=="__main__":
	
	part = QuadPartition(NodeServer)
	
	print "interlace stuff"
	for x,y in [(123,456),(0,0),(35,40)]:
		print bin(x)
		print bin(y)
		i = part._interlace(x,y)
		print bin(i)
		nx,ny = part._deinterlace(i)
		print x,nx
		print y,ny
		print
	
	print "lat, lon -> x,y"
	for lat, lon in [(90,-180),(-90,-180),(-90,180),(90,180),(45,-90)]:
		x,y = part.get_xy(lat,lon)
		print x,y
		print hex(x),hex(y)
		print lat, part.get_lat(y)
		print lon, part.get_lon(x)
		print
	
	print "get ancestor"
	for l in [[0x0a084,0x0a345,0x0a123]]:
		d, a = part.get_ancestor(l)
		print d
		print hex(a)
		print
	
	print "add"
	print part
	print
	for n in [GeoNode(45,-90,"a"),GeoNode(-45,90,"b1"),GeoNode(-45,90,"b2"),GeoNode(-45,90,"b3"),GeoNode(45,90,"c")]:
		part.add(n)
		print part
		print
	
	print "get box"
	for t,l,r,b in [(45,-90,90,-45),(90,-180,-91,46),(90,-180,-1,1)]:
		r = part.get_box(l,b,r,t)
		print r
		print
	
	import sys
	
	if len(sys.argv) > 1:
		data = QuadPartition(NodeServer)
		from xml.sax import make_parser, handler

		class FancyCounter(handler.ContentHandler):
			def __init__(self):
				self._object = None
				self._count = 0

			def startElement(self, name, attrs):
				if name == "node":
					self._object = GeoNode()
					self._object.v = int(attrs["id"])
					self._object.lat = float(attrs["lat"])
					self._object.lon = float(attrs["lon"])

			def endElement(self, name):
				# Store in Berkeley DB
				if name == "node":
					data.add(self._object)
					self._count += 1
					if self._count % 100000 == 0:
						print "Processed %d elements" % self._count
					#if self._count >= 20:
					#	raise Exception("enough nodes")

			def endDocument(self):
				pass

		# data bounds t47.9226, l-122.5801, b47.888, r-122.5315 
		parser = make_parser()
		parser.setContentHandler(FancyCounter())
		try:
			parser.parse(sys.argv[1])
		except:
			pass
		
		print "loaded hansville",data._total_nodes,"nodes"
		#minlat="47.9087" minlon="-122.5617" maxlat="47.9132" maxlon="-122.5569"
		buck_lake = data.get_box(-122.5617,47.9087,-122.5569,47.9132)
		print "got",len(buck_lake),"nodes"
		buck_lake = set(map(lambda n: n.v, buck_lake))
		
		if len(sys.argv)>2:
			f = open(sys.argv[2])
			target = set(map(lambda s: int(s.strip()),f.readlines()))
			f.close()
			
			print len(target & buck_lake),"nodes shared"
			print len(buck_lake - target),"extra nodes"
		else:
			for s in buck_lake:
				print s.v
