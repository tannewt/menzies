import sys
import os
sys.path.append(os.getcwd())

from bsddb import db as bdb

if len(sys.argv)==1:
	print sys.argv[0],"<db file>"
	sys.exit(1)

db = bdb.DB()
db.open(sys.argv[1],"Spatial Index", bdb.DB_BTREE, 0)

import goocanvas
import gtk
import cairo

window = gtk.Window()
window.connect("destroy", lambda x: gtk.main_quit())
window.show()

vbox = gtk.VBox()
vbox.show()

canvas = goocanvas.Canvas()
canvas.set_bounds(0, 0, 360, 180)
root = canvas.get_root_item()
canvas.show()

scrolled = gtk.ScrolledWindow()
scrolled.show()
scrolled.add(canvas)

vbox.pack_start(scrolled)

hbox = gtk.HBox()
hbox.show()
vbox.pack_start(hbox,False,False)
zoom = gtk.HScale()
zoom.set_range(1.0,100.0)
zoom.connect("value-changed",lambda r: canvas.set_scale(r.get_value()))
zoom.set_value(1.0)
zoom.show()
hbox.pack_start(zoom)

window.add(vbox)

rect = goocanvas.Rect(parent=root, x=0, y=0, width=360, height=180, stroke_color_rgba=0x000000ff, line_width=1, line_dash=goocanvas.LineDash([1.0, 1.0]))

num_layers = int(db.get("levels"))+1

layers = {}
for layer in range(num_layers):
	splits = int(db.get("splits%d" % layer))
	layers[layer] = []
	for x in range(splits):
		for y in range(splits):
			bounds = db.get("%d-%d-%d" % (layer,x,y))
			if bounds == None:
				continue
			min_lat, min_lon, max_lat, max_lon = map(float, bounds.split(":")[:4])
			min_lat += 90
			max_lat += 90
			min_lon += 180
			max_lon += 180
			
			min_lat = 180 - min_lat
			max_lat = 180 - max_lat
			
			width = max_lon-min_lon
			height = min_lat-max_lat
			
			#print min_lat, min_lon, width, height
			rect = goocanvas.Rect(parent=root, x=min_lon, y=min_lat, width=width, height=height, fill_color_rgba=0x000000f0, line_width=0)
			layers[layer].append(rect)

def output(button):
	img = cairo.ImageSurface(cairo.FORMAT_ARGB32,3600,1800)
	context = cairo.Context(img)
	canvas.render(context,None,10.0)
	img.write_to_png("output.png")

out = gtk.Button("Output")
out.show()
out.connect("clicked", output)
hbox.pack_start(out,False,False)
gtk.main()
