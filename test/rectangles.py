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

hbox2 = gtk.HBox()
hbox2.show()

level = gtk.VScale()
level.show()

hbox2.pack_start(level, False, False)

vbox = gtk.VBox()
vbox.show()
hbox2.pack_start(vbox)

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
zoom.set_range(1.0,10000.0)
zoom.connect("value-changed",lambda r: canvas.set_scale(r.get_value()))
zoom.set_value(1.0)
zoom.show()
hbox.pack_start(zoom)

window.add(hbox2)

rect = goocanvas.Rect(parent=root, x=0, y=0, width=360, height=180, stroke_color_rgba=0x000000ff, line_width=1, line_dash=goocanvas.LineDash([1.0, 1.0]))

lowest = 0
num_layers = int(db.get("levels"))+1-lowest

level.set_range(lowest,num_layers-1)
current_layer = num_layers-1
level.set_value(num_layers-1)
level.set_inverted(True)
level.set_increments(1,5)

select = goocanvas.Rect(parent=root, x=0, y=0, width=360, height=180, fill_color_rgba=0xff000090, line_width=0, visibility=goocanvas.ITEM_HIDDEN)
start_x = None
start_y = None
end_x = None
end_y = None

def down(item, target, event):
	global select, start_x, start_y
	if event.button == 1:
		start_x = event.x
		start_y = event.y

def up(item, target, event):
	global select, start_x, start_y, end_x, end_y, zoom
	if event.button == 1:
		select.props.visibility = goocanvas.ITEM_HIDDEN
		vscale = scrolled.props.vadjustment.page_size/abs(start_y - end_y)
		hscale = scrolled.props.hadjustment.page_size/abs(start_x - end_x)
		scale = min(hscale,vscale)
		zoom.set_value(scale)
		scrolled.props.hadjustment.value = scale*min(start_x, end_x)
		scrolled.props.vadjustment.value = scale*min(start_y, end_y)

def move(item, target, event):
	global select, start_x, start_y, end_x, end_y
	if event.get_state() & gtk.gdk.BUTTON1_MASK:
		select.props.visibility = goocanvas.ITEM_VISIBLE
		end_x = event.x
		end_y = event.y
		select.props.x = min(start_x, end_x)
		select.props.width = abs(start_x - end_x)
		select.props.y = min(start_y, end_y)
		select.props.height = abs(start_y - end_y)

root.connect("button-press-event",down)
root.connect("button-release-event",up)
root.connect("motion-notify-event",move)

layers = {}
for layer in range(lowest,num_layers+lowest):
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
			rect = goocanvas.Rect(parent=root, x=min_lon, y=min_lat-height, width=width, height=height, fill_color_rgba=0x000000f0, line_width=0)
			if layer!=current_layer:
				rect.props.visibility = goocanvas.ITEM_HIDDEN
			layers[layer].append(rect)

def output(button):
	img = cairo.ImageSurface(cairo.FORMAT_ARGB32,3600,1800)
	context = cairo.Context(img)
	canvas.render(context,None,10.0)
	img.write_to_png("output.png")

def change_level(scale, scroll, value):
	if value < lowest: return

	global current_layer
	value = int(value)
	scale.set_value(value)
	for rect in layers[current_layer]:
		rect.props.visibility = goocanvas.ITEM_HIDDEN
	current_layer = value
	for rect in layers[value]:
		rect.props.visibility = goocanvas.ITEM_VISIBLE
	
level.connect("change-value", change_level)

out = gtk.Button("Output")
out.show()
out.connect("clicked", output)
hbox.pack_start(out,False,False)
gtk.main()
