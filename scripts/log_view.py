import sys
import cairo

PIXEL_WIDTH = 900
SEC_WIDTH = 90
HEIGHT = 20
PADDING = 2

COLORS = { "getNodesInBounds":     (0.0, 207.0/255, 11.0/255),
           "nodeYield":            (0.0, 140.0/255, 7.0/255),
           "getWaysFromNodes":     (103.0/255, 105.0/255, 1.0),
           "yieldWays":            (0.0/255, 3.0/255, 1.0),
           "getRelationsFromWays": (180.0/255, 108.0/255, 1.0),
           "yieldWayRelations":    (4.0/255, 6.0/255, 130.0/255),
           "getRelationsFromNodes":(125.0/255, 0.0, 1.0),
           "forRelation":          (70.0/255, 0.0, 142.0/255),
           "getNode(s)":          (0.0, 75.0/255, 4.0/255)}

if len(sys.argv) < 2:
  print sys.argv[0],"<log files>"
  sys.exit(1)

for fn in sys.argv[1:]:
  f = open(fn)
  first_line = f.readline().strip()
  seq, time, point, end = first_line.split()
  req_order = []
  point = point.strip(":")
  start_time = float(time)
  requests = {}
  #print seq, "0.0", point, end
  requests[seq] = [[point, 0.0, None]]
  req_order.append(seq)
  max_time = 0.0
  for line in f.readlines():
    line = line.strip()
    seq, time, point, end = line.split()
    point = point.strip(":")
    time = float(time)-start_time
    if seq not in requests:
      req_order.append(seq)
      requests[seq] = [[point, time, None]]
    else:
      if end=="start":
        requests[seq].append([point, time, None])
      else:
        requests[seq][-1][-1] = time
    #print seq, time, point, end
    max_time = max(max_time, time)
  
  img = cairo.SVGSurface(fn.rsplit(".",1)[0]+".svg",PIXEL_WIDTH,HEIGHT*len(req_order)+PADDING*(len(req_order)-1))
  context = cairo.Context(img)
  offset = 0
  for req in req_order:
    for point,start,end in requests[req]:
      context.set_source_rgb(*COLORS[point])
      context.rectangle(PIXEL_WIDTH/SEC_WIDTH * start, offset, (end-start)*PIXEL_WIDTH/SEC_WIDTH, HEIGHT)
      context.fill()
      
      context.set_source_rgb(1.0, 1.0, 1.0)
      context.move_to(PIXEL_WIDTH/SEC_WIDTH * start, offset+HEIGHT)
      context.show_text(point)
    offset += HEIGHT + PADDING
