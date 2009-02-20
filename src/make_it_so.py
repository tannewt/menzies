#!/usr/bin/python

import subprocess
import sys
import time

config = {'way': {'args':[],'deps':[]},
				'relation': {'args':[],'deps':[]},
				'node': {'args':[],'deps':[]},
				'nodes': {'num' : 1},
				'front': {'args':[],
									'deps':['way','relation','nodes']}}

if len(sys.argv) > 1:
	args = sys.argv[1:]
	servers = []
	for a in args:
		for s in config[a]['deps']:
			if s not in servers:
				servers.append(s)
		servers.append(a)
else:
	servers = ['way', 'relation', 'nodes', 'front']

print "Running these servers:"," ".join(servers)

def kill(process):
	#print process.poll()
	cmd = ["kill",str(process.pid)]
	print " ".join(cmd)
	r = subprocess.call(cmd)
	if r != 0:
		print "NOT killed. returned",r

def run(cmd, name):
	print "starting",name
	print " ".join(cmd)
	p = subprocess.Popen(cmd,stdin=subprocess.PIPE,stdout=open(name.lower()+".log","w"),stderr=subprocess.STDOUT)
	print "started",p.pid
	print
	return p

processes = []

for s in servers:
	if s=='way':
		# run wayserver with above args
		name = "WayServer"
		cmd =["python","waybox/ready_go.py"]+config[s]["args"]
		p = run(cmd, name)
		processes.append((name, p))
	elif s=='relation':
		# run relationserver with above args
		name = "RelationServer"
		cmd = ["python","relationbox/ready_go.py"]+config[s]["args"]
		p = run(cmd, name)
		processes.append((name, p))
	elif s=='nodes':
		for i in range(config[s]["num"]):
			name = "NodeServer%d"%i
			cmd = ["python","nodebox/ready_go.py"]+config["node"]["args"]
			p = run(cmd, name)
			processes.append((name, p))
	elif s=='node':
		name = "NodeServer"
		cmd = ["python","nodebox/ready_go.py"]+config[s]["args"]
		p = run(cmd, name)
		processes.append((name, p))
	elif s=='front':
		name = "Frontend"
		cmd = ["python","front/rest.py"]+config[s]["args"]
		p = run(cmd, name)
		processes.append((name, p))
	time.sleep(1)

try:
	while True:
		time.sleep(100)
except:
	pass

processes.reverse()

for name,process in processes:
	print "stopping",name
	kill(process)
	print "stopped"
	print
	time.sleep(1)
