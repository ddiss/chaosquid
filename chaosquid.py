#!/usr/bin/python
#
# Random Ceph service killer and reviver
#
# Copyright (C) 2016 SUSE Linux GmbH
#
# Author: David Disseldorp <ddiss@suse.de>
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.

import subprocess
import random
import os.path
import time

RUN_TEST_FLAG_FILE = './chaosquid_run'

kill_osds = None
OSD_DUMP_INDEX_OSD = 0
OSD_DUMP_INDEX_STATE = 1

kill_mons = 1
MON_DUMP_INDEX_MON = 2

def osds_get():
	osds = []
	cmd = ['ceph', 'osd', 'dump']
	p = subprocess.Popen(cmd, stdout=subprocess.PIPE)

	while True:
		line = p.stdout.readline()
		if line == '':
			break

		if line.startswith('osd.'):
			print line.rstrip()
			osd = line.split()[OSD_DUMP_INDEX_OSD]
			osd_id = osd.split('.').pop()
			osds.append(osd_id)

	status = p.wait()
	if status != 0:
		raise RuntimeError("failed to get osd dump")
		return []

	return osds

def osd_svc_from_id(osd_id):
	return 'ceph-osd@%s' % (osd_id)

def mons_get():
	mons = []
	cmd = ['ceph', 'mon', 'dump']
	p = subprocess.Popen(cmd, stdout=subprocess.PIPE)

	while True:
		line = p.stdout.readline()
		if line == '':
			break

		# hardcoded for to only handle <=9 mons
		if line[0].isdigit() and line[1] == ':':
			print line.rstrip()
			mon = line.split()[MON_DUMP_INDEX_MON]
			mon_id = mon.split('.').pop()
			mons.append(mon_id)

	status = p.wait()
	if status != 0:
		raise RuntimeError("failed to get mon dump")
		return []

	return mons

def mon_svc_from_id(mon_id):
	return 'ceph-mon@%s' % (mon_id)

def systemd_bringdown(systemd_svc):
	cmd = ['systemctl', 'stop', systemd_svc]
	p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
	status = p.wait()
	if status != 0:
		raise RuntimeError("failed to take service offline")

def systemd_bringup(systemd_svc):
	cmd = ['systemctl', 'start', systemd_svc]
	p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
	status = p.wait()
	if status != 0:
		raise RuntimeError("failed to bring OSD online")

	# clear error state, to ensure start/stop don't fail due to excessive
	# bouncing
	cmd = ['systemctl', 'reset-failed', systemd_svc]
	p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
	status = p.wait()
	if status != 0:
		raise RuntimeError("failed to clear systemd failed state")


osd_ids = osds_get()
mon_ids = mons_get()

while os.path.exists(RUN_TEST_FLAG_FILE):
	down_sleep = random.randint(40, 120)
	post_sleep = random.randint(180, 240)

	if (kill_osds):
		osd_to_bounce = random.choice(osd_ids)

		print 'Taking OSD %s offline for %d seconds' % (osd_to_bounce, down_sleep)
		osd_svc = osd_svc_from_id(osd_to_bounce)
		systemd_bringdown(osd_svc)

	if (kill_mons):
		mon_to_bounce = random.choice(mon_ids)
		mon_svc = mon_svc_from_id(mon_to_bounce)

		print 'Taking monitor %s offline for %d seconds' % (mon_to_bounce, down_sleep)
		systemd_bringdown(mon_svc)

	time.sleep(down_sleep)

	if (kill_osds):
		print 'Bringing OSD %s online and sleeping for %d seconds' % (osd_to_bounce, post_sleep)
		systemd_bringup(osd_svc)

	if (kill_mons):
		print 'Bringing monitor %s online and sleeping for %d seconds' % (mon_to_bounce, post_sleep)
		systemd_bringup(mon_svc)

	time.sleep(post_sleep)

