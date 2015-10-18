#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Author: v.grigorev@gmail.com

from __future__ import print_function

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('-r', '--retention', type=int, default=60)
parser.add_argument('-e', '--expire', type=int, default=300)
parser.add_argument('-p', '--port', type=int, default=15555)
parser.add_argument('-m', '--mode', type=str, choices=['sum', 'min', 'max', 'avg'], default='sum')

args = parser.parse_args()

RETENTION_INTERVAL = args.retention
EXPIRE = args.expire

import gevent, gevent.pool
from gevent.server import StreamServer
import sys, time, signal

class Aggregator(object):

    def __init__(self, mode):
        self.stats = {}
        self.changed = set()
        self.mode = mode

        self.agg_functions = {
            'sum': sum,
            'min': min,
            'max': max,
            'avg': self.agg_avg
        }

    def aggregate(self, list):
        return self.agg_functions[self.mode](list)

    def agg_avg(self, list):
        return sum(list) / len(list)

    def add_value(self, path, val, ts):
        key = (path, int(ts / RETENTION_INTERVAL))
        values = self.stats.setdefault(key, [])
        values.append(float(val))
        self.changed.add(key)        

    def flush(self):
        for k in frozenset(self.changed):
            ts = k[1] * RETENTION_INTERVAL
            print("%s %s %s" % (k[0], self.aggregate(self.stats[k]), ts))
        # sys.stdout.flush()
        self.changed.clear()

        expire_value = (int(time.time()) - EXPIRE) / RETENTION_INTERVAL
        for k in self.stats.keys():
            if k[1] > expire_value:
                del self.stats[k]



# this handler will be run for each incoming connection in a dedicated greenlet
def connection_handler(socket, address):
    print('New connection from %s:%s' % address, file=sys.stderr)
    # using a makefile because we want to use readline()
    rfileobj = socket.makefile(mode='r')

    while True:
        line = rfileobj.readline()
        if not line:
            print("client disconnected", file=sys.stderr)
            break
        # validate line
        parts = line.strip().split(' ')
        if len(parts) != 3:
            print("Wrong input line: %s" % line, file=sys.stderr)
            break

        path, val, ts = parts
        agg.add_value(path, float(val), int(ts))

    rfileobj.close()



def flush_loop():
    while True:
        gevent.sleep(RETENTION_INTERVAL)
        agg.flush()


def quit_handler():
    server.stop()
    pool.kill()
    agg.flush()


# create pool of greenlets
pool = gevent.pool.Pool()
agg = Aggregator(args.mode)


print('Starting graphite-agg server on port %d' % args.port, file=sys.stderr)
server = StreamServer(('0.0.0.0', args.port), connection_handler)
# spawn streaming server in our pool
server.set_spawn(pool) 
server.start()

pool.spawn(flush_loop)

gevent.signal(signal.SIGTERM, quit_handler)

try:
    pool.join()

except KeyboardInterrupt:
    quit_handler()


