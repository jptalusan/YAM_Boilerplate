from worker_stream import WorkerProcess as wp

import os
import zmq
import time
from datetime import datetime
import json
import threading
from utils.Utils import *

BROKER_HOST = os.environ['BROKER_HOST']
BROKER_PORT = os.environ['BROKER_PORT']
ident = os.environ['WORKER_ID']

PUBLISH_HOST = os.environ['PUBLISH_HOST']
PUBLISH_PORT = os.environ['PUBLISH_PORT']

def server_pub():
    threading.Timer(5.0, server_pub).start()

    addr=(PUBLISH_HOST, PUBLISH_PORT)
    identity="Worker-{}".format(ident)
    # addr may be 'host:port' or ('host', port)
    if isinstance(addr, str):
        addr = addr.split(':')
    host, port = addr if len(addr) == 2 else (addr[0], None)

    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.identity = (u"%s" % identity).encode('ascii')

    socket.connect('tcp://%s:%s' % (host, port))
    time.sleep(3)
    now = current_milli_time()

    conf_json, status = read_json_data('logs', "{}-conf.lock".format(identity), ['under_load'])
    if status:
        under_load = conf_json['under_load']
        payload = json.dumps({"sentAt":now, "under_load":under_load})
    else:
        payload = json.dumps({"sentAt":now})

    socket.send_multipart([b"topic", b"heartbeat", socket.identity, payload.encode('ascii')])

if __name__ == "__main__":
    # TODO: Create a lock/config file, in memory, get data every time you send heartbeat. (if under load)
    server_pub()
    wp.WorkerProcess(bind_addr=(BROKER_HOST, BROKER_PORT), 
                     publish_addr=(PUBLISH_HOST, PUBLISH_PORT), 
                     identity="Worker-{}".format(ident)).start()
