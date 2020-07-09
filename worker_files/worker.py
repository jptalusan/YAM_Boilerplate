import os
import zmq
import threading
import time
import json
import threading
from datetime import datetime

from worker_stream import WorkerProcess as wp
from utils.Utils import *

ROUTER_PORT = os.environ['PORT']
ident = os.environ['WORKER_ID']

HEARTBEAT_HOST = 'broker'
HEARTBEAT_PORT = os.environ['HEARTBEAT_PORT']

def heartbeat(context):
    threading.Timer(60.0, heartbeat, [context]).start()

    addr=(HEARTBEAT_HOST, HEARTBEAT_PORT)
    identity="Worker-{}".format(ident)
    # addr may be 'host:port' or ('host', port)
    if isinstance(addr, str):
        addr = addr.split(':')
    host, port = addr if len(addr) == 2 else (addr[0], None)

    socket = context.socket(zmq.PUSH)
    socket.identity = (u"%s" % identity).encode('ascii')

    socket.connect('tcp://%s:%s' % (host, port))
    time.sleep(1)
    now = current_seconds_time()
    payload = json.dumps({"sentAt":now,
                        #   "identity": decode(socket.identity), 
                          "port": ROUTER_PORT})

    print(f'Sending heartbeat: {payload}')
    socket.send_multipart([b"heartbeat", socket.identity, encode(payload)])
    
if __name__ == "__main__":
    context = zmq.Context()
    heartbeat(context)
    wp.WorkerProcess(bind_addr=('*', ROUTER_PORT), 
                     identity="Worker-{}".format(ident)).start()
