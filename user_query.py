import zmq
import json
from multiprocessing import Process
import time
import random

# https://stackoverflow.com/questions/39163872/zeromq-advantages-of-the-router-dealer-pattern

# Port access the port opened in docker-compose (LEFT side, 7000:7000), in EXPOSE in dockerfile, it is for internal comm between containers
# Localhost is used, because we can't access it by hostname unless we do some DNS
# https://stackoverflow.com/questions/37242217/access-docker-container-from-host-using-containers-name
# https://stackoverflow.com/questions/35828487/docker-1-10-access-a-container-by-its-hostname-from-a-host-machine/
host = 'localhost'
port = 6000

decode = lambda x: x.decode('utf-8')
encode = lambda x: x.encode('ascii')
current_seconds_time = lambda: int(round(time.time()))

# Just to test if all the connections are working.
def reintroduce_workers():
    context = zmq.Context()
    broker_sock = context.socket(zmq.DEALER)
    broker_sock.identity = (u"Client-%s" % str(0).zfill(4)).encode('ascii')
    broker_sock.connect('tcp://%s:%s' % (host, 7000))
    broker_sock.send_multipart([b'reintroduce_workers', b'Ping'])
    print("Sent: Reintroduce worker msg.")
    msg = broker_sock.recv_multipart()
    resp = msg[0]
    print("Received: {}".format(decode(resp)))
    print()

def heartbeat_demo():
    identity=(u"Worker-%s" % str(random.randint(1000, 9999))).encode('ascii')

    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.identity = identity
    socket.connect('tcp://%s:%s' % (host, 8000))
    # time.sleep(1)

    now = current_seconds_time()
    payload = json.dumps({"sentAt":now,
                          "service": 'DEMO', 
                          "port": random.randint(1000, 9999),
                          "x": random.randint(0, 24),
                          "y": random.randint(0, 24)})

    print(f'{identity}: sending heartbeat: {payload}\n')
    socket.send_multipart([b"heartbeat", identity, encode(payload)])

def query_services():
    context = zmq.Context()
    broker_sock = context.socket(zmq.DEALER)
    broker_sock.identity = (u"Client-%s" % str(0).zfill(4)).encode('ascii')
    broker_sock.connect('tcp://%s:%s' % (host, 7000))
    broker_sock.send_multipart([b'query_services', b'Ping'])
    print("Sent: Ping")
    msg = broker_sock.recv_multipart()
    resp = msg[0]
    print("Received: {}".format(decode(resp)))

def ping(PORT):
    context = zmq.Context()
    broker_sock = context.socket(zmq.DEALER)
    broker_sock.identity = (u"Client-%s" % str(0).zfill(4)).encode('ascii')
    broker_sock.connect('tcp://%s:%s' % (host, PORT))
    broker_sock.send_multipart([b'test_ping_query', b'Ping'])
    print("Sent: Ping")
    msg = broker_sock.recv_multipart()
    resp = msg[0]
    print("Received: {}".format(decode(resp)))

# Worker 0000 to Worker 0001 and back to client
def pipeline_ping():
    # How do i just listen to all ports?
    context = zmq.Context()

    host = 'localhost'
    port = 6001

    receiver = context.socket(zmq.DEALER)
    receiver.identity = (u"Client-%s" % str(0).zfill(4)).encode('ascii')
    receiver.connect('tcp://%s:%s' % (host, port))

    timestamp = time.time()
    payload = json.dumps({"time": timestamp, "pipeline":['Worker-0001', 'Worker-0000', 'Worker-0002']})
    receiver.send_multipart([b'pipeline_ping_query', payload.encode('ascii')], flags = zmq.DONTWAIT)
    print(f"Sent: Ping at {timestamp} with payload: {payload}")

    receiver2 = context.socket(zmq.DEALER)
    receiver2.identity = receiver.identity
    receiver2.connect('tcp://%s:%s' % (host, 6002))
    msg = receiver2.recv_multipart()

    received = time.time()
    print(f'Received {msg} at time {received}')
    print(f'Time elapsed: {received - timestamp}')
    print()

def pipeline_async_ping():
    # How do i just listen to all ports?
    context = zmq.Context()

    host = 'localhost'
    port = 6001

    receiver = context.socket(zmq.DEALER)
    receiver.identity = (u"Client-%s" % str(0).zfill(4)).encode('ascii')
    receiver.connect('tcp://%s:%s' % (host, port))

    timestamp = time.time()
    payload = json.dumps({"time": timestamp, "pipeline":['Worker-0001', 'Worker-0000', 'Worker-0002']})
    receiver.send_multipart([b'pipeline_ping_query', payload.encode('ascii')], flags = zmq.DONTWAIT)
    print(f"Sent: Ping at {timestamp} with payload: {payload}")

if __name__ == '__main__':
    ping(6003)

    # reintroduce_workers()

    Process(target=pipeline_ping, args=()).start()

    # for i in {1..20}; do echo $i; python user_query.py; done
    # Process(target=pipeline_async_ping, args=()).start()

    # For demo on heartbeat
    # for _ in range(5):
    #     heartbeat_demo()
    # query_services()
