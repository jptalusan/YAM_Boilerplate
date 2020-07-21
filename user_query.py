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

QUERIES = 1

def generate_route():
    context = zmq.Context()

    host = 'localhost'
    port = 6003

    receiver = context.socket(zmq.DEALER)
    receiver.identity = (u"Client-%s" % str(0).zfill(4)).encode('ascii')
    receiver.connect('tcp://%s:%s' % (host, port))

    for i in range(QUERIES):
        timestamp = time.time()
        payload = json.dumps({"time": timestamp, "OD": (0, 506)})
        receiver.send_multipart([b'generate_route', payload.encode('ascii')], flags = zmq.DONTWAIT)
        print(f"Sent route query {i} at {timestamp} with payload: {payload}")
        # msg = receiver.recv_multipart()
        # received = time.time()
        # print(f'Received {msg} at time {received}')
        # print(f'Time elapsed: {received - timestamp}')
        # print()

def listener():
    host = 'localhost'
    port = 6013

    context = zmq.Context()
    socket_sub = context.socket(zmq.SUB)
    socket_sub.connect('tcp://%s:%s' % (host, port))
    # socket_sub.connect ("tcp://localhost:%s" % port_sub)
    topic = "client_result"
    socket_sub.setsockopt(zmq.SUBSCRIBE, encode(topic))
    print("Connected to publisher with port %s" % port)
    # Initialize poll set
    poller = zmq.Poller()
    poller.register(socket_sub, zmq.POLLIN)

    should_continue = True
    messages_received = 0
    while should_continue:
        socks = dict(poller.poll())
        if socket_sub in socks and socks[socket_sub] == zmq.POLLIN:
            topic, message = socket_sub.recv_multipart()
            payload = json.loads(decode(message))
            timestamp = payload['time']
            received = time.time()
            messages_received += 1
            print(f'{messages_received}: Received {message}\nTime elapsed: {received - timestamp}')
            print()
            if messages_received == QUERIES:
                break
    

if __name__ == '__main__':
    Process(target=listener, args=()).start()
    generate_route()

    # ping(6003)

    # reintroduce_workers()

    # Process(target=pipeline_ping, args=()).start()

    # for i in {1..20}; do echo $i; python user_query.py; done
    # Process(target=pipeline_async_ping, args=()).start()

    # For demo on heartbeat
    # for _ in range(5):
    #     heartbeat_demo()
    # query_services()
