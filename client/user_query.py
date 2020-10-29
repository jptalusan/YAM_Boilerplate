import zmq
import json
from multiprocessing import Process
import time
import random
import pandas as pd
import numpy as np
import os
import pickle
import uuid
import geojson

# https://stackoverflow.com/questions/39163872/zeromq-advantages-of-the-router-dealer-pattern

# Port access the port opened in docker-compose (LEFT side, 7000:7000), in EXPOSE in dockerfile, it is for internal comm between containers
# Localhost is used, because we can't access it by hostname unless we do some DNS
# https://stackoverflow.com/questions/37242217/access-docker-container-from-host-using-containers-name
# https://stackoverflow.com/questions/35828487/docker-1-10-access-a-container-by-its-hostname-from-a-host-machine/
host = 'localhost'
worker = 0
broker_port = 6000 + worker
listener_port = 6010 + worker

decode = lambda x: x.decode('utf-8')
encode = lambda x: x.encode('ascii')
current_seconds_time = lambda: int(round(time.time()))

ROUTE_ERROR = "ROUTE_ERROR"
ROUTE_SUCCESS = "ROUTE_SUCCESS"

QUERIES = 1

def get_queries(x, y, number_of_queries):
    if not os.path.exists(os.path.join(os.getcwd(), 'data')):
        raise OSError("Must first download data, see README.md")
    data_dir = os.path.join(os.getcwd(), 'data')

    file_path = os.path.join(data_dir, 'OD_trips_3x3_df.pkl')
    task_list = pickle.load(open(file_path,'rb'))
    task_list.drop(['osg', 'r'], axis=1, inplace=True)
    # return task_list.sample(n = number_of_queries)
    return task_list[5:10]

def send_query(row):
    context = zmq.Context()

    host = 'localhost'
    port = broker_port

    receiver = context.socket(zmq.DEALER)
    receiver.identity = (u"Client-%s" % str(0).zfill(4)).encode('ascii')
    receiver.connect('tcp://%s:%s' % (host, port))

    timestamp = time.time()
    q_id = row.t_id
    O = int(row.s)
    D = int(row.d)
    task_type = "FULL_ROUTE"
    payload = json.dumps({"q_id": q_id, "time": timestamp, "OD": (O, D), "task_type": task_type})
    receiver.send_multipart([b'receive_route_query', payload.encode('ascii')], flags = zmq.DONTWAIT)
    # print(f"Sent route query {q_id} at {timestamp} with payload: {payload}\n")

def send_route_planning(row):
    time.sleep(0.5)
    context = zmq.Context()

    host = 'localhost'
    port = broker_port

    receiver = context.socket(zmq.DEALER)
    receiver.identity = (u"Client-%s" % str(0).zfill(4)).encode('ascii')
    receiver.connect('tcp://%s:%s' % (host, port))

    timestamp = time.time()
    q_id = row.t_id
    # These are Shapely.Points
    O = row.s
    D = row.d

    # Separate into hour and minute for more granularity
    t_h = int(row.t_h)
    t_m = int(row.t_m)
    h   = int(row.h)
    T = random.choice(range(0, 24))
    task_type = "ROUTE_PLANNING"
    payload = geojson.dumps({"q_id": q_id, 
                             "time": timestamp, 
                             "s": O, "d": D, 
                             "t_h": t_h, "t_m": t_m, 
                             "h": h, "task_type": task_type})
    receiver.send_multipart([b'receive_route_query', payload.encode('ascii')], flags = zmq.DONTWAIT)
    # print(f"Sent route query {q_id} at {timestamp} with payload: {payload}\n")

def generate_route():
    context = zmq.Context()

    host = 'localhost'
    port = broker_port

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
    port = listener_port

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
            print(f"{messages_received}: {payload}")
            # q_id = ""
            # if 'q_id' in payload:
            #     q_id = payload['q_id']
            # else:
            #     # print(payload)
            #     pass
                
            # timestamp = time.time()
            # time_processed = float(payload['time_processed'])
            # time_inquired = float(payload['time_inquired'])

            # result = payload['result']
            # if result != ROUTE_ERROR:
            #     print(f"{messages_received}: received {q_id} {result} in {(time_processed - time_inquired):.2f} s")
            #     route = payload['route'].split(",")
            #     route = [int(r) for r in route]
            #     print(f"Route: {route}")
            # else:
            #     print(f"{messages_received}: received {q_id} {result} in {(time_processed - time_inquired):.2f} s")
            messages_received += 1
            print()

if __name__ == '__main__':
    Process(target=listener, args=()).start()

    df = get_queries(5, 5, QUERIES)
    print(df.head())
    # df.apply(send_query, axis=1)
    df.apply(send_route_planning, axis=1)
    # generate_route()