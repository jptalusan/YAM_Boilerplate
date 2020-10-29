from utils.Utils import *
from utils.constants import *
from local_utils.worker_utils import *
from base_stream import MessageHandlers as mh
import zmq
import json
import threading
import networkx as nx
import osmnx as ox
import os
import numpy as np
from pprint import pprint
from src.conf import GLOBAL_VARS
from itertools import groupby

from src.distributed_routing import task
from src.distributed_routing import basic_utils
from src.route_planning import query_handler as qh
from src.route_planning import route_generation as rg

class WorkerHandler(mh.RouterMessageHandler):
    """Handles messages arrvinge at the RouterMessageHandler REP stream."""
    worker_dict = {}

    # FIXME: Some work around for aggregating routes
    queries = {}
    '''
        SEC001: Main Functions
    '''
    def __init__(self, identity, base_process): # identity, backend_stream, publish_stream, stop, extract_handler, train_handler):
        # json_load is the element index of the msg_type (differs with socket type)
        super().__init__(json_load=1)
        # Instance variables
        self._base_process      = base_process
        self._backend_stream    = base_process.backend_stream
        self._publisher_stream  = base_process.publisher_stream

        self._identity        = identity
        self._query_queue     = self._identity + '_queries'
        self._r               = base_process.redis
        self._r.flushdb()

        print("Worker Identity: {}".format(self._identity))

        self._peer_sockets = {}
        self.context = zmq.Context()
        print("WorkerHandler:__init__")

        self._queue_processor = threading.Thread(target=self.process_tasks_in_queue, args = ())
        self._queue_processor.start()

        # Confirm directories are in place
        if not os.path.exists(os.path.join('data')):
            raise OSError("Must first download data, see README.md")
        self.data_dir = os.path.join(os.getcwd(), 'data')

        # Setup route generator
        self.router = rg.Route_Generator()

    '''
        SEC002: General Handler Functions
    '''
    def plzdiekthxbye(self, *data):
        print("Stopping:WorkerProcess")
        """Just calls :meth:`WorkerProcess.stop`."""
        self._base_process.stop()
        return

    def error(self, *data):
        print(f"Received message from {decode(data[0])}, with error: {data[-1]}!")
        return

    '''
        SEC003: Test Handler Functions
    '''
    # Close neighbors that were in the _peer_sockets but are not anymore.
    def populate_neighbors(self, *data):
        sender = decode(data[0])
        payload = json.loads(decode(data[1]))
        # print(f"Populating neighbors with {[worker for worker in payload.keys() if worker != self._identity]}")

        for worker, data in payload.items():
            if worker == self._identity:
                continue
            port = data['port']
            self.worker_dict[worker] = port

    def test_ping_query(self, *data):
        # Loading nx_g to save time for now...
        file_path = os.path.join(self.data_dir, 'G.pkl')
        with open(file_path, 'rb') as handle:
            nx_g = pickle.load(handle)
            
        sender = decode(data[0])
        print(f'Received {decode(data[1])} from {sender}')
        self.test_ping_response(sender, f'{len(nx_g.nodes)}')
        return
    
    def test_ping_response(self, *data):
        sender = data[0]
        payload = data[1]
        print("Sending response using the other server.")
        self._backend_stream.send_multipart([encode(sender), encode(payload)])
        return

    '''
        SEC004: Service-specific Handler Functions
    '''
    def receive_route_query(self, *data):
        sender = decode(data[0])
        payload = json.loads(decode(data[1]))
        if 'q_id' in payload:
            print_log(f"Received query: {payload['q_id']} from {sender}")
            self._r.hmset(payload['q_id'], {'q_id': payload['q_id'],
                                            'time_inquired': payload['time'],
                                            'time_received': time.time(),
                                            'route': ""})
        elif 't_id' in payload:
            t_id = payload['t_id']
            task_type = payload['task_type']
            print_log(f"Received {t_id}:{task_type} from {sender}")
        # TODO: Check if payload t_id already exists, if yes, get it and update
        self._r.rpush(self._identity, json.dumps(payload))
        return True

    def generate_route(self, payload):
        q_id = payload['q_id']
        OD = payload['OD']
        timestamp = payload['time']

        file_path = os.path.join(self.data_dir, f'complete_graph_3x3.pkl')
        with open(file_path, 'rb') as handle:
            nx_g = pickle.load(handle)

        # Generate route
        try:
            r = nx.shortest_path(nx_g, int(OD[0]), int(OD[1]))
        except nx.NetworkXNoPath:
            r = []

        complete_payload = {'q_id': q_id, 'time': timestamp, 'route': r}
        payload = json.dumps(complete_payload, cls = NpEncoder)

        self._publisher_stream.send_multipart([b'client_result', encode(payload)], zmq.NOBLOCK)
        print(f"Done with {q_id}")

    #TODO: Create a separate queue for queries (aggregating and responding to user)
    def plan_route(self, payload):
        print_log("->plan_route()")
        
        q_id = payload['q_id']
        timestamp = payload['time']
        s = payload['s']
        d = payload['d']
        t_h = payload['t_h']
        t_m = payload['t_m']
        t = str(payload['t_h']) + ':' + str(payload['t_m'])
        h = payload['h']

        # HACK: Until i figure out how to make an optimal sequence grid.
        file_path = os.path.join(self.data_dir, f'complete_graph_3x3.pkl')
        with open(file_path, 'rb') as handle:
            nx_g = pickle.load(handle)

        n_s = ox.get_nearest_node(nx_g, (s['coordinates'][1], s['coordinates'][0]))
        n_d = ox.get_nearest_node(nx_g, (d['coordinates'][1], d['coordinates'][0]))
        route = nx.shortest_path(nx_g, n_s, n_d)
        osg = qh.generate_optimal_sequence_grid(nx_g, route)
        # Removes duplicate consecutive grids
        osg = [x[0] for x in groupby(osg)]

        print_log(f"OSG:{q_id}:{osg}")
    
        payload['osg'] = osg
        tasks = qh.generate_tasks(payload)
        # [pprint(task.get_json()) for task in tasks]

        # FIXME: I'm lazy and i won't do any other checking, assume sequential and route won't be jumbled.

        # Sends to all workers via zeromq
        for i, task in enumerate(tasks):
            data = task.get_json()
            t_id = data['t_id']
            gridA = data['gridA']
            gridB = data['gridB']
            node = data['node']
            curr_grid = None

            if isinstance(gridA, int) and isinstance(gridB, int):
                curr_grid = gridA
            elif isinstance(gridA, int):
                curr_grid = gridA
            elif isinstance(gridB, int):
                curr_grid = gridB
            elif isinstance(node, int):
                curr_grid = node

            worker = GLOBAL_VARS.WORKER[curr_grid]
            port   = GLOBAL_VARS.PORTS[worker]
            print_log(f'Sent {t_id} to {curr_grid}: tcp://{worker}:{port}')
            
            temp_sock = self.context.socket(zmq.DEALER)
            temp_sock.identity = encode(self._identity)
            temp_sock.connect(f'tcp://{worker}:{port}')
            time.sleep(0.1)

            data['task_type'] = GLOBAL_VARS.PARTIAL_ROUTE
            curr_port   = GLOBAL_VARS.PORTS[self._identity]
            data['end_point'] = f'{self._identity}:{curr_port}'

            payload = json.dumps(data, cls = NpEncoder)
            temp_sock.send_multipart([b'receive_route_query', encode(payload)], zmq.NOBLOCK)
            temp_sock.close()
        return True

    # TODO: Convert a json to an object again,.
    def generate_partial_route(self, payload):
        print_log(f"{payload['t_id']}->generate_partial_route()")

        prev_data = self._r.hgetall(payload['t_id'])
        if prev_data:
            payload['prev_route'] = basic_utils.convert_string_to_list(prev_data['prev_route'])

        travel_time, route = self.router.find_route(payload)

        # Notify all other RSUs (or just broker?) that the route is broken and should throw away
        if route is None:
            print_log(f"Route: {payload['t_id']} is broken")
            end_point = payload['end_point']
            temp_sock = self.context.socket(zmq.DEALER)
            temp_sock.identity = encode(self._identity)
            temp_sock.connect(f'tcp://{end_point}')
            time.sleep(0.1)

            payload['task_type'] = GLOBAL_VARS.ROUTE_ERROR
            ready_payload = json.dumps(payload, cls = NpEncoder)
            temp_sock.send_multipart([b'receive_route_query', encode(ready_payload)], zmq.NOBLOCK)
            temp_sock.close()
            return False

        # print_log(f"partial route: {route}")
        print_log(f"{payload['t_id']}->finished partial route")

        # TODO: Save the route and task? into some database/redis? maybe mongodB is better
        next_step = str(int(payload['step']) + 1).zfill(3)
        curr_id = payload['t_id']
        new_id = "{}{}{}".format(payload['parsed_id'], next_step, payload['steps'])
        next_node = route[-1]
        
        step = payload['step']
        steps = payload['steps']
        next_rsu = payload['gridB']
        if int(step) != int(steps):
            worker = GLOBAL_VARS.WORKER[next_rsu]
            port   = GLOBAL_VARS.PORTS[worker]
            print_log(f'Send next {new_id} to {next_rsu}: tcp://{worker}:{port}')

            payload['next_node'] = next_node
            payload['t_id'] = new_id
            payload['route'] = route

            if step != steps:
                payload['task_type'] = GLOBAL_VARS.PARTIAL_ROUTE_UPDATE

            curr_port   = GLOBAL_VARS.PORTS[self._identity]

            temp_sock = self.context.socket(zmq.DEALER)
            temp_sock.identity = encode(self._identity)
            temp_sock.connect(f'tcp://{worker}:{port}')
            time.sleep(0.1)

            ready_payload = json.dumps(payload, cls = NpEncoder)
            temp_sock.send_multipart([b'receive_route_query', encode(ready_payload)], zmq.NOBLOCK)
            temp_sock.close()


        # NOTE: Need to send the generated route to the end_point/broker
        payload['task_type'] = GLOBAL_VARS.AGGREGATE_ROUTE
        payload['t_id'] = curr_id
        payload['route'] = route
        payload['next_node'] = next_node

        end_point = payload['end_point']
        temp_sock = self.context.socket(zmq.DEALER)
        temp_sock.identity = encode(self._identity)
        temp_sock.connect(f'tcp://{end_point}')
        time.sleep(0.1)

        ready_payload = json.dumps(payload, cls = NpEncoder)
        temp_sock.send_multipart([b'receive_route_query', encode(ready_payload)], zmq.NOBLOCK)
        temp_sock.close()

        return True

    def process_tasks_in_queue(self):
        # Should I not pop immediately, but only when the pipeline is "done"
        while True:
            task_count = self._r.llen(self._identity)
            if task_count > 0:
                task = self._r.lpop(self._identity)
                payload = json.loads(task)

                # Debugging
                # if 'next_node' in payload:
                #     if payload['next_node'] is not None:
                #         pprint(task)
                task_type = payload['task_type']
                
                if task_type == GLOBAL_VARS.ROUTE_ERROR:
                        if 'q_id' in payload:
                            q_id = payload['q_id']
                        elif 't_id' in payload:
                            q_id = payload['t_id'][:8]
                        self._r.hmset(q_id, {'q_id': q_id,
                                             'time_processed': time.time(), 
                                             'result': GLOBAL_VARS.ROUTE_ERROR})
                        q_id_data = self._r.hgetall(q_id)
                        payload = json.dumps(q_id_data)

                        self._publisher_stream.send_multipart([b'client_result', encode(payload)], zmq.NOBLOCK)
                        print_log(f"Done with {q_id}")

                if task_type == GLOBAL_VARS.FULL_ROUTE:
                    self.generate_route(payload)

                if task_type == GLOBAL_VARS.ROUTE_PLANNING:
                    self.plan_route(payload)

                if task_type == GLOBAL_VARS.PARTIAL_ROUTE:
                    t_id = payload['t_id']

                    if self._r.exists(t_id) == 1:
                        payload['next_node'] = self._r.hget(t_id, 'next_node')
                        self.generate_partial_route(payload)
                        
                    elif payload['next_node'] is None and payload['step'] != '000':
                        self._r.rpush(self._identity, json.dumps(payload))
                        continue

                    elif payload['step'] == '000':
                        self.generate_partial_route(payload)

                if task_type == GLOBAL_VARS.PARTIAL_ROUTE_UPDATE and payload['next_node']:
                    t_id = payload['t_id']
                    r_str = basic_utils.convert_list_to_string(payload['route'])
                    self._r.hmset(t_id, {'next_node': payload['next_node'],
                                         'prev_route': r_str})

                if task_type == GLOBAL_VARS.AGGREGATE_ROUTE:
                    q_id = payload['t_id'][:8]
                    step = payload['step']
                    steps = payload['steps']

                    route = self._r.hget(q_id, 'route')
                    route = basic_utils.convert_string_to_list(route)
                    
                    if len(route) == 0:
                        route = payload['route']
                    else:
                        first_node = payload['route'][0]
                        try:
                            last_index = len(route) - 1 - route[::-1].index(first_node)
                            new_route = route[:last_index + 1]
                            route = new_route + payload['route'][1:]
                        except:
                            route = []

                    r_str = basic_utils.convert_list_to_string(route)
                    self._r.hmset(q_id, {'route': r_str})
                    
                    if step == steps:
                        self._r.hmset(q_id, {'time_processed': time.time(), 'result': GLOBAL_VARS.ROUTE_SUCCESS})
                        q_id_data = self._r.hgetall(q_id)
                        payload = json.dumps(q_id_data)
                        self._publisher_stream.send_multipart([b'client_result', encode(payload)], zmq.NOBLOCK)
                        print_log(f"Done with {q_id}")

                        # Logging
                        keys = self._r.keys('*')
                        keys = [key for key in keys if len(key) == 8]
                        print(keys)