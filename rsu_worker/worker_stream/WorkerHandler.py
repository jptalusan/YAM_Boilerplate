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
from src.conf import GLOBAL_VARS

from src.distributed_routing import task
from src.route_planning import query_handler as qh
from src.route_planning import route_generation as rg

class WorkerHandler(mh.RouterMessageHandler):
    """Handels messages arrvinge at the PongProc’s REP stream."""
    # worker_dict = {'Worker-0000': 6000,
    #                'Worker-0001': 6001,
    #                'Worker-0002': 6002}
    worker_dict = {}

    # FIXME: Some work around, because i dont want another DB
    next_node_dict = {}

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

        self._query_queue_processor = threading.Thread(target=self.process_queries_in_queue, args = ())
        self._query_queue_processor.start()

        # Confirm directories are in place

        if not os.path.exists(os.path.join('data')):
            raise OSError("Must first download data, see README.md")
        self.data_dir = os.path.join(os.getcwd(), 'data')
        
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

    def receive_route_query(self, *data):
        sender = decode(data[0])
        payload = json.loads(decode(data[1]))
        print("Received:", payload)
        task_type = payload['task_type']
        # print(f"Received {task_type} from: {sender}")
        # TODO: Check if payload t_id already exists, if yes, get it and update
        self._r.rpush(self._identity, json.dumps(payload))
        return

    def generate_route(self, payload):
        q_id = payload['q_id']
        OD = payload['OD']
        timestamp = payload['time']

        file_path = os.path.join(self.data_dir, 'G.pkl')
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
        """Generates optimal sequence grids (using a pre-loaded divided grid) and sends them
           to all the workers/grids based on that sequence.

        Args:
            payload (Dict): Containing the main query payload with (q_id, s, d, t, etc...)

        Returns:
            Bool: Returns True if it works, False if not.
        """
        print("-> plan_route()")
        precision     = 10
        bits_per_char = 2
        q_id = payload['q_id']
        timestamp = payload['time']
        s = payload['s']
        d = payload['d']
        t = payload['t']

        #TODO: Integrate with the new method of dividing a target area (see notebook with Work with Tiau)
        # https://github.com/linusmotu/secure_routing/blob/master/PAPER_Generate_Distributed_Grid_Dataframes.ipynb
        file_path = os.path.join(self.data_dir, f'9-p_{precision}-bpc_{bits_per_char}-G.pkl')
        with open(file_path, 'rb') as handle:
            nx_g = pickle.load(handle)

        r, ogs = qh.generate_optimal_grid_sequence(nx_g, payload)

        # debugging
        complete_payload = {'s': s, 'd': d, 't': t, 
                            'q_id': q_id, 'time': timestamp, 
                            'route': r, 'ogs': ogs}

        tasks = qh.generate_tasks(complete_payload)

        # Creating/adding to query queue (separate from task queue)
        # FIXME: I'm lazy and i won't do any other checking, assume sequential and route won't be jumbled.
        complete_payload['route'] = []
        payload = json.dumps(complete_payload, cls = NpEncoder)
        self.queries[q_id] = payload

        # Sends to all workers via zeromq
        for i, task in enumerate(tasks):
            data = task.get_json()
            gridA = data['gridA']
            gridB = data['gridB']

            if isinstance(gridA, int):
                optimal_rsu = gridB
            else:
                optimal_rsu = gridA

            worker = GLOBAL_VARS.WORKER[optimal_rsu]
            port   = GLOBAL_VARS.PORTS[worker]
            print(f'{optimal_rsu}: tcp://{worker}:{port}')

            temp_sock = self.context.socket(zmq.DEALER)
            temp_sock.identity = encode(self._identity)
            temp_sock.connect(f'tcp://{worker}:{port}')

            data['task_type'] = GLOBAL_VARS.PARTIAL_ROUTE
            curr_port   = GLOBAL_VARS.PORTS[self._identity]
            data['end_point'] = f'{self._identity}:{curr_port}'

            payload = json.dumps(data, cls = NpEncoder)
            temp_sock.send_multipart([b'receive_route_query', encode(payload)], zmq.NOBLOCK)
            temp_sock.close()


        payload = json.dumps(complete_payload, cls = NpEncoder)
        self._publisher_stream.send_multipart([b'client_result', encode(payload)], zmq.NOBLOCK)
        return True

    # TODO: Convert a json to an object again,.
    def generate_partial_route(self, payload):
        """[summary]

        Args:
            payload ([type]): [description]

        Returns:
            [type]: [description]
        """
        print("-> generate_partial_route()")
        # print(f"Received payload: {payload}")
        precision     = 10
        bits_per_char = 2
        geohash = GLOBAL_VARS.RSUS[self._identity]

        file_path = os.path.join(self.data_dir, f'9-p_{precision}-bpc_{bits_per_char}-G.pkl')
        with open(file_path, 'rb') as handle:
            nx_g = pickle.load(handle)

        router = rg.Route_Generator(nx_g, precision, bits_per_char)
        travel_time, route = router.find_route(payload)
        # print(f'Route at {self._identity}: {route}')
        print(f"Finished partial route.")

        # TODO: Save the route and task? into some database/redis? maybe mongodB is better
        next_step = str(int(payload['step']) + 1).zfill(3)
        new_id = "{}{}{}".format(payload['parsed_id'], next_step, payload['steps'])
        next_node = route[-1]
        
        next_rsu = payload['gridB']
        if next_rsu:
            worker = GLOBAL_VARS.WORKER[next_rsu]
            port   = GLOBAL_VARS.PORTS[worker]
            print(f'Send next to: {next_rsu}: tcp://{worker}:{port}')

            payload['next_node'] = next_node
            payload['t_id'] = new_id
            payload['route'] = route

            step = payload['step']
            steps = payload['steps']
            if step != steps:
                payload['task_type'] = GLOBAL_VARS.PARTIAL_ROUTE_UPDATE
            # elif step == steps:
            #     payload['task_type'] = GLOBAL_VARS.PARTIAL_ROUTE_UPDATE

            curr_port   = GLOBAL_VARS.PORTS[self._identity]
            # payload['end_point'] = f'{self._identity}:{curr_port}'

            temp_sock = self.context.socket(zmq.DEALER)
            temp_sock.identity = encode(self._identity)
            temp_sock.connect(f'tcp://{worker}:{port}')

            ready_payload = json.dumps(payload, cls = NpEncoder)
            temp_sock.send_multipart([b'receive_route_query', encode(ready_payload)], zmq.NOBLOCK)
            temp_sock.close()

            time.sleep(0.1)

            # NOTE: Need to send the generated route to the end_point/broker
            payload['task_type'] = GLOBAL_VARS.AGGREGATE_ROUTE
            end_point = payload['end_point']
            temp_sock = self.context.socket(zmq.DEALER)
            temp_sock.identity = encode(self._identity)
            temp_sock.connect(f'tcp://{end_point}')

            ready_payload = json.dumps(payload, cls = NpEncoder)
            temp_sock.send_multipart([b'receive_route_query', encode(ready_payload)], zmq.NOBLOCK)
            temp_sock.close()

        elif not next_rsu:
            # NOTE: Need to send the generated route to the end_point/broker
            payload['task_type'] = GLOBAL_VARS.AGGREGATE_ROUTE
            payload['next_node'] = next_node
            payload['t_id'] = new_id
            payload['route'] = route

            end_point = payload['end_point']
            temp_sock = self.context.socket(zmq.DEALER)
            temp_sock.identity = encode(self._identity)
            temp_sock.connect(f'tcp://{end_point}')

            ready_payload = json.dumps(payload, cls = NpEncoder)
            temp_sock.send_multipart([b'receive_route_query', encode(ready_payload)], zmq.NOBLOCK)
            temp_sock.close()
        return True

    def process_queries_in_queue(self):
        while True:
            task_count = self._r.llen(self._query_queue)
            if task_count > 0:
                query = self._r.lpop(self._query_queue)
                payload = json.loads(query)
                print('process_queries_in_queue:', payload)
                time.sleep(10)

    def process_tasks_in_queue(self):
        # Should I not pop immediately, but only when the pipeline is "done"
        while True:
            task_count = self._r.llen(self._identity)
            if task_count > 0:
                # print(f'{self._identity} has {task_count} tasks in queue.')
                task = self._r.lpop(self._identity)

                payload = json.loads(task)
                task_type = payload['task_type']
                if task_type == GLOBAL_VARS.FULL_ROUTE:
                    self.generate_route(payload)

                if task_type == GLOBAL_VARS.ROUTE_PLANNING:
                    self.plan_route(payload)

                if task_type == GLOBAL_VARS.PARTIAL_ROUTE:
                    t_id = payload['t_id']

                    if t_id in self.next_node_dict:
                        payload['next_node'] = self.next_node_dict.pop(t_id)
                        self.generate_partial_route(payload)
                        
                    elif payload['next_node'] is None and payload['step'] != '000':
                        self._r.rpush(self._identity, json.dumps(payload))
                        continue

                    elif payload['step'] == '000':
                        self.generate_partial_route(payload)

                if task_type == GLOBAL_VARS.PARTIAL_ROUTE_UPDATE and payload['next_node']:
                    # FIXME: Just update the local dictionary "next_node_dict"
                    # print('270', payload)
                    t_id = payload['t_id']
                    self.next_node_dict[t_id] = payload['next_node']
                    pass

                if task_type == GLOBAL_VARS.AGGREGATE_ROUTE:
                    q_id = payload['t_id'][:8]
                    step = payload['step']
                    steps = payload['steps']
                    if q_id not in self.queries:
                        
                        # HACK
                        self._r.lpop(self._identity)
                        continue

                    json_dict = json.loads(self.queries[q_id])
                    route = json_dict['route']
                    
                    if len(route) == 0:
                        json_dict['route'] = payload['route']
                    else:
                        json_dict['route'] = route + payload['route'][1:]
                    
                    self.queries[q_id] = json.dumps(json_dict)
                    
                    if step == steps:
                        self._publisher_stream.send_multipart([b'client_result', encode(self.queries[q_id])], zmq.NOBLOCK)
                        print(f"Done with {q_id}")
                        del self.queries[q_id]
                    
                        pass

    '''
        SEC004: Service-specific Handler Functions
    '''