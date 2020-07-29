from utils.Utils import *
from utils.constants import *
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
        # For testing
        # temp_sock = self.context.socket(zmq.DEALER)
        # temp_sock.identity = encode(self._identity)
        # temp_sock.connect('tcp://Worker-0001:6001')
        # temp_sock.send_multipart([b'test_ping_query', encode('Ping')], zmq.NOBLOCK)
        # temp_sock.close()

    def plan_route(self, payload):
        print("-> plan_route()")
        precision     = 10
        bits_per_char = 2
        q_id = payload['q_id']
        timestamp = payload['time']
        s = payload['s']
        d = payload['d']
        t = payload['t']

        file_path = os.path.join(self.data_dir, f'9-p_{precision}-bpc_{bits_per_char}-G.pkl')
        with open(file_path, 'rb') as handle:
            nx_g = pickle.load(handle)

        r, ogs = qh.generate_optimal_grid_sequence(nx_g, payload)

        # debugging
        complete_payload = {'s': s, 'd': d, 't': t, 
                            'q_id': q_id, 'time': timestamp, 
                            'route': r, 'ogs': ogs}

        tasks = qh.generate_tasks(complete_payload)

        for task in tasks:
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
        print(f"Done with {q_id}")

    # TODO: Convert a json to an object again,.
    def generate_partial_route(self, payload):
        print("-> generate_partial_route()")
        print(payload)
        precision     = 10
        bits_per_char = 2
        geohash = GLOBAL_VARS.RSUS[self._identity]

        file_path = os.path.join(self.data_dir, f'9-p_{precision}-bpc_{bits_per_char}-G.pkl')
        with open(file_path, 'rb') as handle:
            nx_g = pickle.load(handle)

        router = rg.Route_Generator(nx_g, precision, bits_per_char)
        router.find_route(payload)

    def process_tasks_in_queue(self):
        # Should I not pop immediately, but only when the pipeline is "done"
        while True:
            task_count = self._r.llen(self._identity)
            if task_count > 0:
                print(f'{self._identity} has {task_count} tasks in queue.')
                task = self._r.lpop(self._identity)

                payload = json.loads(task)
                task_type = payload['task_type']
                if task_type == GLOBAL_VARS.FULL_ROUTE:
                    self.generate_route(payload)
                if task_type == GLOBAL_VARS.ROUTE_PLANNING:
                    self.plan_route(payload)
                if task_type == GLOBAL_VARS.PARTIAL_ROUTE:
                    self.generate_partial_route(payload)

    '''
        SEC004: Service-specific Handler Functions
    '''