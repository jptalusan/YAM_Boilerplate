from utils.Utils import *
from utils.constants import *
from base_stream import MessageHandlers as mh
import zmq
import json

class WorkerHandler(mh.RouterMessageHandler):
    """Handels messages arrvinge at the PongProc’s REP stream."""
    worker_dict = {'Worker-0000': 6000,
                   'Worker-0001': 6001,
                   'Worker-0002': 6002}

    '''
        SEC001: Main Functions
    '''
    def __init__(self, identity, base_process): # identity, backend_stream, publish_stream, stop, extract_handler, train_handler):
        # json_load is the element index of the msg_type (differs with socket type)
        super().__init__(json_load=1)
        # Instance variables
        self._base_process    = base_process
        self._backend_stream  = base_process.backend_stream

        self._identity        = identity

        print("Worker Identity: {}".format(self._identity))

        self.context = zmq.Context()
        print("WorkerHandler:__init__")
        self._peer_sockets = {}
        for worker, port in self.worker_dict.items():
            if worker == self._identity:
                continue
            temp_sock = self.context.socket(zmq.DEALER)
            print(f'Registering peer: {worker}:{port}')
            temp_sock.identity = encode(self._identity)
            temp_sock.connect('tcp://%s:%s' % (worker, port))
            
            self._peer_sockets[worker] = temp_sock
        
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
    def populate_neighbors(self, *data):
        print("Populating neighbors...")

    def test_ping_query(self, *data):
        sender = decode(data[0])
        print(f'Received {decode(data[1])} from {sender}')
        self.test_ping_response(sender, 'Pong')
        return
    
    def test_ping_response(self, *data):
        sender = data[0]
        payload = data[1]
        print("Sending response.")
        self._backend_stream.send_multipart([encode(sender), encode(payload)])
        return

    def pipeline_ping_query(self, *data):
        print(f'Received: {data}.')
        sender = decode(data[0])
        payload = json.loads(decode(data[1]))
        pipeline = payload['pipeline']
        timestamp = payload['time']
        current = pipeline.pop(0)
        complete_payload = {'time': timestamp, 'pipeline': pipeline}
        if len(pipeline) != 0:
            payload = json.dumps(complete_payload)
            self._peer_sockets[pipeline[0]].send_multipart([b'pipeline_ping_query', encode(payload)])
        else:
            # Need to get the address of the client here.
            self._backend_stream.send_multipart([encode('Client-0000'), encode(str(timestamp)), encode('PONG')])
            print("Sending the result back to sender.")

        return

    '''
        SEC004: Service-specific Handler Functions
    '''