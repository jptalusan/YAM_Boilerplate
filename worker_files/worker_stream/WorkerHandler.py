from utils.Utils import *
from utils.constants import *
from base_stream import MessageHandlers as mh
import zmq
import json
import threading

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
        self._base_process    = base_process
        self._backend_stream  = base_process.backend_stream

        self._identity        = identity
        self._r               = base_process.redis
        self._r.flushdb()

        print("Worker Identity: {}".format(self._identity))

        self._peer_sockets = {}
        self.context = zmq.Context()
        print("WorkerHandler:__init__")

        self._queue_processor = threading.Thread(target=self.process_tasks_in_queue, args = ())
        self._queue_processor.start()
        
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
        print(f"Populating neighbors with {[worker for worker in payload.keys() if worker != self._identity]}")

        for worker, data in payload.items():
            if worker == self._identity:
                continue
            port = data['port']
            self.worker_dict[worker] = port

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
        if payload['pipeline'][0] == self._identity:
            print(f'Task to rpush: {payload}')
            self._r.rpush(self._identity, json.dumps(payload))
        return

    def process_tasks_in_queue(self):
        # Should I not pop immediately, but only when the pipeline is "done"
        while True:
            task_count = self._r.llen(self._identity)
            if task_count > 0:
                print(f'{self._identity} has {task_count} tasks in queue.')
                task = self._r.lpop(self._identity)
                print(f"Task lpop: {task}")

                payload = json.loads(task)
                pipeline = payload['pipeline']
                timestamp = payload['time']
                current = pipeline.pop(0)
                complete_payload = {'time': timestamp, 'pipeline': pipeline}

                # "Processing"
                time.sleep(0.5)
                
                if len(pipeline) != 0:
                    payload = json.dumps(complete_payload)

                    temp_sock = self.context.socket(zmq.DEALER)
                    temp_sock.identity = encode(self._identity)
                    temp_sock.connect('tcp://%s:%s' % (pipeline[0], self.worker_dict[pipeline[0]]))
                    temp_sock.send_multipart([b'pipeline_ping_query', encode(payload)], zmq.NOBLOCK)
                    temp_sock.close()
                else:
                    # Need to get the address of the client here.
                    self._backend_stream.send_multipart([encode('Client-0000'), encode(str(timestamp)), encode('PONG')], zmq.NOBLOCK)
                    print("Sending the result back to sender.")
            # time.sleep(5)

    '''
        SEC004: Service-specific Handler Functions
    '''