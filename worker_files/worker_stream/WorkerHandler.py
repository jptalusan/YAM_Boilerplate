import sys
import time
import os
import json
from datetime import datetime
import numpy as np

sys.path.append('..')
from utils.Utils import *
from utils.constants import *
from base_stream import MessageHandlers as mh

class WorkerHandler(mh.DealerMessageHandler):
    """Handels messages arrvinge at the PongProc’s REP stream."""
    # Debug
    # Class Variables
    received_counter = 0
    some_task_queue = []

    def __init__(self, identity, backend_stream, publish_stream, stop):
        # json_load is the element index of the msg_type
        super().__init__(json_load=0)
        # Instance variables
        self._backend_stream = backend_stream
        self._publish_stream = publish_stream
        self._stop = stop
        self._identity = identity

        WorkerHandler.received_counter = 0
        WorkerHandler.some_task_queue = []

        print("WorkerHandler:__init__")
        self._backend_stream.send_multipart([encode(WORKER_READY)])

    def plzdiekthxbye(self, *data):
        print("Stopping:WorkerProcess")
        """Just calls :meth:`WorkerProcess.stop`."""
        self._stop()

    # Write to lock/config file in memory, whether you are under load.
    def test_ping_task(self, *data):
        print("Received task from broker: {}".format(data))
        sender = decode(data[0])
        task_id = decode(data[1])

        payload_count = int(decode(data[2]))
        print("Paylod count: {}".format(payload_count))
        for i in range(payload_count):
            load_type = decode(data[3 + (i * 2)])
            if load_type == 'String' or load_type == 'Bytes':
                load = decode(data[3 + (i * 2) + 1])
                print("Type: {}, load: {}".format(load_type, load))
                json_query = json.loads(load) #Assumming its always json?
            elif load_type == 'ZippedPickleNdArray':
                load = unpickle_and_unzip(data[3 + (i * 2) + 1])
                print("Type: {}, load shape: {}".format(load_type, load.shape))
                narr = load

        payload_value = json_query['task_sleep']
        query_time = json_query['queried_time']
        print("Task time:{}, task payload:{}".format(query_time, payload_value))

        # Some processing
        time.sleep(payload_value)
        processed_payload = payload_value * 100

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        msg = "Worker is done training."
        payload = json.dumps({"task_id": task_id, 
                              "createdAt":now,
                              "msg":msg,
                              "processed_payload":processed_payload})

        self._publish_stream.send_multipart([b"topic", 
                                        b"status", 
                                        encode(self._identity), 
                                        encode(payload)])
