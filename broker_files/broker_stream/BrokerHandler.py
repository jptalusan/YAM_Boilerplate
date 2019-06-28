from base_stream import MessageHandlers as mh
from utils.constants import *
from utils.Utils import *
from classes.WorkerQueue import WorkerQueue
from classes.Worker import Worker
import sys
import json
import random

import numpy as np

sys.path.append('..')

# TODO: Should I separate functions not entirely related to brokerhandler? (Probably)
# Like what i did with the workerhandler
class BrokerHandler(mh.RouterMessageHandler):
    """Handles messages that arrive on the broker streams (backend and frontend)"""
    """Just name the function the same as your msg_type and it will handle it."""

    some_broker_task_queue = []
    workers = WorkerQueue()

    # Place variables here that i plan on reusing like the arrays etc...

    def __init__(self, frontend_stream, backend_stream, stop):
        # print("BrokerHandler.__init__()")
        super().__init__(json_load=1)
        self._frontend_stream = frontend_stream
        self._backend_stream = backend_stream
        self._stop = stop

        # TODO: This is only for testing
        self.client = ''

        self.aggregated_data = []
        BrokerHandler.some_broker_task_queue = []

    def plzdiekthxbye(self, *data):
        print("Received plzdiekthxbye")
        """Just calls :meth:`BrokerProcess.stop`."""
        self._stop()

    def extract_query(self, *data):
        self.client = decode(data[0])
        query = decode(data[1])
        query = json.loads(query)

        new_tasks = self.extract_query_json_and_generate_tasks(query)

        # For now we assume that only one query at a time, since we are only measuring speeds and overhead
        BrokerHandler.some_broker_task_queue.clear()
        BrokerHandler.some_broker_task_queue.extend(new_tasks)

        self.send_task_to_worker()
        # TODO: Probably needs some information on available workers...

    def extract_response(self, *data):
        sender = decode(data[0])
        msg = decode(data[1])
        array = unpickle_and_unzip(data[2])

        self.aggregated_data.append(data[2])
        # print("Data len:{}".format(len(data)))
        print("Extract {} response: {}, shape: {}".format(sender, msg, array.shape))

        self.notify_client(b' World!')
        worker_addr = data[0]
        BrokerHandler.workers.ready(Worker(worker_addr, b'', b''))
        self.send_task_to_worker()

    def worker_ready(self, *data):
        print("A worker is ready:{}".format(data))
        worker_addr = data[0]
        BrokerHandler.workers.ready(Worker(worker_addr, b'', b''))
        self.send_task_to_worker()

    # This is a random task assignment, but it is load balanced because of a worker queue
    def send_task_to_worker(self):
        # Loop through tasks, as long as there is a task and there is a worker free, send tasks.
        # else, break and wait for new workers
        if len(BrokerHandler.some_broker_task_queue) > 0:
            while len(BrokerHandler.some_broker_task_queue) > 0:
                if len(BrokerHandler.workers.queue) > 0:
                    task = BrokerHandler.some_broker_task_queue.pop()
                    addr = BrokerHandler.workers.next()

                    # TODO: Serialize
                    self._backend_stream.send_multipart(
                        [encode(addr), encode(EXTRACT_TASK), encode(task)])
                else:
                    break
        else:
            print("No tasks available...")
            print(self.aggregate_data(self.aggregated_data).shape)

    def notify_client(self, *data):
        print("Notifying {}".format(self.client))
        message = data[0]
        self._frontend_stream.send_multipart([b'Client-000', b'Hello', message])

    def extract_query_json_and_generate_tasks(self, json_request):
        # Hardcoded labels since there are 12 labels possible in the dataset
        # And it is easier to query random values by label in influxDB
        print("-> extract_query_parser")
        task_queue = []
        for i in range(1, 13):
            json_request["label"] = i

            task_queue.append(json.dumps(json_request))

        return task_queue

    def aggregate_data(self, aggregated_pickles):
        output = []
        for pickled in aggregated_pickles:
            unpickld = unpickle_and_unzip(pickled)
            temp = unpickld.tolist()
            output.extend(temp)
        np_output = np.asarray(output)
        return np_output
