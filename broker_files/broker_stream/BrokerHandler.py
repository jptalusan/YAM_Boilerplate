from base_stream import MessageHandlers as mh
from utils.constants import *
from utils.Utils import *
import sys
import json
sys.path.append('..')


class BrokerHandler(mh.RouterMessageHandler):
    """Handles messages that arrive on the broker streams (backend and frontend)"""
    """Just name the function the same as your msg_type and it will handle it."""

    some_broker_task_queue = []
    # TODO: Make this a class, similar to the old one
    some_broker_worker_queue = []

    def __init__(self, frontend_stream, backend_stream, stop):
        print("BrokerHandler.__init__()")
        super().__init__(json_load=1)
        self._frontend_stream = frontend_stream
        self._backend_stream = backend_stream
        self._stop = stop

        # TODO: This is only for testing
        self.client = ''
        BrokerHandler.some_broker_task_queue = []

    def plzdiekthxbye(self, *data):
        print("Received plzdiekthxbye")
        """Just calls :meth:`BrokerProcess.stop`."""
        self._stop()

    def extract_query(self, *data):
        self.client = decode(data[0])
        query = decode(data[1])
        query = json.loads(query)

        print("Received Extract {} from {}".format(query['other'], query['sender']))
        BrokerHandler.some_broker_task_queue.append(EXTRACT_QUERY)
        self.send_task_to_worker()
        # TODO: Probably needs some information on available workers...

    def extract_response(self, *data):
        sender = decode(data[0])
        msg = decode(data[1])
        print("Extract {} response: {}".format(sender, msg))

        self.notify_client(b' World!')
        self.worker_ready(sender)

    def worker_ready(self, *data):
        print("A worker is ready:{}".format(data))
        if isinstance(data[0], bytes):
            worker_addr = decode(data[0])
        else:
            worker_addr = data[0]

        BrokerHandler.some_broker_worker_queue.append(worker_addr)

        if len(BrokerHandler.some_broker_task_queue) > 0:
            self.send_task_to_worker()
        else:
            print("No tasks available...")

    # TODO: Load balancer
    def send_task_to_worker(self):
        if len(BrokerHandler.some_broker_worker_queue) > 0:
            task = BrokerHandler.some_broker_task_queue.pop()
            addr = BrokerHandler.some_broker_worker_queue.pop()
            print("Task: {}".format(task))
            # TODO: Serialize
            self._backend_stream.send_multipart(
                [encode(addr), encode(EXTRACT_TASK), b'9999', bytes(1)])

    def notify_client(self, *data):
        print("Notifying {}".format(self.client))
        message = data[0]
        self._frontend_stream.send_multipart([b'Client-000', b'Hello', message])
