import sys
sys.path.append('..')
from utils.Utils import *
from utils.constants import *
from base_stream import MessageHandlers as mh
import time

class WorkerHandler(mh.DealerMessageHandler):
    """Handels messages arrvinge at the PongProc’s REP stream."""
    # Debug
    # Class Variables
    received_counter = 0
    some_task_queue = []

    def __init__(self, backend_stream, stop, mani_handler, extract_handler):
        # json_load is the element index of the msg_type
        super().__init__(json_load=0)
        # Instance variables
        self._backend_stream = backend_stream
        self._stop = stop
        self._mani_handler = mani_handler
        self._extract_handler = extract_handler

        WorkerHandler.received_counter = 0
        WorkerHandler.some_task_queue = []

        print("WorkerHandler:__init__")
        self._backend_stream.send_multipart([encode(WORKER_READY)])

    def mani(self, *data):
        sender = decode(data[0])
        data_arr = data[1]
        c = self._mani_handler.make_mani(WorkerHandler.received_counter, data_arr)
        print(c)
        if c == 4:
            print("Sending kill code...")
            self._backend_stream.send_multipart([b'plzdiekthxbye'])
            self._stop()
        
    def ping(self, data):
        """Send back a pong."""
        rep = self._ping_handler.make_pong(data)
        self._backend_stream.send_json(rep)

    def plzdiekthxbye(self, *data):
        print("Stopping:WorkerProcess")
        """Just calls :meth:`WorkerProcess.stop`."""
        self._stop()

    def extract_task(self, *data):
        print(data)
        print(type(data))
        print(type(data[1]))
        sender = decode(data[0])
        data_arr = decode(data[1])
        print("Received {}:{} from Broker".format(EXTRACT_TASK, data_arr))
        extracted_data = self._extract_handler.extract_features(data[1:])
        print("Extracted: {}".format(extracted_data))

        self._backend_stream.send_multipart([encode(EXTRACT_RESPONSE), b'Done extracting...'])


# TODO: Create a handler for each type of message for the broker
class ManiHandler(object):

    def make_mani(self, counter, *data_arr):
        """Creates and returns a pong message."""
        print("Make_mani got {}".format(data_arr))
        WorkerHandler.received_counter += 1
        return WorkerHandler.received_counter

class ExtractHandler(object):
    def extract_features(self, *data):
        d = data[0]
        print("Doing some extracting on {}.".format(d[0]))
        time.sleep(1)
        print("Done extracting...")
        return int(d[0]) + 1