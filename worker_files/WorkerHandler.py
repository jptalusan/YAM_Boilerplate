import sys
sys.path.append('..')
from utils.Utils import *
from base import MessageHandlers as mh

class WorkerHandler(mh.DealerMessageHandler):
    """Handels messages arrvinge at the PongProc’s REP stream."""
    # Debug
    # Class Variables
    received_counter = 0

    def __init__(self, backend_stream, stop, mani_handler):
        # json_load is the element index of the msg_type
        super().__init__(json_load=0)
        # Instance variables
        self._backend_stream = backend_stream
        self._stop = stop
        self._mani_handler = mani_handler
        WorkerHandler.received_counter = 0

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
        """Just calls :meth:`WorkerProcess.stop`."""
        self._stop()


# TODO: Create a handler for each type of message for the broker
class ManiHandler(object):

    def make_mani(self, counter, *data_arr):
        """Creates and returns a pong message."""
        print("Make_mani got {}".format(data_arr))
        WorkerHandler.received_counter += 1
        return WorkerHandler.received_counter