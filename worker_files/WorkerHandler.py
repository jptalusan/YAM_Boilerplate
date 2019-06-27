import sys
sys.path.append('..')
from utils.Utils import *
from base import MessageHandlers as mh

class WorkerHandler(mh.DealerMessageHandler):
    """Handels messages arrvinge at the PongProc’s REP stream."""
    def __init__(self, backend_stream, stop, ping_handler):
        # json_load is the element index of the msg_type
        super().__init__(json_load=0)
        self._backend_stream = backend_stream
        self._stop = stop
        self._ping_handler = ping_handler
        
        # Debug
        self.received_counter = 0

    def mani(self, *data):
        print("Received mani({})".format(data))
        sender = decode(data[0])
        data_arr = data[1]
        print(self.received_counter)
        if self.received_counter == 4:
            print("Sending kill code...")
            self._backend_stream.send_multipart([b'plzdiekthxbye'])
            self._stop()
        
        self.received_counter += 1
        
    def ping(self, data):
        """Send back a pong."""
        rep = self._ping_handler.make_pong(data)
        self._rep_stream.send_json(rep)

    def plzdiekthxbye(self, *data):
        """Just calls :meth:`PongProc.stop`."""
        self._stop()


# TODO: Create a handler for each type of message for the broker
class PingHandler(object):

    def make_pong(self, num_pings):
        """Creates and returns a pong message."""
        print('Pong got request number %s' % num_pings)

        return ['pong', num_pings]