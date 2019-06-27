import sys
sys.path.append('..')
from utils.Utils import *
from base import MessageHandlers as mh

class BrokerHandler(mh.RouterMessageHandler):
    """Handels messages arrvinge at the PongProc’s REP stream."""
    def __init__(self, frontend_stream, backend_stream, stop, ping_handler):
        super().__init__(json_load=1)
        self._frontend_stream = frontend_stream
        self._backend_stream = backend_stream
        self._stop = stop
        self._ping_handler = ping_handler

    def hopia(self, *data):
        print("Received Hopia({})".format(data))
        sender = decode(data[0])
        data_arr = data[1]

        self._backend_stream.send_multipart([encode('Worker-000'), b'mani', b'111'])
        print("Sending mani...")

    def ping(self, data):
        """Send back a pong."""
        rep = self._ping_handler.make_pong(data)
        self._rep_stream.send_json(rep)

    def plzdiekthxbye(self, *data):
        print("Received plzdiekthxbye")
        """Just calls :meth:`BrokerProcess.stop`."""
        self._stop()


# TODO: Create a handler for each type of message for the broker
class PingHandler(object):

    def make_pong(self, num_pings):
        """Creates and returns a pong message."""
        print('Pong got request number %s' % num_pings)

        return ['pong', num_pings]