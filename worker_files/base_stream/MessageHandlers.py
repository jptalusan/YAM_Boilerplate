# from zmq.utils import jsonapi as json
import json
import sys
sys.path.append('..')
from utils.Utils import *
from utils.constants import *

# Different classes for each type of socket because they vary in how they send messages
class RouterMessageHandler(object):
    def __init__(self, json_load=-1):
        self._json_load = json_load

    def __call__(self, msg):
        if __debug__ == DEBUG:
            print("RouterMessageHandler: __call__: ", msg)
        """
        Gets called when a messages is received by the stream this handlers is
        registered at. *msg* is a list as return by
        :meth:`zmq.core.socket.Socket.recv_multipart`.

        """
        i = self._json_load
        msg_type = decode(msg[i])

        # Rest of array is the message itself
        del msg[i]

        # Get the actual message handler and call it
        if msg_type.startswith('_'):
            raise AttributeError('%s starts with an "_"' % msg_type)

        getattr(self, msg_type)(*msg)

class DealerMessageHandler(object):
    def __init__(self, json_load=-1):
        self._json_load = json_load

    def __call__(self, msg):
        if __debug__ == DEBUG:
            print("DealerMessageHandler: __call__: ", msg)
        """
        Gets called when a messages is received by the stream this handlers is
        registered at. *msg* is a list as return by
        :meth:`zmq.core.socket.Socket.recv_multipart`.

        """
        i = self._json_load
        msg_type = decode(msg[i])

        # Rest of array is the message itself
        # del msg[i]

        # Get the actual message handler and call it
        if msg_type.startswith('_'):
            raise AttributeError('%s starts with an "_"' % msg_type)

        getattr(self, msg_type)(*msg)

class RequestHandler(object):
    """
    Base class for message handlers for a :class:`ZMQProcess`.

    Inheriting classes only need to implement a handler function for each
    message type.

    """

    def __init__(self, json_load=-1):
        self._json_load = json_load

    def __call__(self, msg):
        print("__call__: ", msg)
        """
        Gets called when a messages is received by the stream this handlers is
        registered at. *msg* is a list as return by
        :meth:`zmq.core.socket.Socket.recv_multipart`.

        """
        # Try to JSON-decode the index "self._json_load" of the message
        i = self._json_load
        msg_type, data = json.loads(msg[i])
        # print("type:{}, data:{}".format(msg_type, data))
        msg[i] = data

        # Get the actual message handler and call it
        if msg_type.startswith('_'):
            raise AttributeError('%s starts with an "_"' % msg_type)

        getattr(self, msg_type)(*msg)
