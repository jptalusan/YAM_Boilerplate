from base_stream import MessageHandlers as mh
from utils.constants import *
from utils.Utils import *

# import panda as pd
import sys
sys.path.append('..')


# TODO: Should I separate functions not entirely related to brokerhandler? (Probably)
# Like what i did with the workerhandler

class PubHandler(mh.PubMessageHandler):
    """Handles messages that arrive on the broker streams (backend and frontend)"""
    """Just name the function the same as your msg_type and it will handle it."""

    some_broker_task_queue = []
    # Place variables here that i plan on reusing like the arrays etc...

    def __init__(self, frontend_stream, backend_stream, stop):
        print("PubHandler.__init__()")
        super().__init__(json_load=1)
        self._frontend_stream = frontend_stream
        self._backend_stream = backend_stream
        self._stop = stop
        return
        
    def status(self, *data):
        print("Subs sent:{}".format(data))
        return

    def error(self, *data):
        print("Recived error, worker should just be ready again.")
        sender = decode(data[0])
        return

