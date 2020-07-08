from base_stream import MessageHandlers as mh
from utils.constants import *
from utils.Utils import *
import json
import redis

# TODO: Should I separate functions not entirely related to brokerhandler? (Probably)
# Like what i did with the workerhandler

class PullHandler(mh.PullMessageHandler):
    """Handles messages that arrive on the broker streams (backend and frontend)"""
    """Just name the function the same as your msg_type and it will handle it."""

    # Place variables here that i plan on reusing like the arrays etc...

    def __init__(self):
        print("BrokerHandler.__init__()")
        super().__init__(json_load=0)
        # self._frontend_stream = frontend_stream
        # self._backend_stream = backend_stream
        # self._stop = stop
        self._r =  redis.Redis(host='redis', port=6379, db=0)
        return
        
    def heartbeat(self, *data):
        print("DATA:", data)
        sender = decode(data[0])
        payload = json.loads(decode(data[1]))
        print(f"heartbeat received: {payload} from {sender}")
        worker = {f"worker:{sender}": payload }
        self._r.hmset(sender, payload)
        print("HGETALL", self._r.hgetall(f"{sender}"))
        print("KEYS", self._r.keys())

        return

