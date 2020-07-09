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
        self._r =  redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)
        return
        
    def heartbeat(self, *data):
        # self._r.flushdb()

        print("DATA:", data)
        sender = decode(data[0])
        payload = json.loads(decode(data[1]))
        print(f"heartbeat received: {payload} from {sender}")
        worker = {f"worker:{sender}": payload }
        self._r.hmset(sender, payload)
        for worker in self._r.scan_iter(match='Worker-*'):
            print(f'Worker: {worker}')

        return

