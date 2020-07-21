from base_stream import MessageHandlers as mh
from utils.constants import *
from utils.Utils import *
import json
import redis
import threading
import os

TIMEOUT = float(os.environ['TIMEOUT'])
# TODO: Should I separate functions not entirely related to brokerhandler? (Probably)
# Like what i did with the workerhandler

class PullHandler(mh.PullMessageHandler):
    """Handles messages that arrive on the broker streams (backend and frontend)"""
    """Just name the function the same as your msg_type and it will handle it."""

    # Place variables here that i plan on reusing like the arrays etc...

    def __init__(self, base_process):
        print("PullHandler.__init__()")
        super().__init__(json_load=0)
        self._r =  redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)
        self._r.flushdb()

        self._publish_stream = base_process.publish_stream

        self._timeout_processor = threading.Thread(target=self.timeout, args = ())
        self._timeout_processor.start()
        return

    # Use pumba to kill containers..
    # pumba kill yaml_master_worker-0000_1
    def timeout(self):
        print("Checking timeout")
        threading.Timer(TIMEOUT/2, self.timeout, []).start()
        current_time = current_seconds_time()
        for worker in self._r.scan_iter(match='Worker-*'):
            data = self._r.hgetall(worker)
            last_beat = data['sentAt']
            last_seen = current_time - int(last_beat)
            print(worker, last_seen)
            if last_seen > TIMEOUT:
                print(f'{worker} is dead')
                self._r.hdel(worker, *data.keys())
        
    def heartbeat(self, *data):
        # print("DATA:", data)
        sender = decode(data[0])
        payload = json.loads(decode(data[1]))
        print(f"heartbeat received: {payload} from {sender}")
        worker = {f"worker:{sender}": payload }
        
        self._r.hset(sender, mapping=payload)
        for worker in self._r.scan_iter(match='Worker-*'):
            # print(worker)
            data = self._r.hgetall(worker)
            # print(f'{worker} info: {data}')

        self.reintroduce_neighbors()
        return

    def error(self, *data):
        print("Error:{}".format(data))
        print("Recived error for heartbeat....")
        return

    def reintroduce_neighbors(self):
        payload = {}
        for worker in self._r.scan_iter(match='Worker-*'):
            payload[worker] = self._r.hgetall(worker)

        self._publish_stream.send_multipart([encode('broker'), encode('populate_neighbors'), encode(json.dumps(payload))])

