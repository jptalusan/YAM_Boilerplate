from utils.Utils import *
import json

class Worker(object):
    def __init__(self, address, capability, status):
        if __debug__:
            print("Created worker object with address %s" % decode(address))
        self.address = decode(address)
        self.capability = decode(capability)
        self.status = decode(status)
        # self.last_alive = last_alive
        # self.last_alive = current_seconds_time() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS

    def __repr__(self):
        ddict = {}
        ddict['capability'] = self.capability
        ddict['status'] = self.status
        ddict['last_alive'] = self.last_alive
        return json.dumps(ddict)

    def __str__(self):
        ddict = {}
        ddict['capability'] = self.capability
        ddict['status'] = self.status
        ddict['last_alive'] = self.last_alive
        return json.dumps(ddict)
