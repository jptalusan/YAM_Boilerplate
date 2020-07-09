from base_stream import MessageHandlers as mh
from utils.constants import *
from utils.Utils import *
import sys
import json
# Routing imports

sys.path.append('..')

# TODO: Should I separate functions not entirely related to brokerhandler? (Probably)
# Like what i did with the workerhandler
class BrokerHandler(mh.RouterMessageHandler):
    """Handles messages that arrive on the broker streams (backend and frontend)"""
    """Just name the function the same as your msg_type and it will handle it."""

    # Place variables here that i plan on reusing like the arrays etc...

    '''
        SEC001: Main Functions
    '''
    def __init__(self, identity, base_process):
        print("BrokerHandler.__init__()")
        # json_load is the element index of the msg_type (differs with socket type)
        super().__init__(json_load=1)
        self._base_process      = base_process
        self._frontend_stream   = base_process.frontend_stream
        self._backend_stream    = base_process.backend_stream
        self._heartbeat_stream  = base_process.heartbeat_stream
        self._identity          = identity
        self._r                 = base_process.redis

        # TODO: This is only for testing
        BrokerHandler.client = ''

        return

    # Send heartbeat to different brokers and everyone else connected to this?
    def send_heartbeat(self):
        return

    # Moved them to the PullHandler, but must communicate with redis for access to which nodes are dead?
    def heartbeat(self, *data):
        print("Received heartbeat")

    # Handle sent heartbeats to this broker
    def handle_heartbeat(self, *data):
        return

    def error(self, *data):
        print("Error:{}".format(data))
        print("Recived error, worker should just be ready again.")
        sender = decode(data[1])
        self.worker_ready(sender)
        return

    '''
        SEC003: Test Handler Functions
            Test functions for checking if the docker/middleware is working 
            in terms of connectivity.
    '''
    def query_services(self, *data):
        sender = decode(data[0])
        BrokerHandler.client = sender

        payload = {}
        for worker in self._r.scan_iter(match='Worker-*'):
            # print(worker)
            
            # print("HGETALL", self._r.hgetall(f"{worker}"))
            payload[worker] = self._r.hgetall(worker)
        # print(payload)

        self._frontend_stream.send_multipart([encode(BrokerHandler.client), encode(json.dumps(payload))])
        return
        
    def test_ping_query(self, *data):
        sender = decode(data[0])
        BrokerHandler.client = sender
        print(f'Received {decode(data[1])} from {BrokerHandler.client}')
        self.test_ping_response('Pong')
        return
    
    def test_ping_response(self, *data):
        payload = data[0]
        print(data[0], type(data[0]))
        print("Sending response.")
        self._frontend_stream.send_multipart([encode(BrokerHandler.client), encode(payload)])
        return

    def reintroduce_workers(self, *data):
        sender = decode(data[0])
        BrokerHandler.client = sender

        print("Introducing workers to each other...")
        payload = {}
        workers = []
        for worker in self._r.scan_iter(match='Worker-*'):
            payload[worker] = self._r.hgetall(worker)
            workers.append(worker)
            print(worker)

        # print("YES")
        # self._backend_stream.send_multipart([encode('broker'), encode('populate_neighbors'), encode(json.dumps({"payload":"BOO"}))])
        for worker in workers:
            self._backend_stream.send_multipart([encode('broker'), encode('populate_neighbors'), encode(json.dumps(payload))])

        self._frontend_stream.send_multipart([encode(BrokerHandler.client), encode("Done reintroducing...")])
        print("Done reintroducing...")