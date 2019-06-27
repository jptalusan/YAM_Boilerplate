import zmq
from .BrokerHandler import BrokerHandler, PingHandler

import sys
sys.path.append('..')
from base import ZmqProcess as zp

# TODO: Change names
class BrokerProcess(zp.ZmqProcess):
    """
    Main processes for the Ponger. It handles ping requests and sends back
    a pong.

    """

    def __init__(self, bind_addr, bind_addr2, identity=None):
        super().__init__()

        self.bind_addr = bind_addr
        self.bind_addr2 = bind_addr2

        self.identity = identity

        # TODO: add some self.backend_stream etc... to connect this Process to some other socket
        self.frontend_stream = None
        self.backend_stream = None

        self.ping_handler = PingHandler()

    def setup(self):
        """Sets up PyZMQ and creates all streams."""
        super().setup()

        # Create the stream and add the message handler
        self.frontend_stream, _ = self.stream(zmq.ROUTER, self.bind_addr, bind=True, identity=self.identity)

        # Create the stream and add the message handler
        self.backend_stream, _ = self.stream(zmq.ROUTER, self.bind_addr2, bind=True, identity=self.identity)

        # Attach handlers to the streams
        self.frontend_stream.on_recv(BrokerHandler(self.frontend_stream, self.backend_stream, 
                                                   self.stop, self.ping_handler))

        # Attach handlers to the streams
        self.backend_stream.on_recv(BrokerHandler(self.frontend_stream, self.backend_stream, 
                                                  self.stop, self.ping_handler))

    def run(self):
        """Sets up everything and starts the event loop."""
        self.setup()
        self.loop.start()

    def stop(self):
        """Stops the event loop."""
        print("Stopping.")
        self.loop.stop()
