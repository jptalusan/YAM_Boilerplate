import zmq
from .BrokerHandler import BrokerHandler
from .SendHandler import SendHandler

import sys
sys.path.append('..')
from base_stream import ZmqProcess as zp

# https://gist.github.com/abhinavsingh/6378134
# TODO: Change names
class BrokerProcess(zp.ZmqProcess):
    """
    Main processes for the Ponger. It handles ping requests and sends back
    a pong.

    """

    def __init__(self, bind_addr, backend_addr, subs_addr, identity=None):
        super().__init__()

        self.bind_addr = bind_addr
        self.backend_addr = backend_addr
        self.subs_addr = subs_addr

        self.identity = identity

        # TODO: add some self.backend_stream etc... to connect this Process to some other socket
        self.frontend_stream = None
        self.backend_stream = None
        self.subscribe_stream = None

        return

    def setup(self):
        """Sets up PyZMQ and creates all streams."""

        # This setup() function overrides the one in ZmqProcess by default, so
        #   we have to call the superclass' setup() function
        super().setup()

        # Create the stream and add the message handler
        self.frontend_stream, _  = self.stream(zmq.ROUTER, self.bind_addr, bind=True, identity=self.identity)
        self.backend_stream, _   = self.stream(zmq.ROUTER, self.backend_addr, bind=True, identity=self.identity)
        self.subscribe_stream, _ = self.stream(zmq.SUB, self.subs_addr, bind=True, identity=self.identity, subscribe=b"topic")

        # Create the handlers
        sendHandler = SendHandler(sender='Backend')
        # Also, pass this BrokerProcess to the BrokerHandler as an argument
        brokerHandler = BrokerHandler(self.identity, self)

        self.frontend_stream.on_recv(brokerHandler)

        # Attach handlers to the streams
        self.backend_stream.on_recv(brokerHandler)
        self.backend_stream.on_send(sendHandler.logger)
        # Subscribe: Consumes data in the form: ['topic', 'msg_type','identity, 'payloads'....]
        self.subscribe_stream.on_recv(brokerHandler)

        return

