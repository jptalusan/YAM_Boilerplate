import zmq
from .WorkerHandler import WorkerHandler
from .SendHandler import SendHandler

import sys
sys.path.append('..')
from base_stream import ZmqProcess as zp
class WorkerProcess(zp.ZmqProcess):
    """
    Main processes for the Ponger. It handles ping requests and sends back
    a pong.

    """
    def __init__(self, bind_addr, publish_addr, identity=None):
        super().__init__()

        self.bind_addr = bind_addr
        self.publish_addr = publish_addr
        self.identity = identity

        self.backend_stream = None
        self.publish_stream = None

    def setup(self):
        """Sets up PyZMQ and creates all streams."""
        super().setup()

        # Create the stream and add the message handler
        # Take note that socket types are different compared to some other process
        self.backend_stream, _ = self.stream(zmq.DEALER, self.bind_addr, bind=False, identity=self.identity)
        self.publish_stream, _ = self.stream(zmq.PUB, self.publish_addr, bind=False, identity=self.identity)

        # Attach handlers to the streams
        self.backend_stream.on_recv(WorkerHandler(self.identity,
                                                  self.backend_stream, 
                                                  self.publish_stream,
                                                  self.stop 
                                                #   List of custom handlers here..
                                                  ))

        bakSendHandler = SendHandler(sender='Worker', recipient='Backend')
        pubSendHandler = SendHandler(sender='Worker', recipient='Subscriber')
        self.backend_stream.on_send(bakSendHandler.logger)
        self.publish_stream.on_send(pubSendHandler.logger)


    def run(self):
        """Sets up everything and starts the event loop."""
        self.setup()
        self.loop.start()

    def stop(self):
        print("Stopping {}.".format(self.identity))
        """Stops the event loop."""
        self.loop.stop()
