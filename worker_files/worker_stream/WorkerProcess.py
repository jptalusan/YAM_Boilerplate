import zmq
from .WorkerHandler import WorkerHandler
from .SendHandler import SendHandler
from base_stream import ZmqProcess as zp

class WorkerProcess(zp.ZmqProcess):
    """
    Main processes for the Ponger. It handles ping requests and sends back
    a pong.

    """
    def __init__(self, bind_addr, broker_addr, identity=None):
        super().__init__()
        self.bind_addr = bind_addr
        self.broker_addr = broker_addr
        self.identity = identity

        self.backend_stream = None
        self.broker_stream = None
        return

    def setup(self):
        """Sets up PyZMQ and creates all streams."""
        super().setup()

        # Create the stream and add the message handler
        # Take note that socket types are different compared to some other process
        self.backend_stream, _ = self.stream(zmq.ROUTER, self.bind_addr, bind=True, identity=self.identity)
                                # self.stream(zmq.SUB, self.subs_addr, bind=True, identity=self.identity, subscribe=b"topic")
        self.broker_stream, _ = self.stream(zmq.SUB, self.broker_addr, bind=False, identity=self.identity, subscribe=b"broker")

        # Create the handlers
        workerHandler = WorkerHandler(self.identity, self)

        self.backend_stream.on_recv(workerHandler)

        self.broker_stream.on_recv(workerHandler)

        bakSendHandler = SendHandler(sender='Worker', recipient='Backend')
        # Attach handlers to the streams
        self.backend_stream.on_send(bakSendHandler.logger)

        return

    def stop(self):
        """Stops the event loop."""
        # Override the ZmqProcess.stop() to print identity
        print("Stopping {}.".format(self.identity))
        super().stop()
        return

