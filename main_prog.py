from broker_files.BrokerProcess import *
from worker_files.WorkerProcess import *
import zmq
from utils.Utils import *
from multiprocessing import Process

host = '127.0.0.1'
port = 5678
port2 = 5679


def client():
    """Sends ping requests and waits for replies."""
    context = zmq.Context()
    sock = context.socket(zmq.DEALER)
    sock.identity = (u"Client-%s" % str(0).zfill(3)).encode('ascii')
    sock.connect('tcp://%s:%s' % (host, port))

    for i in range(5):
        sock.send_multipart([b'hopia', bytes([i]), b'HEY'])

        # msg = sock.recv_multipart()
        # print(msg)

    # sock.send_multipart([b'plzdiekthxbye'])
    # print("Sent kill code.")

if __name__ == '__main__':
    broker = BrokerProcess(bind_addr=('*', port), bind_addr2=('*', port2), identity='Broker')
    broker.start()

    worker = WorkerProcess(bind_addr=(host, port2), identity='Worker-000')
    worker.start()

    Process(target=client, args=()).start()

    broker.join()
    worker.join()
