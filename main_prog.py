import zmq
import json
from multiprocessing import Process

host = 'localhost'
port = 7000

decode = lambda x: x.decode('utf-8')
encode = lambda x: x.encode('ascii')

EXTRACT_QUERY= 'extract_query'
EXTRACT_TASK = 'extract_task'
EXTRACT_RESPONSE = 'extract_response'

WORKER_READY = 'worker_ready'

def client():
    """Sends ping requests and waits for replies."""
    context = zmq.Context()
    broker_sock = context.socket(zmq.DEALER)
    broker_sock.identity = (u"Client-%s" % str(0).zfill(3)).encode('ascii')
    broker_sock.connect('tcp://%s:%s' % (host, port))

    dict_req = {}
    dict_req['sender'] = broker_sock.identity.decode('ascii')
    dict_req['other'] = 'I AM CLIENT'
    dict_req = json.dumps(dict_req)

    broker_sock.send_multipart([encode(EXTRACT_QUERY), encode(dict_req)])
    try:
        while True:
            msg = broker_sock.recv_multipart()
            print(msg)
            if msg:
                break
    except zmq.ContextTerminated:
      return

if __name__ == '__main__':
    Process(target=client, args=()).start()
