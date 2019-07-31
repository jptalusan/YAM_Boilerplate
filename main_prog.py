import zmq
import json
from multiprocessing import Process
import time

host = 'localhost'
port = 7000

decode = lambda x: x.decode('utf-8')
encode = lambda x: x.encode('ascii')
current_seconds_time = lambda: int(round(time.time()))

EXTRACT_QUERY= 'extract_query'
TRAIN_QUERY = 'train_query'
EXTRACT_TRAIN_QUERY = 'extract_train_query'
CLASSIFY_QUERY = 'classify_query'
ROUTING_QUERY = 'routing_query'

WORKER_READY = 'worker_ready'

def routing():
    context = zmq.Context()
    broker_sock = context.socket(zmq.DEALER)
    broker_sock.identity = (u"Client-%s" % str(0).zfill(4)).encode('ascii')
    broker_sock.connect('tcp://%s:%s' % (host, port))

    dict_req = {}
    dict_req['distributed'] = True

    # orig_node = ox.get_nearest_node(nx_g, (36.139046, -86.792665))
    # dest_node = ox.get_nearest_node(nx_g, (36.224590, -86.835104))
    dict_req['orig'] = (36.139046, -86.792665)
    dict_req['dest'] = (36.224590, -86.835104)
    print(type(dict_req))
    dict_req = json.dumps(dict_req)
    broker_sock.send_multipart([encode(ROUTING_QUERY), encode(dict_req)])

def client():
    context = zmq.Context()
    broker_sock = context.socket(zmq.DEALER)
    broker_sock.identity = (u"Client-%s" % str(0).zfill(4)).encode('ascii')
    broker_sock.connect('tcp://%s:%s' % (host, port))

    dict_req = {}
    dict_req['database'] = 'acc'
    dict_req['model'] = 'RF'
    dict_req['train_method'] = 'DISTRIBUTED'
    dict_req['distribution_method'] = 'RND'
    dict_req['queried_time'] = current_seconds_time()
    dict_req['rows'] = 128
    dict_req['users'] = [1, 3, 5, 6, 7, 8] #[1, 3, 5, 6, 7, 8, 11, 14, 15, 16, 17, 19, 21, 22, 23, 25, 26, 27, 28, 29, 30]
    print(type(dict_req))
    dict_req = json.dumps(dict_req)

#  must scalarize data ,unbalanced
    broker_sock.send_multipart([encode(EXTRACT_TRAIN_QUERY), encode(dict_req)])
    # broker_sock.send_multipart([encode(TRAIN_QUERY), encode(dict_req)])
    # broker_sock.send_multipart([encode(EXTRACT_QUERY), encode(dict_req)])
    # broker_sock.send_multipart([encode(CLASSIFY_QUERY), encode(dict_req)])
    try:
        while True:
            msg = broker_sock.recv_multipart()
            print(msg)
            if msg:
                break
    except zmq.ContextTerminated:
      return

# Just to test if all the connections are working.
def ping():
    context = zmq.Context()
    broker_sock = context.socket(zmq.DEALER)
    broker_sock.identity = (u"Client-%s" % str(0).zfill(4)).encode('ascii')
    broker_sock.connect('tcp://%s:%s' % (host, port))
    broker_sock.send_multipart([b'test_ping_query', b'Ping'])
    print("Sent: Ping")
    msg = broker_sock.recv_multipart()
    resp = msg[0]
    print("Received: {}".format(decode(resp)))

if __name__ == '__main__':
    Process(target=client, args=()).start()
    # Process(target=ping, args=()).start()
    
