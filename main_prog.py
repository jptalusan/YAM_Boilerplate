import zmq
import json
from multiprocessing import Process
import time
import blosc
import pickle

host = 'localhost'
port = 7000

decode = lambda x: x.decode('utf-8')
encode = lambda x: x.encode('ascii')
current_seconds_time = lambda: int(round(time.time()))

def zip_and_pickle(obj, flags=0, protocol=-1):
    """pickle an object, and zip the pickle before sending it"""
    p = pickle.dumps(obj, protocol)
    z = blosc.compress(p, typesize=8)
    return z

def unpickle_and_unzip(pickled):
    unzipped = blosc.decompress(pickled)
    unpickld = pickle.loads(unzipped)
    return unpickld

# Query types
TEST_PING_QUERY = 'test_ping_query'
# Just to test if all the connections are working.
def ping():
    context = zmq.Context()
    broker_sock = context.socket(zmq.DEALER)
    broker_sock.identity = (u"Client-%s" % str(0).zfill(4)).encode('ascii')
    broker_sock.connect('tcp://%s:%s' % (host, port))

    dict_req = {}
    dict_req['task_count'] = 6
    dict_req['task_sleep'] = 2
    dict_req = json.dumps(dict_req)
    
    start = time.time()

    broker_sock.send_multipart([encode(TEST_PING_QUERY), encode(dict_req)])
    print("Sent: Ping")

    try:
        while True:
            msg = broker_sock.recv_multipart()
            response = msg[0]
            print("Received from broker:{}".format(unpickle_and_unzip(response)))
            if msg:
                elapsed = time.time() - start
                print("Total time: {}".format(elapsed))
                break
    except zmq.ContextTerminated:
        return

if __name__ == '__main__':
    Process(target=ping, args=()).start()
    
