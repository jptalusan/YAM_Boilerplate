import zmq
import json
from multiprocessing import Process
import time

# https://stackoverflow.com/questions/39163872/zeromq-advantages-of-the-router-dealer-pattern

# Port access the port opened in docker-compose (LEFT side, 7000:7000), in EXPOSE in dockerfile, it is for internal comm between containers
# Localhost is used, because we can't access it by hostname unless we do some DNS
# https://stackoverflow.com/questions/37242217/access-docker-container-from-host-using-containers-name
# https://stackoverflow.com/questions/35828487/docker-1-10-access-a-container-by-its-hostname-from-a-host-machine/
host = 'localhost'
port = 6000

decode = lambda x: x.decode('utf-8')
encode = lambda x: x.encode('ascii')
current_seconds_time = lambda: int(round(time.time()))

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

# Worker 0000 to Worker 0001 and back to client
def pipeline_ping():
    # How do i just listen to all ports?
    context = zmq.Context()

    host = 'localhost'
    port = 6001

    # Connect to weather server
    receiver = context.socket(zmq.DEALER)
    receiver.identity = (u"Client-%s" % str(0).zfill(4)).encode('ascii')
    receiver.connect('tcp://%s:%s' % (host, port))
    payload = json.dumps({"pipeline":['Worker-0001', 'Worker-0000', 'Worker-0002']})
    receiver.send_multipart([b'pipeline_ping_query', payload.encode('ascii')])
    print("Sent: Ping")

    worker_dict = {'Worker-0000': 6000,
                   'Worker-0001': 6001,
                   'Worker-0002': 6002}

    # Initialize poll set
    poller = zmq.Poller()

    for worker, port in worker_dict.items():
        sender = context.socket(zmq.DEALER)
        sender.identity = (u"Client-%s" % str(0).zfill(4)).encode('ascii')
        sender.connect('tcp://%s:%s' % (host, port))
        poller.register(sender, zmq.POLLIN)

    # Process messages from both sockets
    received = False
    while not received:
        try:
            socks = dict(poller.poll())
        except KeyboardInterrupt:
            break
        for k, v in socks.items():
            message = k.recv()
            decoded = message.decode('utf-8')
            if decoded == 'PONG':
                print(f'Received: {decoded}')
                received = True
                break

if __name__ == '__main__':
    Process(target=pipeline_ping, args=()).start()
    
