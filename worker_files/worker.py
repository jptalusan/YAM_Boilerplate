from worker_stream import WorkerProcess as wp

import os

BROKER_HOST = os.environ['BROKER_HOST']
BROKER_PORT = os.environ['BROKER_PORT']
ident = os.environ['WORKER_ID']

PUBLISH_HOST = os.environ['PUBLISH_HOST']
PUBLISH_PORT = os.environ['PUBLISH_PORT']

print(PUBLISH_HOST, PUBLISH_PORT)

if __name__ == "__main__":
    wp.WorkerProcess(bind_addr=(BROKER_HOST, BROKER_PORT), 
                     publish_addr=(PUBLISH_HOST, PUBLISH_PORT), 
                     identity="Worker-{}".format(ident)).start()