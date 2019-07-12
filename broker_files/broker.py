from broker_stream import BrokerProcess as bp

import os

FRONTEND_PORT = os.environ['FRONTEND_PORT']
BACKEND_PORT = os.environ['BACKEND_PORT']
SUBSCRIBE_PORT = os.environ['SUBSCRIBE_PORT']

if __name__ == "__main__":
    bp.BrokerProcess(bind_addr=('*', FRONTEND_PORT), 
                     backend_addr=('*', BACKEND_PORT), 
                     subs_addr=('*', SUBSCRIBE_PORT), 
                     identity='Broker').start()