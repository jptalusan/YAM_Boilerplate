from broker_stream import BrokerProcess as bp

import os

FRONTEND_PORT = os.environ['FRONTEND_PORT']
BACKEND_PORT = os.environ['BACKEND_PORT']

if __name__ == "__main__":
    bp.BrokerProcess(bind_addr=('*', FRONTEND_PORT), 
                     bind_addr2=('*', BACKEND_PORT), 
                     identity='Broker').start()