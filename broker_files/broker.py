from broker_stream import BrokerProcess as bp
import os

FRONTEND_PORT = os.environ['FRONTEND_PORT']
BACKEND_PORT = os.environ['BACKEND_PORT']
HEARTBEAT_PORT = os.environ['HEARTBEAT_PORT']

if __name__ == "__main__":
    # Old/current implementation
    # Frontend accepts user queries
    # backend sends data to workers
    # subscribe port Receives "Heartbeat"
    bp.BrokerProcess(bind_addr      =  ('*', FRONTEND_PORT), 
                     backend_addr   =  ('*', BACKEND_PORT), 
                     heartbeat_addr =  ('*', HEARTBEAT_PORT), 
                     identity='Broker').start()