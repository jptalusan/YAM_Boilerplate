from base_stream import MessageHandlers as mh
import os
import datetime
import json
import sys
sys.path.append('..')
from utils.Utils import *

ident = 'Broker'
current_milli_time = lambda: int(round(time.time() * 1000))

class SendHandler(object):
    def __init__(self, sender=''):
        print("SendHandler.__init__()")
        # If you need the other streams, you can pass it here.
        self._sender = sender

    def stream_logger(self, stream, msg, status):
        # Need to take note of the size, time and sender/receiver, which stream etc...
        print("MSGstream sent Log: {}:{}:{}".format(dir(stream), msg, status))
        print("Type:{}".format(type(msg)))
    
    def logger(self, msg, status):
        # Need to take note of the size, time and sender/receiver, which stream etc...
        if __debug__ == 0:
            print("MSG sent Log: {}:{}".format(msg, status))

        if not os.path.exists(os.path.join(os.getcwd(), 'logs')):
            os.mkdir(os.path.join(os.path.join(os.getcwd(), 'logs')))
        logs_dir = os.path.join(os.getcwd(), 'logs')
        logs_path = os.path.join(logs_dir, '{}-sentmsgs.log'.format(self._sender))
        try:
            open(logs_path, 'r')
        except IOError:
            open(logs_path, 'w')

        self.write_data(logs_path, msg)

    def write_data(self, file_path, data):
        if os.stat(file_path).st_size == 0:
            with open(file_path, "w") as myfile:
                myfile.write("recipient,id,time,size" + '\n')
       
        with open(file_path, "a") as myfile:
            # time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            time = current_milli_time()

            if len(data) > 4:
                recipient = decode(data[0])
                msg_type = data[1]
                msg_id = data[2]
                total_bytes = 0
                for d in data:
                    total_bytes += len(d)

                data_row = "{},{},{},{},{}".format(recipient, msg_type, msg_id, time, total_bytes)
                myfile.write(data_row + '\n')
            # Catch all: recipient, time, total_bytes
            else:
                total_bytes = 0
                for d in data:
                    total_bytes += len(d)
                data_row = "{},{},{},{},{}".format(recipient, time, total_bytes)
                myfile.write(data_row + '\n')