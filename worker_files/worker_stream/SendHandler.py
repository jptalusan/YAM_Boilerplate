from base_stream import MessageHandlers as mh
import os
import datetime
import json
import sys
sys.path.append('..')
from utils.Utils import *

ident = os.environ['WORKER_ID']
current_milli_time = lambda: int(round(time.time() * 1000))

class SendHandler(object):
    def __init__(self, sender='', recipient=''):
        print("SendHandler.__init__({}{})".format(sender, recipient))
        # If you need the other streams, you can pass it here.
        # self._sender = sender
        self._sender = "Worker-{}".format(ident)
        self._recipient = recipient

    def stream_logger(self, stream, msg, status):
        # Need to take note of the size, time and sender/receiver, which stream etc...
        print("MSGstream sent Log: {}:{}:{}".format(dir(stream), msg, status))
        print("Type:{}".format(type(msg)))

    def logger(self, msg, status):
        # Need to take note of the size, time and sender/receiver, which stream etc...
        if __debug__ == 0:
            print("MSG sent Log: {}:{}".format(msg, status))

        file = "logs/{}".format(self._sender)
        if not os.path.exists(os.path.join(os.getcwd(), 'logs')):
            os.mkdir(os.path.join(os.path.join(os.getcwd(), 'logs')))
        logs_dir = os.path.join(os.getcwd(), 'logs')
        logs_path = os.path.join(logs_dir, '{}-sentmsgs.log'.format(self._sender))
        try:
            file = open(logs_path, 'r')
        except IOError:
            file = open(logs_path, 'w')

        self.write_data(logs_path, msg)

    def write_data(self, file_path, data):
        if os.stat(file_path).st_size == 0:
            with open(file_path, "w") as myfile:
                myfile.write("recipient,id,time,size" + '\n')
       
        with open(file_path, "a") as myfile:
            # time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            time = current_milli_time()
            total_bytes = 0
            if isinstance(data, list):
                for m in data:
                    total_bytes += len(m)
                data_row = f'{self._recipient},{time},{total_bytes}'
                myfile.write(data_row + '\n')

            # [b'worker_ready']
            if len(data) == 1:
                data_row = f'{self._recipient},{time},{len(data[0])}'
                myfile.write(data_row + '\n')

            # # [b'topic', b'status', b'Worker-0002', b'{"task_id": "a1072e6c-2847-4d41-90c7-fce7e0f26502", "createdAt": "2019-07-29 06:10:47", "under_load": false, "msg": "Worker is done training."}']
            # elif len(data) == 4:
            #     topic = decode(data[0])
            #     status = decode(data[1])
            #     sender = decode(data[2])
            #     json_obj = json.loads(decode(data[3]))
            #     task_id = json_obj['task_id']

            #     total_bytes = 0
            #     if isinstance(data, list):
            #         for m in data:
            #             total_bytes += len(m)

            #     data_row = "{},{},{},{}".format(self._recipient, task_id, time, total_bytes)
            #     myfile.write(data_row + '\n')
            # # Catch all: recipient, time, total_bytes
            # else:
            #     total_bytes = 0
            #     for m in data:
            #         total_bytes += len(m)
            #     data_row = "{}, {}, {}".format(self._recipient, time, total_bytes)
            #     myfile.write(data_row + '\n')