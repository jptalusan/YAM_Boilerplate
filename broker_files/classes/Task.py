from utils.Utils import *
import numpy as np

class Task(object):
    def __init__(self, task_type, socket):
        self.payload = []
        self._task_type = task_type
        self._socket = socket

    # TODO: This send method should probably in a different place
    def send(self, address):
        payload_to_send = []
        if isinstance(address, bytes):
            payload_to_send.append(address)
        else:
            payload_to_send.append(encode(address))
        print("Task.send: ", self._task_type)
        payload_to_send.append(encode(self._task_type))

        # Extend because payload is also a list. (i dont know why, probably shouldnt be one)
        payload_to_send.extend(self.payload)
        # print(payload_to_send)
        # for payload in payload_to_send:
        #     print(payload)
        # print(payload_to_send, len(payload_to_send))
        self._socket.send_multipart(payload_to_send)

    def add_payload(self, data):
        print("Add_payload of type:{}".format(type(data)))
        if isinstance(data, np.ndarray):
            self.payload.append(zip_and_pickle(data))
        elif isinstance(data, bytes):
            self.payload.append(data)
        elif isinstance(data, str):
            self.payload.append(encode(data))
        # print("Payload added: {}".format(self.payload[-1]))
