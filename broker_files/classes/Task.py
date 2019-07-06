from utils.Utils import *
import numpy as np
import uuid

class Task(object):
    def __init__(self, task_type, socket):
        self.payload = []
        self._id = str(uuid.uuid4())
        self._task_type = task_type
        self._socket = socket

    # TODO: This send method should probably in a different place
    # Payload:
    '''
    [0] = Address
    [1] = Task type
    [2] = Task ID
    [3] = Payload type
    [4] = Payload
    '''
    def send(self, address):
        payload_to_send = []
        if isinstance(address, bytes):
            payload_to_send.append(address)
        else:
            payload_to_send.append(encode(address))
        print("Task.send: ", self._task_type)
        payload_to_send.append(encode(self._task_type))
        payload_to_send.append(encode(self._id))
        # Extend because payload is also a list. (i dont know why, probably shouldnt be one)
        payload_to_send.extend(self.payload)
        self._socket.send_multipart(payload_to_send)

    def add_payload(self, data):
        if __debug__ == 0:
            print("Add_payload of type:{}".format(type(data)))
        if isinstance(data, np.ndarray):
            self.payload.append(b"ZippedPickleNdArray")
            self.payload.append(zip_and_pickle(data))
        elif isinstance(data, bytes):
            self.payload.append(b"Bytes")
            self.payload.append(data)
        elif isinstance(data, str):
            self.payload.append(b"String")
            self.payload.append(encode(data))

    def deconstruct_payload(self):
        pass