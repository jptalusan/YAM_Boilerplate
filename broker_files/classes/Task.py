from utils.Utils import *
import numpy as np
import uuid

class Task(object):
    
    def __init__(self, task_type, socket):
        self.payload = []
        self._id = str(uuid.uuid4())
        self._task_type = task_type
        self._socket = socket
        self._payload_count = 0

    # TODO: This send method should probably in a different place
    # Payload:
    '''
    [0] = Address
    # This is consumed by the Handlers
    [1] = Task type
    [2] = Task ID
    [3] = Payload Count
    # Determined by payload count
    [4] = Payload type
    [5] = Payload
    [6] = Payload type
    [7] = Payload ....
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
        payload_to_send.append(encode(str(self._payload_count)))
        # Extend because payload is also a list. (i dont know why, probably shouldnt be one)
        payload_to_send.extend(self.payload)
        self._socket.send_multipart(payload_to_send)

    def add_payload(self, data):
        self._payload_count += 1
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
        else:
            self._payload_count -= 1
            return

    def deconstruct_payload(self):
        pass

