import time
import blosc
import pickle
import numpy as np

decode = lambda x: x.decode('utf-8')
# encode = lambda x: x.encode('ascii')
current_seconds_time = lambda: int(round(time.time()))

def encode(raw):
    if isinstance(raw, list):
        return zip_and_pickle(raw, protocol=2)
    elif isinstance(raw, str):
        return raw.encode('ascii')
    elif isinstance(raw, np.ndarray):
        return zip_and_pickle(raw)
    return None

def write_data_to_file(data, filename):
    file = open(filename,"a")
    file.write(data)
    file.close()

def zip_and_pickle(obj, flags=0, protocol=-1):
    """pickle an object, and zip the pickle before sending it"""
    p = pickle.dumps(obj, protocol)
    z = blosc.compress(p, typesize=8)
    return z

def unpickle_and_unzip(pickled):
    unzipped = blosc.decompress(pickled)
    unpickld = pickle.loads(unzipped)
    return unpickld
    