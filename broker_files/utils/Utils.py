import time
import pickle
import blosc
import numpy as np

decode = lambda x: x.decode('utf-8')
encode = lambda x: x.encode('ascii')
current_seconds_time = lambda: int(round(time.time()))

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
    
# TODO: For some reason, the shuffle is not working? I thought it was inplace.
# This might cause too much memory use though. (most probably)
def split(a, n):
    # np.random.shuffle(a)
    a_ = a[np.random.permutation(a.shape[0])]
    k, m = divmod(len(a_), n)
    return (a_[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))