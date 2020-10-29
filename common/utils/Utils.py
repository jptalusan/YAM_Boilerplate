import time
import pickle
import blosc
import json
import datetime

decode = lambda x: x.decode('utf-8')
encode = lambda x: x.encode('ascii')
# int: time in milliseconds
time_print = lambda type: datetime.now().strftime("%d/%m/%Y %H:%M:%S") if type == 'str' else int(round(time.time() * 1000))
current_seconds_time = lambda: int(round(time.time()))
current_milli_time = lambda: int(round(time.time() * 1000))

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

def print_log(log, level=0, log_type="D"):
    d = datetime.datetime.utcnow().strftime("%d %b %Y %H:%M:%S.%f")[:-3]
    print(f"{level}:{log_type} {d} # {log}")