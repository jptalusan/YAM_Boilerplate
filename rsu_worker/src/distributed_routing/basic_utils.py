import datetime
import time
import random
import string
import csv
import os
from common.conf import GLOBAL_VARS

# int: time in milliseconds
time_print = lambda type: datetime.now().strftime("%d/%m/%Y %H:%M:%S") if type == 'str' else int(round(time.time() * 1000))

decode = lambda x: x.decode('utf-8')
encode = lambda x: x.encode('ascii')

def add_destination(topic, dest):
    if topic[-1] == '/':
        # topic = topic[:-1]
        topic = topic + dest
    else:
        topic = topic + "/" + dest

    return topic

def randomString(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))

def print_log(log, level=0, log_type="D"):
    d = datetime.datetime.utcnow().strftime("%d %b %Y %H:%M:%S.%f")[:-3]
    print("{}:{} {} # {}".format(level, log_type, d, log))

def f7(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]

def write_log(path, dict_entry):
    keys = dict_entry.keys()
    with open(path, 'a') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, delimiter = ';')
        if not os.path.exists(path) or os.stat(path).st_size == 0:
            dict_writer.writeheader()
        dict_writer.writerow(dict_entry)

def get_worker_from_topic(topic):
    grid = topic.split("/")[-1]
    return GLOBAL_VARS.WORKER[grid]