import sys
import time
import os
import json

# Testing
import uuid
from datetime import datetime
import random

import time
from string import ascii_uppercase as uppercase
# End testing

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
from sklearn.preprocessing import StandardScaler
import numpy as np
from joblib import dump, load

sys.path.append('..')
from utils.Utils import *
from utils.constants import *
from base_stream import MessageHandlers as mh
from feature_extraction import database_specific


class WorkerHandler(mh.DealerMessageHandler):
    """Handels messages arrvinge at the PongProc’s REP stream."""
    # Debug
    # Class Variables
    received_counter = 0
    some_task_queue = []

    def __init__(self, identity, backend_stream, publish_stream, stop, mani_handler, extract_handler, train_handler):
        # json_load is the element index of the msg_type
        super().__init__(json_load=0)
        # Instance variables
        self._backend_stream = backend_stream
        self._publish_stream = publish_stream
        self._stop = stop
        self._mani_handler = mani_handler
        self._extract_handler = extract_handler
        self._train_handler = train_handler
        self._identity = identity

        WorkerHandler.received_counter = 0
        WorkerHandler.some_task_queue = []

        print("WorkerHandler:__init__")

        self._publish_stream.send_multipart([b"topic", b"Message"])
        self._publish_stream.send(b'topic helloworld')
        self._publish_stream.send(b' helloworld2')

        self._backend_stream.send_multipart([encode(WORKER_READY)])

    def plzdiekthxbye(self, *data):
        print("Stopping:WorkerProcess")
        """Just calls :meth:`WorkerProcess.stop`."""
        self._stop()

    def test_ping_task(self, *data):
        print("Received task from broker.")
        self._backend_stream.send_multipart([b"test_ping_response"])

    def extract_task(self, *data):
        sender = decode(data[0])
        data_arr = decode(data[1])
        print("Received {}:{} from Broker".format(EXTRACT_TASK, data_arr))
        
        json_req = json.loads(data_arr)
        extracted_data = self._extract_handler.extract_features(json_req)
        print("Extracted: {}".format(extracted_data.shape))

        self._backend_stream.send_multipart([encode(EXTRACT_RESPONSE), b'Done extracting...', zip_and_pickle(extracted_data)])

    def train_task(self, *data):
        sender = decode(data[0])
        task_id = decode(data[1])
        print("Received a {} with ID: {}".format(sender, task_id))

        # payload_count = int(decode(data[2]))
        # # Useful but probably better to hard code right now
        # json_query = None
        # narr = None
        # print("Paylod count: {}".format(payload_count))
        # for i in range(payload_count):
        #     load_type = decode(data[3 + (i * 2)])
        #     if load_type == 'String' or load_type == 'Bytes':
        #         load = decode(data[3 + (i * 2) + 1])
        #         print("Type: {}, load: {}".format(load_type, load))
        #         json_query = load
        #     elif load_type == 'ZippedPickleNdArray':
        #         load = unpickle_and_unzip(data[3 + (i * 2) + 1])
        #         print("Type: {}, load shape: {}".format(load_type, load.shape))
        #         narr = load


        # clf = self._train_handler.train_model(narr)
        # self._train_handler.save_model(self._identity)
        time.sleep(3)
        for reqnum in range(10):
            string = "%s-%05d" % (uppercase[random.randint(0,5)], random.randint(0,100000))
            print("Publishing: {}".format(string))
    
            # You can send it in the format described in above [topic-data]
            # socket.send(string.encode('utf-8'))
    
            # As part of a multipart
            self._publish_stream.send_multipart([string.encode('utf-8'), b"TEST"])
    
            # Or with the topic listed as the first part of the multipart
            # socket.send_multipart([b"A", b"YES"])
    
            # You can also add payloads
            # socket.send_multipart([b"status", b"something", b"YES"])
            time.sleep(0.1)
        
        # message_id, now = str(uuid.uuid4()), datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # humidity = random.randrange(20, 40)
        # temperature_in_celsius = random.randrange(32, 41)
        # payload = json.dumps({"message_id": message_id, "humidity":humidity, "temperature_in_celsius":temperature_in_celsius, "createdAt":now})
        # message = "{topic} #{id} at {timestamp} --> {payload}  ".format(topic="topic", id="id", timestamp="timestamp", payload=payload)
        # print("Publishing: {}".format(message))
        # self._publish_stream.send(encode(message))
        # self._publish_stream.send_multipart([b"topic", b"Message"])
        # self._publish_stream.send(b'topic helloworld')
        # self._publish_stream.send(b' helloworld2')
        # print("finished publishing.")

    def classify_task(self, *data):
        sender = decode(data[0])
        test_array = unpickle_and_unzip(data[1])
        print("Received classify task from broker with shape:{}".format(test_array.shape))
        
        X = test_array[:,:-1]
        y = test_array[:,-1:]
        print(X.shape, y.shape)

        model_name = "models/{}-RF-model.joblib".format(self._identity)
        loaded_clf = self._train_handler.load_model(model_name)
        y_pred = loaded_clf.predict(X)

        acc = accuracy_score(y, y_pred)
        print("Accuracy: {}".format(acc))
        print(confusion_matrix(y, y_pred))
        print(classification_report(y, y_pred))


# TODO: Create a handler for each type of message for the broker
class ManiHandler(object):

    def make_mani(self, counter, *data_arr):
        """Creates and returns a pong message."""
        print("Make_mani got {}".format(data_arr))
        WorkerHandler.received_counter += 1
        return WorkerHandler.received_counter

class ExtractHandler(object):
    def extract_features(self, json_req):
        nout = self.get_data_from_DB(json_req['label'], json_req['database'], json_req['rows'])
        print("Done extracting...")

        if nout.size != 0:
            label_col = np.full((nout.shape[0], 1), int(json_req['label']))
            nout = np.append(nout, label_col, axis=1)

        return nout

    def get_data_from_DB(self, label, database, limit):
        INFLUX_HOST = os.environ['INFLUX_HOST']
        INFLUX_PORT = os.environ['INFLUX_PORT']
        INFLUX_DB = os.environ['INFLUX_DB']
        dbs = database_specific.Database_Specific(INFLUX_HOST, INFLUX_PORT, INFLUX_DB)
        if database == 'both':
            nout = dbs.get_rows_with_label_both(int(label), limit=limit)
            # print("Nout {}:{}".format(database, nout.shape))
            return nout
        elif database == 'acc' or database == 'gyro':
            nout = dbs.get_rows_with_label(int(label), database, limit=limit)
            return nout
        else:
            return None

class TrainHandler(object):
    def train_model(self, extracted_data):
        X = extracted_data[:,:-1]
        y = extracted_data[:,-1:]
        print("Training data received: {}".format(extracted_data.shape))
        print(X.shape, y.shape)
        # print(y)
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=100)

        clf = RandomForestClassifier(n_estimators=20, random_state=100)  
        self.clf = clf
        self.clf.fit(X_train, y_train.ravel())

        y_pred = clf.predict(X_test)
        acc = accuracy_score(y_test, y_pred)
        print(acc)
        print(confusion_matrix(y_test, y_pred))
        print(classification_report(y_test, y_pred))
        return self.clf

    def save_model(self, identity):
        # TODO: Save to directory 'models'
        model_name = "models/{}-RF-model.joblib".format(identity)
        dump(self.clf, model_name)
    
    def load_model(self, model_name):
        return load(model_name)