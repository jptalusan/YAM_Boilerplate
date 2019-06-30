import sys
import time
import os
import json

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

    def __init__(self, identity, backend_stream, stop, mani_handler, extract_handler, train_handler):
        # json_load is the element index of the msg_type
        super().__init__(json_load=0)
        # Instance variables
        self._backend_stream = backend_stream
        self._stop = stop
        self._mani_handler = mani_handler
        self._extract_handler = extract_handler
        self._train_handler = train_handler
        self._identity = identity

        WorkerHandler.received_counter = 0
        WorkerHandler.some_task_queue = []

        print("WorkerHandler:__init__")
        self._backend_stream.send_multipart([encode(WORKER_READY)])

    def plzdiekthxbye(self, *data):
        print("Stopping:WorkerProcess")
        """Just calls :meth:`WorkerProcess.stop`."""
        self._stop()

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
        dict_req = decode(data[1])
        unpickled_data = unpickle_and_unzip(data[2])
        print("Received train_task from: {}, {}, {}".format(sender, dict_req, unpickled_data.shape))

        clf = self._train_handler.train_model(unpickled_data)
        self._train_handler.save_model(self._identity)

        self._backend_stream.send_multipart([encode(TRAIN_RESPONSE), b'Done training and saved a model...'])

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