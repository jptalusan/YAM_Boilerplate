import sys
import time
import os

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import numpy as np

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

    def __init__(self, backend_stream, stop, mani_handler, extract_handler, train_handler):
        # json_load is the element index of the msg_type
        super().__init__(json_load=0)
        # Instance variables
        self._backend_stream = backend_stream
        self._stop = stop
        self._mani_handler = mani_handler
        self._extract_handler = extract_handler
        self._train_handler = train_handler

        WorkerHandler.received_counter = 0
        WorkerHandler.some_task_queue = []

        print("WorkerHandler:__init__")
        self._backend_stream.send_multipart([encode(WORKER_READY)])

    def plzdiekthxbye(self, *data):
        print("Stopping:WorkerProcess")
        """Just calls :meth:`WorkerProcess.stop`."""
        self._stop()

    def extract_task(self, *data):
        print(data)
        print(type(data))
        print(type(data[1]))
        sender = decode(data[0])
        data_arr = decode(data[1])
        print("Received {}:{} from Broker".format(EXTRACT_TASK, data_arr))
        extracted_data = self._extract_handler.extract_features(data[1:])
        print("Extracted: {}".format(extracted_data.shape))

        clf = self._train_handler.train_model(extracted_data)
        print(clf)

        self._backend_stream.send_multipart([encode(EXTRACT_RESPONSE), b'Done extracting...'])

# TODO: Create a handler for each type of message for the broker
class ManiHandler(object):

    def make_mani(self, counter, *data_arr):
        """Creates and returns a pong message."""
        print("Make_mani got {}".format(data_arr))
        WorkerHandler.received_counter += 1
        return WorkerHandler.received_counter

class ExtractHandler(object):
    def extract_features(self, *data):
        d = data[0]
        print("Doing some extracting on {}.".format(d[0]))

        nout = self.get_data_from_DB('1', 'acc', 128)
        print("Done extracting...")

        if nout.size != 0:
            label_col = np.full((nout.shape[0], 1), int('1'))
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
        print(y)
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=100)
        clf = RandomForestClassifier(n_estimators=20, random_state=100)  
        clf.fit(X_train, y_train.ravel())
        y_pred = clf.predict(X_test)
        # acc = accuracy_score(y_test, y_pred)
        return clf
