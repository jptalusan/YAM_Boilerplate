import sys
import time
import os
import json

# Testing
import uuid
from datetime import datetime
import random

import threading

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
from feature_extraction import database_specific, feature_extraction_separate, feature_extraction

class WorkerHandler(mh.DealerMessageHandler):
    """Handels messages arrvinge at the PongProc’s REP stream."""
    # Debug
    # Class Variables
    received_counter = 0
    some_task_queue = []

    def send_heartbeat(self):
        # Add payload to see if something is processing
        # threading.Timer(2.0, self.send_heartbeat).start()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        payload = json.dumps({"sentAt":now, "under_load":self._under_load})
        self._publish_stream.send_multipart([b"topic", 
                                             b"heartbeat", 
                                             encode(self._identity), 
                                             encode(payload)])
        
    def __init__(self, identity, backend_stream, publish_stream, stop, extract_handler, train_handler):
        # json_load is the element index of the msg_type
        super().__init__(json_load=0)
        # Instance variables
        self._backend_stream = backend_stream
        self._publish_stream = publish_stream
        self._stop = stop
        self._extract_handler = extract_handler
        self._train_handler = train_handler
        self._identity = identity
        print("Worker Identity: {}".format(self._identity))
        self._under_load = False

        WorkerHandler.received_counter = 0
        WorkerHandler.some_task_queue = []

        print("WorkerHandler:__init__")
        # self.send_heartbeat()

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
        self._under_load = True

        print("Received train task:{}".format(data))

        sender = decode(data[0])
        task_id = decode(data[1])
        payload_count = int(decode(data[2]))
        print("Paylod count: {}".format(payload_count))
        for i in range(payload_count):
            load_type = decode(data[3 + (i * 2)])
            if load_type == 'String' or load_type == 'Bytes':
                load = decode(data[3 + (i * 2) + 1])
                print("Type: {}, load: {}".format(load_type, load))
                json_query = json.loads(load) #Assumming its always json?
            elif load_type == 'ZippedPickleNdArray':
                load = unpickle_and_unzip(data[3 + (i * 2) + 1])
                print("Type: {}, load shape: {}".format(load_type, load.shape))
                narr = load

        user = json_query['user']
        model = json_query['model']
        database = json_query['database']
        query_time = json_query['queried_time']
        print(user, model, query_time)
        print(os.getcwd())
        # TODO: if multiple users are indicated, loop through all and extract and append and then train
        HAPT_dir = 'data/HAPT'
        if database == 'both':
            acc_array = np.load('{}/{}-user{}.pkl.npy'.format(HAPT_dir, 'acc', user))
            gyr_array = np.load('{}/{}-user{}.pkl.npy'.format(HAPT_dir, 'gyro', user))
            eh = ExtractHandler()
            extracted_np_array = eh.extract_both_features(acc_array, gyr_array, labeled=True)
            clf = self._train_handler.train_model(extracted_np_array)
            self._train_handler.save_model(self._identity)
            print(clf)

        else:
            np_array = np.load('{}/{}-user{}.pkl.npy'.format(HAPT_dir, database, user))
            eh = ExtractHandler()
            extracted_np_array = eh.extract_features(np_array, database, labeled=True)
            clf = self._train_handler.train_model(extracted_np_array)
            self._train_handler.save_model(self._identity)
            print(clf)

        self._under_load = False
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        msg = "Worker is done training."
        payload = json.dumps({"task_id": task_id, 
                              "createdAt":now,
                              "under_load":self._under_load,
                              "msg":msg})

        self._publish_stream.send_multipart([b"topic", 
                                             b"status", 
                                             encode(self._identity), 
                                             encode(payload)])
        print("finished publishing.")

# Working, but trying something
    # def train_task(self, *data):
    #     self._under_load = True
    #     self.send_heartbeat() #Probably needs delay

    #     sender = decode(data[0])
    #     task_id = decode(data[1])
    #     print("Received a {} with ID: {}".format(sender, task_id))

    #     payload_count = int(decode(data[2]))
    #     # Useful but probably better to hard code right now
    #     json_query = None
    #     narr = None
    #     print("Paylod count: {}".format(payload_count))
    #     for i in range(payload_count):
    #         load_type = decode(data[3 + (i * 2)])
    #         if load_type == 'String' or load_type == 'Bytes':
    #             load = decode(data[3 + (i * 2) + 1])
    #             print("Type: {}, load: {}".format(load_type, load))
    #             json_query = load
    #         elif load_type == 'ZippedPickleNdArray':
    #             load = unpickle_and_unzip(data[3 + (i * 2) + 1])
    #             print("Type: {}, load shape: {}".format(load_type, load.shape))
    #             narr = load

    #     clf = self._train_handler.train_model(narr)
    #     self._train_handler.save_model(self._identity)

    #     self._under_load = False
    #     now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    #     msg = "Worker is done training."
    #     payload = json.dumps({"task_id": task_id, 
    #                           "createdAt":now,
    #                           "under_load":self._under_load,
    #                           "msg":msg})

    #     self._publish_stream.send_multipart([b"topic", 
    #                                          b"status", 
    #                                          encode(self._identity), 
    #                                          encode(payload)])
    #     print("finished publishing.")

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


class ExtractHandler(object):
    def __init__(self):
        self.window = 128
        self.slide = 64
        self.fs = 50.0

    # def extract_features(self, json_req):
    #     nout = self.get_data_from_DB(json_req['label'], json_req['database'], json_req['rows'])
    #     print("Done extracting...")

    #     if nout.size != 0:
    #         label_col = np.full((nout.shape[0], 1), int(json_req['label']))
    #         nout = np.append(nout, label_col, axis=1)

    #     return nout

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

    def extract_features(self, np_arr, database, labeled=False):
        if database == 'acc':
            n_features = feature_extraction_separate.compute_all_Acc_features(
                    np_arr, self.window, self.slide, self.fs)
            pass
        elif database == 'gyro':
            n_features = feature_extraction_separate.compute_all_Gyr_features(
                    np_arr, self.window, self.slide, self.fs)
            pass

        if n_features is not None:
            if labeled:
                labels = feature_extraction_separate.compute_labels(np_arr, self.window, self.slide)
                labels = labels.reshape((labels.shape[0], 1))
                n_features = np.hstack((n_features, labels))
            
            return n_features
        else:
            return None
        
    def extract_both_features(self, acc, gyro, labeled=False):
        all_feat_np = feature_extraction.compute_all_features(acc, gyro, self.window, self.slide, self.fs)
        # labels = feature_extraction
        print("Extracted: {}".format(all_feat_np.shape))
        if all_feat_np is not None:
            if labeled:
                labels = feature_extraction_separate.compute_labels(acc, self.window, self.slide)
                labels = labels.reshape((labels.shape[0], 1))
                all_feat_np = np.hstack((all_feat_np, labels))
                print(all_feat_np.shape)
            return all_feat_np
        else:
            return None

class TrainHandler(object):
    def train_model(self, extracted_data):
        X = extracted_data[:,:-1]
        y = extracted_data[:,-1:]
        print("Training data received: {}".format(extracted_data.shape))
        print(X.shape, y.shape)
        # print(y)
        X_train, _, y_train, _ = train_test_split(X, y, test_size=0.2, random_state=100)

        clf = RandomForestClassifier(n_estimators=20, random_state=100)  
        self.clf = clf
        self.clf.fit(X_train, y_train.ravel())

        # y_pred = clf.predict(X_test)
        # acc = accuracy_score(y_test, y_pred)
        # print(acc)
        # print(confusion_matrix(y_test, y_pred))
        # print(classification_report(y_test, y_pred))
        return self.clf

    def save_model(self, identity):
        # TODO: Save to directory 'models'
        model_name = "models/{}-RF-model.joblib".format(identity)
        dump(self.clf, model_name)
    
    def load_model(self, model_name):
        return load(model_name)