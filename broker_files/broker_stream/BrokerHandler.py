from base_stream import MessageHandlers as mh
from utils.constants import *
from utils.Utils import *
from classes.WorkerQueue import WorkerQueue
from classes.Worker import Worker
from classes.Task import Task
import sys
import json
import random
# import panda as pd
import numpy as np
import pickle

from sklearn.model_selection import train_test_split

sys.path.append('..')

NUMBER_OF_TRAINERS = 3

# TODO: Should I separate functions not entirely related to brokerhandler? (Probably)
# Like what i did with the workerhandler
class BrokerHandler(mh.RouterMessageHandler):
    """Handles messages that arrive on the broker streams (backend and frontend)"""
    """Just name the function the same as your msg_type and it will handle it."""

    some_broker_task_queue = []
    workers = WorkerQueue()

    # Place variables here that i plan on reusing like the arrays etc...

    def __init__(self, frontend_stream, backend_stream, stop):
        print("BrokerHandler.__init__()")
        super().__init__(json_load=1)
        self._frontend_stream = frontend_stream
        self._backend_stream = backend_stream
        self._stop = stop


        self.aggregated_data = []

        # TODO: STOP GAP to implement a workflow, change in future
        self.last_response_received = ''
        BrokerHandler.some_broker_task_queue = []
        # TODO: This is only for testing
        BrokerHandler.client = ''

    def plzdiekthxbye(self, *data):
        print("Received plzdiekthxbye")
        """Just calls :meth:`BrokerProcess.stop`."""
        self._stop()

    def test_ping_query(self, *data):
        sender = decode(data[0])
        BrokerHandler.client = sender
        print("Received {} query.".format(BrokerHandler.client))
        self._backend_stream.send_multipart([b'Worker-0000', b'test_ping_task'])
    
    def test_ping_response(self, *data):
        print("Received worker response.")
        self._frontend_stream.send_multipart([encode(BrokerHandler.client), b'Pong'])

    # Start DEBUG
    def extract_query(self, *data):
        sender = decode(data[0])
        self.client = sender

        file = open('test_data/tda.pkl',"rb")
        tda_loaded = pickle.load(file)
        print(tda_loaded.shape)

        # some_test_data = tda_loaded
        
        # self._backend_stream.send_multipart([b'Worker-0000', encode(TRAIN_TASK), zip_and_pickle(tda_loaded)])

        shuffled_split_data = self.shuffle_and_split_aggregated_extracted_data(tda_loaded)
        generated_train_tasks = self.generate_train_tasks(shuffled_split_data)
        
        BrokerHandler.some_broker_task_queue.extend(generated_train_tasks)
        self.send_task_to_worker()
    # End DEBUG
    
    # Start ACTUAL
    # def extract_query(self, *data):
    #     self.last_response_received = EXTRACT_QUERY
    #     self.client = decode(data[0])
    #     query = decode(data[1])
    #     query = json.loads(query)

    #     # new_tasks = self.extract_query_json_and_generate_tasks(query)
    #     new_tasks = self.generate_tasks(EXTRACT_TASK, query)
    #     # For now we assume that only one query at a time, since we are only measuring speeds and overhead
    #     BrokerHandler.some_broker_task_queue.clear()
    #     BrokerHandler.some_broker_task_queue.extend(new_tasks)

    #     self.send_task_to_worker()
    #     # TODO: Probably needs some information on available workers...
    # End ACTUAL

    def extract_response(self, *data):
        self.last_response_received = EXTRACT_RESPONSE
        sender = decode(data[0])
        msg = decode(data[1])
        array = unpickle_and_unzip(data[2])

        self.aggregated_data.append(data[2])
        # print("Data len:{}".format(len(data)))
        print("Extract {} response: {}, shape: {}".format(sender, msg, array.shape))

        self.notify_client(b' World!')
        worker_addr = data[0]
        BrokerHandler.workers.ready(Worker(worker_addr, b'', b''))
        self.send_task_to_worker()

    def train_response(self, *data):
        sender = decode(data[0])
        message = decode(data[1])
        print("{} has finished training.".format(sender))

    def worker_ready(self, *data):
        print("A worker is ready:{}".format(data))
        worker_addr = data[0]
        BrokerHandler.workers.ready(Worker(worker_addr, b'', b''))
        self.send_task_to_worker()

    def classify_query(self, *data):
        sender = decode(data[0])
        self.client = sender

        file = open('test_data/tda.pkl',"rb")
        tda_loaded = pickle.load(file)
        print(tda_loaded.shape)

        # Start DEBUG
        X = tda_loaded[:,:-1]
        y = tda_loaded[:,-1:]
        # print(y)

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=100)
        test_data_arr = np.append(X_test, y_test, axis=1)
        # End DEBUG

        # some_test_data = tda_loaded
        self._backend_stream.send_multipart([b'Worker-0000', encode(CLASSIFY_TASK), zip_and_pickle(test_data_arr)])
        # For testing mainly, read data from test_data
        # send for classification and get array of decision tree predictions

        # input_data_dir = 'test_data/'
        # filename = 'X_test.txt'
        # path = input_data_dir + filename
        # delimiter = " "
        # df_X = pd.read_csv(path, 
        #                 header=None, 
        #                 delimiter=delimiter, 
        #                 dtype=np.float64)

        # print(df_X.values.shape)
        # print(df_X.shape)
        # print(type(df_X.values))

        # input_data_dir = 'test_data/'
        # filename = 'y_test.txt'
        # path = input_data_dir + filename
        # delimiter = " "
        # df_y = pd.read_csv(path, 
        #                 header=None, 
        #                 delimiter=delimiter, 
        #                 dtype=np.int)

        # df_y.head()
        # test_data_arr = np.append(df_X.values, df_y.values, axis=1)
        # test_data_arr
        # print(test_data_arr.shape)

    def classify_response(self, *data):
        self._frontend_stream.send_multipart(b"Client-000", b"Done classifying")
        pass

    # This is a random task assignment, but it is load balanced because of a worker queue
    def send_task_to_worker(self):
        # Loop through tasks, as long as there is a task and there is a worker free, send tasks.
        # else, break and wait for new workers
        if len(BrokerHandler.some_broker_task_queue) > 0:
            while len(BrokerHandler.some_broker_task_queue) > 0:
                if len(BrokerHandler.workers.queue) > 0:
                    # TODO: Make a TASK class which includes a TASK FLAG then just create abstraction that sends the whole thing
                    task = BrokerHandler.some_broker_task_queue.pop()
                    addr = BrokerHandler.workers.next()
                    task.send(addr)
                else:
                    break
        # After extracting, you aggregate and shuffle the data, then split it into the number of trainers you want
        # This will start becoming messy
        # TODO: Put these in a function
        else:
            # TODO: Check if all workers are available (just to know if all arrived)
            if len(BrokerHandler.workers.queue) != NUMBER_OF_TRAINERS:
                return False

            print("No tasks available...")
            # Have to have some flag here that prevents it from going in a  loop,
            # it should have some flow EXTRACT -> TRAIN -> CLASSIFY
            # TODO: Stop gap. in conjunction with the last_response_received flag

            if self.last_response_received == EXTRACT_RESPONSE:
                aggregated_extracted_data = self.aggregate_data(self.aggregated_data)

                shuffled_split_data = self.shuffle_and_split_aggregated_extracted_data(aggregated_extracted_data)
                generated_train_tasks = self.generate_train_tasks(shuffled_split_data)
                
                BrokerHandler.some_broker_task_queue.extend(generated_train_tasks)
                self.send_task_to_worker()
            else:
                return False #?
                    

    def notify_client(self, *data):
        print("Notifying {}".format(self.client))
        message = data[0]
        self._frontend_stream.send_multipart([b'Client-000', b'Hello', message])

    def generate_tasks(self, task_type, json_request=None):
        if task_type == EXTRACT_TASK:
            task_queue = []
            for i in range(1, 13):
                json_request["label"] = i
                t = Task(task_type, self._backend_stream)
                t.add_payload(json.dumps(json_request))
                task_queue.append(t)

            return task_queue

        pass
    def extract_query_json_and_generate_tasks(self, json_request):
        # Hardcoded labels since there are 12 labels possible in the dataset
        # And it is easier to query random values by label in influxDB
        print("-> extract_query_parser")
        task_queue = []
        for i in range(1, 13):
            json_request["label"] = i

            task_queue.append(json.dumps(json_request))

        return task_queue

    def shuffle_and_split_aggregated_extracted_data(self, aggregated_extracted_data):
        return list(split(aggregated_extracted_data, NUMBER_OF_TRAINERS))

    def generate_train_tasks(self, split_np_arr_extracted_data):
        task_queue = []
        for data_split in split_np_arr_extracted_data:
            q = []
            dict_req = {}
            dict_req['model'] = 'RF'
            dict_req['queried_time'] = current_seconds_time()

            t = Task(TRAIN_TASK, self._backend_stream)
            t.add_payload(json.dumps(dict_req))
            t.add_payload(data_split)
            task_queue.append(t)

        return task_queue

    def aggregate_data(self, aggregated_pickles):
        output = []
        for pickled in aggregated_pickles:
            unpickld = unpickle_and_unzip(pickled)
            temp = unpickld.tolist()
            output.extend(temp)
        np_output = np.asarray(output)
        print("Aggregated data with shape: {}".format(np_output.shape))
        return np_output

    # TODO: Move send in Task.py to here
    def send_task(self, address):
        pass
