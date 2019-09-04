from base_stream import MessageHandlers as mh
from utils.constants import *
from utils.Utils import *
from classes.WorkerQueue import WorkerQueue
from classes.Worker import Worker
from classes.Task import Task
from classes.Query import Query

from numpy import genfromtxt
import sys
import json
import random
# import panda as pd
import numpy as np
import pickle

from sklearn.model_selection import train_test_split

# Routing imports

sys.path.append('..')

# TODO: Should I separate functions not entirely related to brokerhandler? (Probably)
# Like what i did with the workerhandler
class BrokerHandler(mh.RouterMessageHandler):
    """Handles messages that arrive on the broker streams (backend and frontend)"""
    """Just name the function the same as your msg_type and it will handle it."""

    some_broker_task_queue = []
    workers = WorkerQueue()

    # Place variables here that i plan on reusing like the arrays etc...

    '''
        SEC001: Main Functions
    '''
    def __init__(self, identity, base_process):
        print("BrokerHandler.__init__()")
        super().__init__(json_load=1)
        self._base_process    = base_process
        self._frontend_stream = base_process.frontend_stream
        self._backend_stream  = base_process.backend_stream
        self._identity        = identity
        self.aggregated_data  = []

        # TODO: STOP GAP to implement a workflow, change in future
        self.last_response_received = ''
        BrokerHandler.some_broker_task_queue = []
        # TODO: This is only for testing
        BrokerHandler.client = ''

        self.alive_workers = []
        return

    def purge_done_queries_in_queue(self):
        print("->purge_done_queries_in_queue()")
        # Check if a query has all its tasks done
        # If so, remove it from the broker queue
        done_queue = []
        for q in BrokerHandler.some_broker_task_queue:
            for t in q._tasks:
                print("{}:{}".format(t._id, t._status))
            if q.are_tasks_done():
                done_queue.append(q)
                print("Queue:{} is done.".format(q._id))

        for dq in done_queue:
            print("Removing queue:{}".format(dq._id))
            BrokerHandler.some_broker_task_queue.remove(dq)

        return

    # This is a random task assignment, 
    # but it is load balanced because of a worker queue
    def send_task_to_worker(self):
        if len(BrokerHandler.workers.queue) == 0:
            return # Return code probably

        if len(BrokerHandler.some_broker_task_queue) == 0:
            return # No tasks remaining

        print("Trying to send task to worker.")
        # TODO: Probably not the best way to do it.
        query = BrokerHandler.some_broker_task_queue[0]
        print(query)
        
        for task in query._tasks:
            if len(BrokerHandler.workers.queue) == 0:
                break

            if task._status == 'None':
                addr = BrokerHandler.workers.next()
                # TODO: Add some flag to the task that it is sent already
                task.update_status(1) #1 == sent
                task.send(addr)

        return

    '''
        SEC002: General Handler Functions
    '''
    def worker_ready(self, *data):
        print("A worker is ready:{}".format(data))
        worker_addr = data[0]
        BrokerHandler.workers.ready(Worker(worker_addr, b'', b''))
        self.send_task_to_worker()
        return

    def plzdiekthxbye(self, *data):
        print("Received plzdiekthxbye")
        """Just calls :meth:`BrokerProcess.stop`."""
        self._base_process.stop()
        return

    def status(self, *data):
        print("Subs received messsage:{}".format(data))
        topic = decode(data[0])
        sender = decode(data[1])
        payload = json.loads(decode(data[2]))
        task_id = payload['task_id']
        print(topic, sender, task_id)

        for q in BrokerHandler.some_broker_task_queue:
            for task in q._tasks:
                if task._id == task_id:
                    print("Found a match: {}".format(task._id))
                    task.update_status(2) #2 == Done

        # Update the task queue that the task with task_id is done..

        self.purge_done_queries_in_queue()
        self.worker_ready(data[1])
        return

    def heartbeat(self, *data):
        topic = decode(data[0])
        sender = decode(data[1])
        print("Worker: {} is still alive.".format(sender))
        self.alive_workers.append(sender)
        return

    def purge(self, workers):
        # If not in alive_workers queue, remove from workers queue.
        return

    def error(self, *data):
        print("Error:{}".format(data))
        print("Recived error, worker should just be ready again.")
        sender = decode(data[1])
        self.worker_ready(sender)
        return

    '''
        SEC003: Test Handler Functions
            Test functions for checking if the docker/middleware is working 
            in terms of connectivity.
    '''
    def test_ping_query(self, *data):
        sender = decode(data[0])
        BrokerHandler.client = sender
        print("Received {} query.".format(BrokerHandler.client))
        self._backend_stream.send_multipart([b'Worker-0000', b'test_ping_task'])
        return
    
    def test_ping_response(self, *data):
        print("Received worker response.")
        self._frontend_stream.send_multipart([encode(BrokerHandler.client), b'Pong'])
        return

    '''
        SEC004: Training (Machine Learning) Handler Functions
            Handle the training queries and responses from the clients and 
            workers respectively.
    '''
    def train_query(self, *data):
        print("->train_query()")
        sender = decode(data[0])
        json_str = decode(data[1])

        q = Query(sender, json_str)
        print("Received query with id: {}".format(q._id))
        print("Json: {}".format(q._json_str))

        # For this iteration of the code, just get the data from broker
        # Split it up and add it to payloads of tasks
        # Reading CSV to numpy array and stacking them (feat + label)

        # Generate "Collect" Tasks
        X_train = genfromtxt('data/Train/X_train.txt', delimiter=' ')
        print(X_train.shape)

        y_train = genfromtxt('data/Train/y_train.txt', delimiter=' ')
        y_train = y_train.reshape(-1, 1)
        print(y_train.shape)

        train = np.append(X_train, y_train, axis=1)
        print(train.shape)
        
        data_arr = split(train, NUMBER_OF_TRAINERS)

        # Generate "Process" Tasks
        new_tasks = self.generate_train_tasks(data_arr)
        [q.add_task_id(task._id) for task in new_tasks]
        # print(q)

        [q.add_task(task) for task in new_tasks]

        # Generate "Aggregate" Tasks

        # Add Query task to "Task" Queue
        BrokerHandler.some_broker_task_queue.append(q)
        self.send_task_to_worker()
        return

    def train_response(self, *data):
        sender = decode(data[0])
        message = decode(data[1])
        self.worker_ready(sender)
        print("{} has finished training.".format(sender))
        return

    def extract_train_query(self, *data):
        sender = decode(data[0])
        json_str = decode(data[1])

        q = Query(sender, json_str)
        json_data = json.loads(json_str)

        tasks = []
        for user in json_data['users']:
            task = self.generate_train_tasks_users(user, json_data['database'])
            tasks.append(task)

        [q.add_task_id(task._id) for task in tasks]
        [q.add_task(task) for task in tasks]
        BrokerHandler.some_broker_task_queue.append(q)
        self.send_task_to_worker()
        return

    '''
        SEC005: Classification (Machine Learning) Handler Functions
    '''
    def classify_query(self, *data):
        sender = decode(data[0])
        self.client = sender

        file = open('test_data/tda.pkl',"rb")
        tda_loaded = pickle.load(file)
        print(tda_loaded.shape)

        # Start DEBUG
        X = tda_loaded[:,:-1]
        y = tda_loaded[:,-1:]

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=100)
        test_data_arr = np.append(X_test, y_test, axis=1)

        self._backend_stream.send_multipart([b'Worker-0000', encode(CLASSIFY_TASK), zip_and_pickle(test_data_arr)])
        return

    def classify_response(self, *data):
        self._frontend_stream.send_multipart(b"Client-000", b"Done classifying")
        return

    def notify_client(self, *data):
        print("Notifying {}".format(self.client))
        message = data[0]
        self._frontend_stream.send_multipart([b'Client-000', b'Hello', message])
        return

    def shuffle_and_split_aggregated_extracted_data(self, aggregated_extracted_data):
        return list(split(aggregated_extracted_data, NUMBER_OF_TRAINERS))

    def generate_train_tasks_users(self, users, db):
        dict_req = {}
        dict_req['model'] = 'RF'
        dict_req['user'] = users
        dict_req['database'] = db
        dict_req['queried_time'] = current_seconds_time()

        t = Task(TRAIN_TASK, self._backend_stream)
        t.add_payload(json.dumps(dict_req))
        return t

    def generate_train_tasks(self, split_np_arr_extracted_data):
        task_queue = []
        for data_split in split_np_arr_extracted_data:
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

    '''
        SEC006: Routing Handler Functions
    '''
    def routing_query(self, *data):
        print('routing_query()')

        sender = decode(data[0])
        json_str = decode(data[1])

        q = Query(sender, json_str)
        json_data = json.loads(json_str)

        tasks = []
        for user in json_data['users']:
            task = self.generate_train_tasks_users(user, json_data['database'])
            tasks.append(task)

        [q.add_task_id(task._id) for task in tasks]
        [q.add_task(task) for task in tasks]
        BrokerHandler.some_broker_task_queue.append(q)
        self.send_task_to_worker()

        return

    # TODO: Move send in Task.py to here
    def send_task(self, address):
        return

