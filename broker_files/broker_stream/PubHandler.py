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

sys.path.append('..')

NUMBER_OF_TRAINERS = 3

# TODO: Should I separate functions not entirely related to brokerhandler? (Probably)
# Like what i did with the workerhandler

class PubHandler(mh.PubMessageHandler):
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
        return
        
    def status(self, *data):
        print("Subs sent:{}".format(data))
        return

    def error(self, *data):
        print("Recived error, worker should just be ready again.")
        sender = decode(data[0])
        self.worker_ready(sender)
        return

