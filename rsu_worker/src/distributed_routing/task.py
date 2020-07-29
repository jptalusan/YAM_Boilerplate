import datetime
import time
import json
from collections import namedtuple
from json import JSONEncoder

def customTaskDecoder(taskDict):
    return namedtuple('X', taskDict.keys())(*taskDict.values())
    
current_milli_time = lambda: int(round(time.time() * 1000))

# A query handles a group of tasks
# Maybe only applicable in centralized, else need to query the databases i said below.
class Query(object):
    def __init__(self):
        pass

# Probably need a database for this
class Task(object):
    
    inquiry_time = None
    assignment_time = None # Issue time
    execution_start_time = None
    execution_end_time = None
    actual_grid_assignment = None
    manhattan_distance = None
    state = None
    
    def __init__(self, json_data):
        self.inquiry_time = json_data['inquiry_time']
        self.t_id = json_data['_id']
        self.node = json_data['node']
        self.gridA = json_data['gridA']
        self.gridB = json_data['gridB']
        self.time_window = json_data['time_window']
        self.state = json_data['state']
        self.next_node = json_data['next_node']
        self.next_rsu = json_data['next_rsu']
        self.rsu_assigned_to = json_data['rsu_assigned_to']
        self.route = json_data['route']
        
        self.parsed_id = self.t_id[0:-6]
        self.step = self.t_id[-6:-3]
        self.steps = self.t_id[-3:]

        self.retry_count = 0

    def get_tuple(self):
        pass
    
    # How long it lives in the queue before processing
    def assign_task(self, actual_grid_assignment):
        self.assignment_time = current_milli_time()
        self.actual_grid_assignment = actual_grid_assignment
        pass
    
    # Does this really belong here?
    # I think this is ok, but should be called by the RSU
    def start_execution(self):
        self.execution_start_time = current_milli_time()
        
        r1 = get_rsu_by_grid_id(rsu_arr, self.actual_grid_assignment)
        r2 = get_rsu_by_grid_id(rsu_arr, self.start_point)
        self.manhattan_distance = manhattan_distance(r1, r2)
        pass
    
    def end_execution(self):
        self.execution_end_time = current_milli_time()
        elapsed = self.execution_end_time - self.execution_start_time
        return elapsed
    
    # Actual, links, time_window, time stats (exec, issue etc..)
    def get_stats(self):
        pass
        
    def get_json(self):
        d = {}
        
        d['inquiry_time'] = self.inquiry_time
        d['t_id'] = self.t_id
        d['node'] = self.node
        d['gridA'] = self.gridA
        d['gridB'] = self.gridB
        d['time_window'] = self.time_window
        d['state'] = self.state
        d['next_node'] = self.next_node
        d['parsed_id'] = self.parsed_id
        d['step'] = self.step
        d['steps'] = self.steps
        d['retry_count'] = self.retry_count
        d['next_rsu'] = self.next_rsu
        d['rsu_assigned_to'] = self.rsu_assigned_to
        d['route'] = self.route
        
        return d

    def toJson(self):
        return json.dumps(self, default=lambda o: o.__dict__)

    def __repr__(self):
        return "{:16.16}\t{}:{}\\{}\t{}".format(self.t_id, self.node, self.gridA, self.gridB, self.time_window)

    def __str__(self):
        return "{:16.16}\t{}:{}\\{}\t{}".format(self.t_id, self.node, self.gridA, self.gridB, self.time_window)