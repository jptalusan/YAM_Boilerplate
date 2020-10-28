import networkx as nx
from utils.Utils import *
from src.conf import GLOBAL_VARS
from src.distributed_routing.task import Task

from shapely.geometry import Point

def generate_optimal_sequence_grid(G, route):
    osg = []
    es = convert_route_to_edge_pairs(route)
    for i, e in enumerate(es):
        e_data = G.get_edge_data(*e, 0)
        is_bounds = e_data['is_boundary']
        if i == 0:
            u = e[0]
            osg.append(G.nodes[u]['grid'])
        if is_bounds:
            u = e[0]
            v = e[1]
            osg.append(G.nodes[v]['grid'])
    return osg

def convert_route_to_edge_pairs(route):
    edges = zip(route[::], route[1::])
    return list(edges)

# payload sample: 
'''
{"q_id": "43d5a688", 
"time": 1603782550.165596, 
"s": {"type": "Point", "coordinates": [135.516419, 34.709908]}, 
"d": {"type": "Point", "coordinates": [135.530706, 34.679987]}, 
"t_h": 10, "t_m": 59, 
"h": 0, 
"task_type": "ROUTE_PLANNING"}
'''

def generate_tasks(payload):
    task_list = []
    #     print(i, row)
    og = payload['osg']
    t_id = payload['q_id']
    s = Point(payload['s']['coordinates'][0], payload['s']['coordinates'][1])
    d = Point(payload['d']['coordinates'][0], payload['d']['coordinates'][1])
    # s = payload['s']
    # d = payload['d']
    t = str(payload['t_h']) + ':' + str(payload['t_m'])
    h = payload['h']

    if len(og) >= 2:
        nodes = []
        nodes.append(s)
        nodes.extend([None] * (len(og) - 2))
        nodes.append(d)
        og.append(None)

    #             # [t] * len(og) -> Assigns time variable to each pair
        ids = ["{}{}{}".format(t_id, 
                                str(i).zfill(3), 
                                str(len(og) - 2).zfill(3)) for i in range(len(og) - 1)]
        pairs = zip(ids, nodes, og, og[1:], [t] * len(og), [h] * len(og))
    elif len(og) == 1:
        id_ = "{}{}{}".format(t_id, 
                                str(0).zfill(3), 
                                str(0).zfill(3))
        pairs = zip([id_], [s], [d], [og[0]], [t], [h])
        pass

    labels = ["_id", "node", "gridA", "gridB", "time_window", "budget"]
    for p in pairs:
        task_json = dict(zip(labels, p))
        task_json['state'] = GLOBAL_VARS.TASK_STATES["UNSENT"]
        task_json['next_node'] = None
        task_json['inquiry_time'] = time_print(0)
        task_json['next_rsu'] = None
        task_json['rsu_assigned_to'] = None
        task_json['route'] = None
        task_list.append(Task(task_json))
    task_list.extend(list(pairs))
    return task_list