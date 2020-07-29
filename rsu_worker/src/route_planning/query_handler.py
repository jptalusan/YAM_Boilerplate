import networkx as nx
from utils.Utils import *
from src.conf import GLOBAL_VARS
from src.distributed_routing.task import Task

def generate_optimal_grid_sequence(nx_g, payload):
    q_id = payload['q_id']
    s = payload['s']
    d = payload['d']
    t = payload['t']
    
    route = nx.shortest_path(nx_g, s, d)
    
    sg = []
    for i, n in enumerate(route):
        node = nx_g.node[n]
        if i == 0 or i == len(route) - 1:
            if 'grid_id' in node:
                if node['grid_id'] not in sg:
                    sg.append(node['grid_id'])

        if 'is_bounds' in node:
            if node['is_bounds']:
                pass
            else:
                if node['grid_id'] not in sg:
                    sg.append(node['grid_id'])
    return (route, list(sg))

def generate_tasks(payload):
    task_list = []
    #     print(i, row)
    og = payload['ogs']
    t_id = payload['q_id']
    s = payload['s']
    d = payload['d']
    t = payload['t']

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
        pairs = zip(ids, nodes, og, og[1:], [t] * len(og))
    elif len(og) == 1:
        id_ = "{}{}{}".format(t_id, 
                                str(0).zfill(3), 
                                str(0).zfill(3))
        pairs = zip([id_], [s], [d], [og[0]], [t])
        pass

    labels = ["_id", "node", "gridA", "gridB", "time_window"]
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