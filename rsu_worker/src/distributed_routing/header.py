import random
import networkx as nx
import pandas as pd
import uuid
import copy
from .task import Task
import json
from common.conf import GLOBAL_VARS
import common.src.basic_utils as utils

# This is the entirety of query generation because the SG needs to be "pre-determined" before processing
# (since we dont have any model that would tell us the next best grid right now)
# Maybe creating a dataframe will be easier here.
# Generates a dataframe with columns (s, d, t, r)
'''
*s: source node
*d: destination node
*t: departure time
*r: route
'''
def generate_query(G, no_of_queries):
    Q = []
    routes = []
    for _ in range(no_of_queries):
        while len(routes) != no_of_queries:
            orig_node = random.choice(list(G.nodes))
            dest_node = random.choice(list(G.nodes))
            if orig_node == dest_node:
                continue
            try:
                q = {}
                q['t_id'] = get_trunc_task_id(uuid.uuid1())
                q['s'] = orig_node
                q['d'] = dest_node
                time = random.randint(0, 23)
                q['t'] = time
                route = nx.shortest_path(G, orig_node, dest_node)
                if __debug__:
                    print('Path: {}:{}'.format(orig_node, dest_node))
                routes.append(route)
                q['r'] = route
                Q.append(q)
            except nx.NetworkXNoPath:
#                 print('No path: {}:{}'.format(orig_node, dest_node))
                pass
    return pd.DataFrame(Q)

def generate_single_query(G, s, d, t):
    print("generate_single_query(G, {}, {}, {})".format(s, d, t))
    Q = []
    routes = []
    orig_node = s
    dest_node = d
    if orig_node == dest_node:
        print("Orig node and dest node should not be equal")
        return None
    try:
        q = {}
        q['t_id'] = get_trunc_task_id(uuid.uuid1())
        q['s'] = orig_node
        q['d'] = dest_node
        q['t'] = t
        route = nx.shortest_path(G, orig_node, dest_node)
        if __debug__:
            print('Path: {}:{}'.format(orig_node, dest_node))
        routes.append(route)
        q['r'] = route
        Q.append(q)
    except nx.NetworkXNoPath:
        print('No path: {}:{}'.format(orig_node, dest_node))
        pass
    return pd.DataFrame(Q)

# Measure the number of routes that exist in only 1 grid maybe
# [s, x    , d]
# grid_id is not always available?
def gen_SG(G, Qdf):
    sg_list = []
    for _, row in Qdf.iterrows():
        route = row['r']
        sg = []
        for i, n in enumerate(route):
            node = G.nodes[n]
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
        sg_list.append(list(sg))
    return sg_list

def save_query_dataframe(df, path):
    df.to_pickle(path)
    return True

def get_trunc_task_id(t_id):
        return '{:8.8}'.format(str(t_id))

# ID, Node, Grid1, Grid2, Time
def generate_tasks(Qdf): 
    task_list = []
    for _, row in Qdf.iterrows():
        t_id = row.t_id
        og = copy.copy(row['og'])
        s = row.s
        d = row.d
        t = row.t
        
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
            task_json['inquiry_time'] = utils.time_print(0)
            task_json['next_rsu'] = None
            task_json['rsu_assigned_to'] = None
            task_json['route'] = None
            task_list.append(Task(task_json))
        task_list.extend(list(pairs))
    return task_list