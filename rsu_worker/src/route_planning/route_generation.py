import os
import math
import random
import warnings
warnings.filterwarnings('ignore')

import networkx as nx
import osmnx as ox
from shapely.geometry import Point, shape

from utils.Utils import *
import src.distributed_routing.utility as geo_utils

DEBUG = 0
ident = os.environ['WORKER_ID']

class Route_Generator(object):
    def __init__(self):

        if not os.path.exists(os.path.join(os.getcwd(), 'data')):
            raise OSError("Must first download data, see README.md")
        self.data_dir = os.path.join(os.getcwd(), 'data')

        self.file_path = os.path.join(self.data_dir, '3x3_graphs', f'{ident}.pkl')
        self.g = nx.read_gpickle(self.file_path)

        # Without obfuscation
        self.percent_to_delete = 0

    def find_route(self, payload):
        t, r = self.partial_route_executor(self.g, payload)
        return t, r

    # Obfuscation
    def remove_per_of_nodes(self, base_graph, percent_to_delete=0):
        node_count = len(base_graph.nodes)
        number_to_delete = node_count * percent_to_delete // 100
        nodes_to_delete = np.random.choice(base_graph.nodes, size=number_to_delete, replace=False)
        
        edges = ox.graph_to_gdfs(base_graph, edges=True, nodes=False)
        edges = edges[~edges['u'].isin(nodes_to_delete)]
        edges = edges[~edges['v'].isin(nodes_to_delete)]
        
        nodes = ox.graph_to_gdfs(base_graph, edges=False, nodes=True)
        nodes = nodes[~nodes.index.isin(nodes_to_delete)]
        G = ox.graph_from_gdfs(nodes, edges)
        return G
        
    # Distributed Routing
    def partial_route_executor(self, G, task, start_node=0, mod_G = None, speed_data=None):
        if not mod_G:
            mod_G = self.g

        node    = task['node']
        gridA   = task['gridA']
        gridB   = task['gridB']

        if task['next_node']:
            start_node = task['next_node']

        if isinstance(node, dict):
            node = shape(node)
            task['node'] = node
        if isinstance(gridA, dict):
            gridA = shape(gridA)
            task['gridA'] = gridA
        if isinstance(gridB, dict):
            gridB = shape(gridB)
            task['gridB'] = gridB

        print_log(f"->partial_route_executor()")
        
        t = None
        r = None
        if isinstance(node, Point) and isinstance(gridA, Point) and isinstance(gridB, int):
            t, r = self.complete_route_single_grid(G, mod_G, task, speed_data)
            
        elif isinstance(node, Point) and isinstance(gridA, int) and gridB is None:
            t, r = self.partial_route_destination_grid(G, task, start_node, speed_data)
                    
        elif isinstance(node, Point) and isinstance(gridA, int) and isinstance(gridB, int):
            t, r = self.partial_route_start_grid(G, mod_G, task, speed_data)
            
        elif isinstance(gridA, int) and isinstance(gridB, int):
            t, r = self.partial_route_intermediate_grid(G, task, start_node, speed_data)

        return t, r

    # Single grid task (1 task only)
    #  'node':Point, 'gridA': Point, 'gridB': geohash (int),
    def complete_route_single_grid(self, G, mod_G, task, speed_data=None):
        print_log(f"->complete_route_single_grid()")
        start_loc = task['node']
        end_loc   = task['gridA']
        time_window = task['time_window']
        time_key = self.generate_time_key(time_window)
        n_s = ox.get_nearest_node(mod_G, (start_loc.y, start_loc.x))
        n_d = ox.get_nearest_node(G, (end_loc.y, end_loc.x))
        try:
            # t, r = nx.dijkstra_path_timed(G, n_s, n_d, get_speed_data_at_time_dict, speed_data, time_key)
            t, r = nx.dijkstra_path_timed(G, n_s, n_d, self.get_random_speed)
            return t, r
        except:
            return None, None

    # if gridA is a point and gridB is a geohash
    def partial_route_start_grid(self, G, mod_G, task, speed_data=None):
        print_log(f"->partial_route_start_grid()")
        curr_grid = task['gridA']
        next_grid = task['gridB']
        start_loc = task['node']
        time_window = task['time_window']
        time_key = self.generate_time_key(time_window)
        
        n_s = ox.get_nearest_node(mod_G, (start_loc.y, start_loc.x))
        
        # TODO: look for all nodes which go to the next_grid
        candidate_nodes = []
        for n in G.nodes:
            node = G.nodes[n]
            grid = node['grid']
            if grid == next_grid:
                candidate_nodes.append(n)
                
        shortest_l = 9999
        best_node = None
        for cn in candidate_nodes:
            try:
                l = nx.shortest_path_length(G, n_s, cn)
                if l < shortest_l:
                    shortest_l = l
                    best_node = cn
            except:
                pass
            
        if not best_node:
            return None, None
        else:
            try:
                # t, r = nx.dijkstra_path_timed(G, n_s, best_node, get_speed_data_at_time_dict, speed_data, time_key)
                t, r = nx.dijkstra_path_timed(G, n_s, best_node, self.get_random_speed)
                return t, r
            except:
                return None, None

    # Needs information from the previous partial route
    # if gridA and gridB are both geohashes
    def partial_route_intermediate_grid(self, G, task, start_node, speed_data=None):
        print_log(f"->partial_route_intermediate_grid()")
        curr_grid = task['gridA']
        next_grid = task['gridB']
        time_window = task['time_window']
        time_key = self.generate_time_key(time_window)

        nodes, edges = ox.graph_to_gdfs(G, nodes=True, edges=True)
        candidate_edges = edges.loc[edges['is_boundary']]
        candidate_edges['u_grid'] = candidate_edges['u'].apply(lambda x: nodes.loc[nodes.index == x]['grid'].values[0])
        candidate_edges['v_grid'] = candidate_edges['v'].apply(lambda x: nodes.loc[nodes.index == x]['grid'].values[0])
        candidate_edges = candidate_edges.loc[((candidate_edges['u_grid'] == curr_grid) | \
                                               (candidate_edges['v_grid'] == curr_grid)) & 
                                               ((candidate_edges['u_grid'] == next_grid) | \
                                               (candidate_edges['v_grid'] == next_grid))]
        candidate_nodes_u = candidate_edges.loc[candidate_edges['u_grid'] == next_grid]['u'].unique().tolist()
        candidate_nodes_v = candidate_edges.loc[candidate_edges['v_grid'] == next_grid]['v'].unique().tolist()

        candidate_nodes = candidate_nodes_u + candidate_nodes_v

        prev_route = task['prev_route']
        removed_nodes = -1

        while abs(removed_nodes) != (len(prev_route) + 1):
            start_node = prev_route[removed_nodes]
            shortest_l = math.inf
            best_node = None
            for cn in candidate_nodes:
                try:
                    l = nx.shortest_path_length(G, start_node, cn)
                    if l < shortest_l:
                        shortest_l = l
                        best_node = cn
                except:
                    pass
            if best_node:
                try:
                    t, r = nx.dijkstra_path_timed(G, start_node, best_node, self.get_random_speed)
                    return t, r
                except:
                    removed_nodes -= 1
                    pass
            else:
                removed_nodes -= 1
                
        #     r = nx.shortest_path(G, start_node, best_node)
            # t, r = nx.dijkstra_path_timed(G, start_node, best_node, get_speed_data_at_time_dict, speed_data, time_key)
        return None, None

    # Final grid
    # node is Point, gridA is a geohash and gridB is undefined.
    # must go from gridA at start_node to the Point 
    def partial_route_destination_grid(self, G, task, start_node, speed_data=None):
        print_log(f"->partial_route_destination_grid()")
        curr_grid = task['gridA']
        end_loc = task['node']
        time_window = task['time_window']
        time_key = self.generate_time_key(time_window)
        
        prev_route = task['prev_route']

        removed_nodes = -1
        n_d = ox.get_nearest_node(G, (end_loc.y, end_loc.x))

        while abs(removed_nodes) != (len(prev_route) + 1):
            start_node = prev_route[removed_nodes]

            try:
                t, r = nx.dijkstra_path_timed(G, start_node, n_d, self.get_random_speed)
                return t, r
            except:
                removed_nodes -= 1
                pass
        return None, None

    # Speeds data getter lambda

    def get_random_speed(self, start, end, attr, sensor_data=None, time_window=None):
        return random.randint(1, 21)

    # Got lazy, and used a graph that is a global variable. wont be a problem when implemented in Dockers
    def get_speed_data_at_time_dict(self, start, end, attr, sensor_data=None, time_window=None):
        vals = attr[0]
        osmid = vals['osmid']
        length = vals['length']
        
        start_loc = (G.nodes[start]['y'], G.nodes[start]['x'])
        end_loc   = (G.nodes[end]['y'], G.nodes[end]['x'])
        km_length = haversine(start_loc, end_loc)
        if km_length:
            length = km_length
            
        if isinstance(osmid, list):
            for osmid_ in osmid:
                if str(osmid_) in sensor_data['speeds']:
                    if time_window in sensor_data['speeds'][str(osmid_)]:
                        return length / sensor_data['speeds'][str(osmid_)][time_window]
            return length / GLOBAL_SPEED
        
        osmid = str(osmid)
        if osmid not in sensor_data['speeds']:
            return length / GLOBAL_SPEED
        else:
            if time_window in sensor_data['speeds'][osmid]:
                return length / sensor_data['speeds'][osmid][time_window]
            else:
                e_mean = np.asarray(list(sensor_data['speeds'][osmid].values())).mean()
                return length / e_mean

    def generate_time_key(self, task_time_window):
        t_h = int(task_time_window.split(":")[0])
        t_m = int(task_time_window.split(":")[1])
        if t_m == 0:
            time_start = f"{str((t_h - 1)% 12).zfill(2)}:59"
            time_end   = f"{str(t_h % 12).zfill(2)}:{str(t_m).zfill(2)}"
        else:
            time_start = f"{str(t_h % 12).zfill(2)}:{str(t_m - 1).zfill(2)}"
            time_end   = f"{str(t_h % 12).zfill(2)}:{str(t_m).zfill(2)}"
        time_key = (time_start, time_end)
        return time_key
