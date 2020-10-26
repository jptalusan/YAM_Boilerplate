import os
import networkx as nx
import osmnx as ox
import src.distributed_routing.utility as geo_utils
import math
import random

DEBUG = 0

# This will all be redesigned i guess?
class Route_Generator(object):
    def __init__(self, nx_g, p, bpc):
        self.precision = p
        self.bits_per_char = bpc
        
        if not os.path.exists(os.path.join(os.getcwd(), 'data')):
            raise OSError("Must first download data, see README.md")
        self.data_dir = os.path.join(os.getcwd(), 'data')

        self.sub_graphs_dir = os.path.join(self.data_dir, 
                                 f'sub_graphs_p{self.precision}_b{self.bits_per_char}')

        self.sub_graph_dict = geo_utils.read_saved_sub_graphs(self.sub_graphs_dir)

        self.g = nx_g

    def find_route(self, task):
        # print("Find_route of task: ", task['_id'])
        # Assuming first all tasks are assigned to one single RSU
        # Add checking for task Id so can reset the visualization
        # task_queue = task_list[8:14]

        next_node = task['next_node']
        task_id = task['t_id']
        n = task['node']
        gA = task['gridA']
        gB = task['gridB']
        t = task['time_window']
        parsed_id = task['parsed_id']
        task_count = int(task['step'])
        task_length = int(task['steps'])

        if next_node is None and (task_count != 0 and task_length != 0):
            print("Error: Need to get the next_node value first.")
            return None

        if next_node is None:
            if DEBUG:
                print("{}-node1: {}, gridA: {}, gridB: {}, node2: None, time: {}".
                    format(task_count, n, gA, gB, t))

            r = self.get_task_route(node1=n, gridA=gA, gridB=gB, node2=None, time=t)
        elif next_node:
            if DEBUG:
                print("{}-node1: {}, gridA: {}, gridB: {}, node2: {}, time: {}".
                    format(task_count, n, gA, gB, next_node, t))

            r = self.get_task_route(node1=n, gridA=gA, gridB=gB, node2=next_node, time=t)

        return r

    # TODO: Redesign
    def get_bounds_between_two_grids(self, grid1, grid2):
        possible_nodes = []
        
        sg1 = self.sub_graph_dict[grid1]
        sg2 = self.sub_graph_dict[grid2]
        
        for n in sg1.nodes:
            node = sg1.node[n]
            if 'is_bounds' in node and node['is_bounds']:
                boundaries = node['boundaries']
                if set([grid1, grid2]) == set(boundaries):
                    # print(n, node)
                    if n not in possible_nodes:
                        possible_nodes.append(n)
                        
        # print("\n")

        for n in sg2.nodes:
            node = sg2.node[n]
            if 'is_bounds' in node and node['is_bounds']:
                boundaries = node['boundaries']
                if set([grid1, grid2]) == set(boundaries):
                    # print(n, node)
                    if n not in possible_nodes:
                        possible_nodes.append(n)
        # print("Sub-Grids:", type(sg1), type(sg2))
        return possible_nodes

    def random_speeds(self, start, end, attr, sensor_data=None, time_window=None):
        if 0 not in attr:
            key = random.choice(list(attr))
            tmc_id = attr[key]['tmc_id']
        else:
            tmc_id = attr[0]['tmc_id']
        average_speed_at_time_window = random.uniform(1.86, 80.78)
        return average_speed_at_time_window

    def get_shortest_route_random(self, sg, grid, node, time, bounds_list):
        # Assume that rsu_arr is present in the rsu
        fastest = math.inf
        route = None
        for b in bounds_list:
            try:
                (total_time, avg_speed_route) = nx.dijkstra_path_timed(sg, node, b, self.random_speeds, rsu_hash=None, time_window=time)
                if total_time < fastest:
                    fastest = total_time
                    route = avg_speed_route
            except nx.NetworkXNoPath:
                print("No path: {}-{}".format(node, b))
                pass
            except nx.NodeNotFound as e:
                utils.print_log("Error: {}".format(e))

        if route == None:
            return None, None
        return (total_time, route)
        
    # Node 2 is the node from the previous hop/route
    def get_task_route(self, node1, gridA, gridB, time, node2=None):
        if (node1 is None) and ((gridA is None) or (gridB is None)):
            print("False")
            return False
        elif node1 is not None and (gridA is not None and isinstance(gridA, str)) and (gridB is not None and isinstance(gridB, str)):
            # First hop (use node1 and the boundary )
            if DEBUG:
                print("Task A")
            bounds = self.get_bounds_between_two_grids(gridA, gridB)
            # (total_time, route) = self.get_shortest_route(whole_graph, gridA, node1, time, bounds)
            (total_time, route) = self.get_shortest_route_random(self.g, gridA, node1, time, bounds)
            return total_time, route
            
        elif node1 is None and (gridA is not None and isinstance(gridB, str)) and (gridB is not None and isinstance(gridB, str)):
            # Middle hop: must make use of node2 (the result of the previous hop)
            if DEBUG:
                print("Task B")
            bounds = self.get_bounds_between_two_grids(gridA, gridB)
            # (total_time, route) = self.get_shortest_route(self.whole_graph, gridA, node2, time, bounds)
            (total_time, route) = self.get_shortest_route_random(self.g, gridA, node2, time, bounds)
            return total_time, route
        
        elif node1 is not None and gridA is not None and gridB is None:
            # Last hop: must make use of node2 (the result of the previous hop)

            if DEBUG:
                print("Task C")
            
            # TODO: Return the use of speed hashes for this part. For now its just random, to make the basic route planning work
            # Assume that rsu_arr is present in the rsu
            # G = self.get_dataframe_historical_data(gridA, with_neighbors=True)
            # G = get_speeds_hash_for_grid(gridA, with_neighbors=True)
            
            fastest = math.inf
            route = None
            try:
                sg = self.g
                # (total_time, avg_speed_route) = nx.dijkstra_path_timed(sg, node2, node1, self.get_travel_time_from_database, G, time_window=time)
                # (total_time, avg_speed_route) = nx.dijkstra_path_timed(sg, node2, node1, get_avg_speed_at_edge, G, time_window=time)
                (total_time, avg_speed_route) = nx.dijkstra_path_timed(sg, node2, node1, self.random_speeds, rsu_hash=None, time_window=time)
                if total_time < fastest:
                    fastest = total_time
                    route = avg_speed_route
            except nx.NetworkXNoPath:
                print("No path: {}-{}".format(node2, node1))
            except nx.NodeNotFound as e:
                utils.print_log("Error: {}".format(e))

            if route == None:
                return None, None
            return total_time, route
        
        elif node1 is not None and isinstance(gridA, int) and isinstance(gridB, str):
            # Single grid, just use node1 and gridA
            if DEBUG:
                print("Task D")

            # TODO: Same as above
            # Assume that rsu_arr is present in the rsu
            # G = self.get_dataframe_historical_data(gridB, with_neighbors=True)
            # G = get_speeds_hash_for_grid(gridB, with_neighbors=True)
            
            fastest = math.inf
            route = None
            try:
                sg = self.g
                # (total_time, avg_speed_route) = nx.dijkstra_path_timed(sg, node1, gridA, self.get_travel_time_from_database, G, time_window=time)
                # (total_time, avg_speed_route) = nx.dijkstra_path_timed(sg, node1, gridA, get_avg_speed_at_edge, G, time_window=time)
                (total_time, avg_speed_route) = nx.dijkstra_path_timed(sg, node1, gridA, self.random_speeds, rsu_hash=None, time_window=time)
                
                if total_time < fastest:
                    fastest = total_time
                    route = avg_speed_route
            except nx.NetworkXNoPath:
                print("No path: {}-{}".format(node1, gridA))
            except nx.NodeNotFound as e:
                utils.print_log("Error: {}".format(e))
            if route == None:
                return None, None
            return total_time, route
        else:
            print("Failed")
        pass