# Dependencies
import importlib
import time
import os
import pandas as pd
import numpy as np
import pickle
import matplotlib.pyplot as plt
import json
import random
import uuid
import math
import time
import datetime
import sys

from os import listdir
from os.path import isfile, join
from statistics import mean

import networkx as nx

import geohash_hilbert as ghh

from shapely.geometry import Polygon
from shapely.geometry import LineString
from shapely.geometry import Point
from shapely.geometry import MultiPoint
from shapely.geometry.polygon import Polygon
from shapely.ops import nearest_points

SHOW_GRAPHS = False

def random_color():
    color = tuple(np.random.random(size=3))
    return color

def divide_grid(boundary, divisions):
    wd, hd = divisions
    # Divide the boundary
    points = MultiPoint(boundary.boundary.coords)
    height = abs(points[1].y - points[0].y)
    hdivs = height / hd
    
    width = abs(points[2].x - points[1].x)
    wdivs = width / wd
    
    tl = Point(points[0].x, points[0].y)
    
    polys = []
    
    for i in range(wd):
        for j in range(hd):
            
            tl3 = Point(tl.x + (wdivs * i), tl.y - (hdivs * j))
            tr3 = Point(tl.x + (wdivs * (i + 1)), tl.y - (hdivs * j))
            bl3 = Point(tl.x + (wdivs * i), tl.y - (hdivs * (j + 1)))
            br3 = Point(tl.x + (wdivs * (i + 1)), tl.y - (hdivs * (j + 1)))
            pl3 = [tl3, tr3, br3, bl3]
            poly3 = Polygon([[p.x, p.y] for p in pl3])
            polys.append(poly3)
    return polys

def cleanup_graph(G):
    # cleanup
    to_delete = []
    for u,v,a in G.edges(data=True):
        if not bool(a):
            to_delete.append((u ,v))
    for u, v in to_delete:
        G.remove_edge(u, v)
    return G

def breakdown_graph_to_nodes_edges(G, rsu_arr):
    new_nodes = []
    new_edges = []
    old_edges = []
    total_nodes = len(G.nodes)
    total_edges = len(G.edges)

    print("Current nodes: {}, current edges: {}".format(total_nodes, total_edges))
    total_nodes += 1

    for i, e in enumerate(G.edges):
        start, end, _ = e
        # This shouldn't happen or at least have any effect
        if start == end:
            continue

        edge = G.get_edge_data(start, end)
        for edge_count in list(edge.keys()):
            geometry = edge[edge_count]['geometry']
            tmc_id = edge[edge_count]['tmc_id']
            length = edge[edge_count]['length']

            start_node = G.nodes[e[0]]
            end_node   = G.nodes[e[1]]

            crossed = False
            r_crossings = []
            for r in rsu_arr:
                rpoly = r.poly

                if rpoly.crosses(geometry):
                    r_crossings.append(r)
                    crossed = True

            # For debugging
            if SHOW_GRAPHS:
                if len(r_crossings) < 3:
                    continue        

            if crossed:
                if SHOW_GRAPHS:
                    plt.title("Edge: {}".format(e))
                    fig, ax = plt.subplots(1, 1, figsize=(13, 13))
                    plt.plot(*geometry.xy, color='red', alpha=1)

                    # Graphing ends of line
                    plt.plot(start_node['x'], start_node['y'], marker='o')
                    plt.text(start_node['x'], start_node['y'], 'S')
                    plt.plot(end_node['x'], end_node['y'], marker='^')
                    plt.text(end_node['x'], end_node['y'], 'D')

                int_arr = []
                for r in r_crossings:
                    rpoly = r.poly
                    boundary = rpoly.boundary

                    if SHOW_GRAPHS:
                        plt.plot(*rpoly.exterior.xy, color='blue', alpha=0.5)
                        plt.text(rpoly.centroid.x, rpoly.centroid.y, '{}-{}'.format(r.get_idx(), r.grid_id))

                    top    = LineString(rpoly.boundary.coords[0:2])
                    right  = LineString(rpoly.boundary.coords[1:3])
                    bottom = LineString(rpoly.boundary.coords[2:4])
                    left   = LineString([rpoly.boundary.coords[3], rpoly.boundary.coords[0]])
                    lines = [top, right, bottom, left]
                    line_labels = ['north', 'east', 'south', 'west']

                    for line in lines:
                        intersections = line.intersection(geometry)

                        if not intersections.is_empty:
                            if isinstance(intersections, MultiPoint):
                                for intersection in intersections:
                                    if intersection not in int_arr:
                                        int_arr.append(intersection)
                            else:
                                if intersections not in int_arr:
                                    int_arr.append(intersections)

                # Checking distance of boundaries from one end
                nearest = math.inf
                ordered_bounds = []
                if len(int_arr) > 0:
                    for bounds in int_arr:
                        p_start_node = Point(start_node['x'], start_node['y'])
                        dist = p_start_node.distance(bounds)
                        if dist < nearest:
                            ordered_bounds.insert(0, bounds)
                            nearest = dist
                        else:                    
                            ordered_bounds.append(bounds)


                # Check which boundaries are intersecting which polys
                # Create the list which will be the basis of the new edges
                temp_edge = []
                temp_edge.append(start)
                for bounds in ordered_bounds:
                    temp = []
                    for r in r_crossings:
                        rpbounds = r.poly.boundary
                        # Is this really supposed to be just "contains"?
                        if rpbounds.contains(bounds):
    #                     if rpbounds.crosses(bounds):
                            temp.append(r.grid_id)
        #             print(bounds, temp)
                    new_nodes.append({'idx': total_nodes, 'point': bounds, 'boundaries':temp})
                    temp_edge.append(total_nodes)
                    total_nodes += 1
                temp_edge.append(end)
                new_edges.append({edge_count: temp_edge})
                old_edges.append([start, end, edge_count])

        #         print(len(int_arr), *ordered_bounds)
                for j, bounds in enumerate(ordered_bounds):
                    if SHOW_GRAPHS:
                        plt.plot(bounds.x, bounds.y, marker='x')
                        plt.text(bounds.x, bounds.y, str(j), color='red')


            if SHOW_GRAPHS:
                if i == 1:
                    break

    to_add_edge = 0
    for e in new_edges:
        to_add_edge += len(e)
    print("Nodes to add: {}, edges to add: {}".format(len(new_nodes), to_add_edge))
    print("Edges to del: {}".format(len(old_edges)))
    print("Done")
    return new_nodes, new_edges, old_edges

'''
These functions serve as the way to reconstruct the existing network map G by the following:
* labelling_of_nodes_within_grids: Adds labels to all the nodes within grids:
{'x': -86.75849000000007,
 'y': 36.19380000000001,
 'grid_id': 'SPB_nt',
 'is_bounds': False}
 

* create_new_boundary_nodes: Creates new boundary nodes with additional attributes (it loses the grid_id attr), while boundaries is a list of all neighbors bounded by the node (max of 4):
{'x': -86.80295767513587,
 'y': 36.162157134375,
 'is_bounds': True,
 'boundaries': ['SPB_uP', 'SPBZWF']}
 
 
'''

def labelling_of_nodes_within_grids(G, rsu_arr):
    sub_graph_nodes = {}
    for r in rsu_arr:
        sub_graph_nodes[r.grid_id] = []
        rpoly = r.poly
        for n in G.nodes:
            node = G.node[n]
            node_point = Point(node['x'], node['y'])
            if node_point.within(rpoly):
                sub_graph_nodes[r.grid_id].append(n)
                node['grid_id'] = r.grid_id
                node['is_bounds'] = False
    #             display(node)

    # Sanity check
#     display(G.node[1500])
    print("Current nodes: {}, current edges: {}".format(len(G.nodes), len(G.edges)))
    return G

def create_new_boundary_nodes(G, new_nodes):

    # Create new boundary nodes first
    for node in new_nodes:
        idx = node['idx']
        point = node['point']
        boundaries = node['boundaries']
        G.add_node(idx)
        attrs = {idx: {'x': point.x, 'y': point.y, 'is_bounds':True, 'boundaries': boundaries}}
        nx.set_node_attributes(G, attrs)
    #     break

    # Sanity check
#     display(nx_g.nodes[idx])
    print("Current nodes: {}, current edges: {}".format(len(G.nodes), len(G.edges)))
    return G

'''
The following are all functions that detail how to break down edges into its smaller components.
These are all edges that traverse 2 or more grids in its geometry. Edges are broken down into smaller edges
with endpoints between boundary nodes.

|s----X----X----e| where s and e are start and end nodes, while X are boundary nodes.
so edge s-e will be split into 3 smaller edges.

*break_edges: Attempts to break edge geometries by identifying where to insert the new boundary nodes
into the current list of nodes that make up the edge.

*create_new_edges: Creates new edges with the following attributes.
{
(x, y, edge_count): {'tmc_id': tmc_id, 'length': length, 'geometry': geometry}
}

*delete_old_edges: Deletes the old edge s-e. Need to remove, if not, will cause erroneous data.

*restoring_missed_boundaries: Problem faced when extremely short edges (s and e are the same node)
and nearby the grid boundaries. Just changing the attributes of nodes, setting them [is_bounds] to True and,
modifying the [boundaries] list. Not creating new nodes.
'''
def node_to_point(N, n):
    return Point(N.node[n]['x'], N.node[n]['y'])

def find_nearest_index(line, bounds):
    indices = []
    edge_mp = MultiPoint(line.coords)
    new_list = list(line.coords)
    temp_b = None
    for b in bounds:
        near_pt = nearest_points(edge_mp, b)[0]
        temp_dist = math.inf
        temp_indx = 0
        for i, (x, y) in enumerate(line.coords):
            pt = Point(x, y)
            dist1 = pt.distance(b)
            if dist1 < temp_dist:
                temp_dist = dist1
                temp_indx = i
                temp_b = (b.x, b.y)
        if temp_indx in indices:
            temp_indx = temp_indx + 1
        indices.append(temp_indx)
            
        if temp_b is not None:
            new_list.insert(temp_indx, temp_b)
        temp_b = None
        temp_indx = 0
           
    return indices, new_list

def break_edges(indices, line_list, bounds):
    edges_to_add = []
    e1_list = []
    # Create nodes
    
    # Start
    if len(indices) == 1:
        if len(line_list[0:indices[0] + 1]) != 1:
            e1_list.extend(line_list[0:indices[0] + 1])
            e1 = LineString(e1_list) 
            edges_to_add.append(e1)

        e1_list = []
        e1_list.extend(line_list[indices[0]:])
        e1 = LineString(e1_list) 
        edges_to_add.append(e1)
        return edges_to_add
    
    if indices[0] == 0:
        e1_list.extend(line_list[0:2])
    else:
        e1_list.extend(line_list[0:indices[0] + 1])
    e1 = LineString(e1_list) 
    edges_to_add.append(e1)
    # Middle segments
    for i, idx in enumerate(indices):
        e1_list = []
        if i == 0:
            e1_list.extend(line_list[idx:indices[i + 1] + 1])
            e1 = LineString(e1_list) 
            edges_to_add.append(e1)
        if i != 0 and i < len(indices) - 1:
            if indices[i + 1] == (idx + 1):
                e1_list.extend(line_list[idx:indices[i + 1] + 1])
            else:
                e1_list.extend(line_list[idx:indices[i + 1] + 1])
                
            e1 = LineString(e1_list) 
            edges_to_add.append(e1)
            
    # End
    e1_list = []
    e1_list.extend(line_list[indices[-1]:])
    e1 = LineString(e1_list) 
    edges_to_add.append(e1)
    
    return edges_to_add

def create_new_edges(G, new_edges):
    # I dont think of the edge length first because I dont know how to split it accurately.
    # This is jerry-rigged and inaccurate but this is all we have
    ifs = 0
    els = 0
    for n_e in new_edges:
        for edge_count, node_list in n_e.items():
            es = G.get_edge_data(node_list[0], node_list[-1])
            tmc_id = es[edge_count]['tmc_id']
            length = es[edge_count]['length']
            geometry = es[edge_count]['geometry']

            b_ps = [Point(G.node[n]['x'], G.node[n]['y']) for n in node_list[1:-1]]
            i, nl = find_nearest_index(geometry, b_ps)
            broken_edges = break_edges(i, nl, b_ps)

            s_p = node_to_point(G, node_list[0])

            distances = []
            for node in node_list:
                np = node_to_point(G, node)
                distances.append(s_p.distance(np))

            # FIXME: ZeroDivisionError: float division by zero
            # Encountered when i rerun on the new network graph
            distances = [n / distances[-1] for n in distances]
            distances = [n * length for n in distances]

            d_idx = 1
            pairs = zip(node_list, node_list[1:])
            
            # If edges and bounds match, then try to guesstimate the resulting length of edge fragments
            if (len(node_list) - 1) == len(broken_edges):
                for i, (x, y)in enumerate(pairs):
                    segment_length = distances[d_idx] - distances[d_idx - 1]
                    G.add_edge(x, y)
                    attrs = {(x, y, edge_count): {'tmc_id': tmc_id, 'length': segment_length, 'geometry': broken_edges[i]}}
                    nx.set_edge_attributes(G, attrs)
                    d_idx += 1
                    ifs += 1
            # Else: just keep it as is. This is a limitation of the network graph from the TMC data.
            else:
                for x, y in pairs:
                    G.add_edge(x, y)
                    attrs = {(x, y, edge_count): {'tmc_id': tmc_id, 'length': length, 'geometry': geometry}}
                    nx.set_edge_attributes(G, attrs)
                    els += 1
    print("Added {} ifs, {} els".format(ifs, els))
    return G

def delete_old_edges(G, old_edges):
    for oe in old_edges:
        s = oe[0]
        e = oe[1]
        k = oe[2]
        if G.has_edge(s, e, k):
    #         print("Removing: {}".format(oe))
            G.remove_edge(s, e, k)
        
    return G

def restoring_missed_boundaries(G, rsu_arr, grid_df):
    total = len(rsu_arr) - 1
    crossing_edges = []
    for count, rsu in enumerate(rsu_arr):
        rpoly = rsu.poly
        grid_id = rsu.grid_id
        sub_df = grid_df.loc[grid_df['grid_id'] == grid_id]
        edge_counter = 0
        for u, v, a in G.edges(data=True):
            if 'tmc_id' not in a:
                continue
            tmc_id = a['tmc_id']
            found = False
            if tmc_id in sub_df['tmc_ids'].values.tolist()[0]:
                edge = a['geometry']

                top    = LineString(rpoly.boundary.coords[0:2])
                right  = LineString(rpoly.boundary.coords[1:3])
                bottom = LineString(rpoly.boundary.coords[2:4])
                left   = LineString([rpoly.boundary.coords[3], rpoly.boundary.coords[0]])
                lines = [top, right, bottom, left]
                line_labels = ['north', 'east', 'south', 'west']

                boundaries = []
                for i, line in enumerate(lines):
                    if edge.crosses(line):
                        if edge not in crossing_edges:
                            crossing_edges.append(edge)
                        else:
                            continue
                        nodeu = node_to_point(G, u)
                        nodev = node_to_point(G, v)

                        if nodeu.equals(nodev):                            
                                ngrid = rsu.get_neighbors(rsu_arr)[line_labels[i]].grid_id
                                node = G.node[u]
                                node.pop('grid_id', None)
                                node['is_bounds'] = True
                                if 'boundaries' in node:
                                    if ngrid not in node['boundaries']:
                                        node['boundaries'].append(ngrid)
                                else:
                                    node['boundaries'] = [ngrid]

                                node = G.node[v]
                                node.pop('grid_id', None)
                                node['is_bounds'] = True
                                if 'boundaries' in node:
                                    if ngrid not in node['boundaries']:
                                        node['boundaries'].append(ngrid)
                                else:
                                    node['boundaries'] = [ngrid]

                                found = True
                                edge_counter += 1

            if found:
                node = G.node[u]
                bs = node['boundaries']
                bs.append(grid_id)
                node['boundaries'] = list(set(bs))

                node = G.node[v]
                bs = node['boundaries']
                bs.append(grid_id)
                node['boundaries'] = list(set(bs))

        if edge_counter > 0:
            print("{}/{} \t {} \t modified \t {} edges".format(count, total, grid_id, edge_counter))
            
    return G
    