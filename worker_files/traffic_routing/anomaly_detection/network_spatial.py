from fastparquet import ParquetFile
import geopandas as gp
import pandas as pd
import numpy as np
import math
import json
from copy import deepcopy
from shapely.geometry import *
from shapely.ops import split, linemerge
from shapely.ops import cascaded_union
import osmnx as ox
import time
import networkx as nx
from math import radians, sin, cos, sqrt, asin
import time


def get_tmc_graph_part_one(tmc_df):
    """
    Convert TMC Paths to a directed graph.
    This step goes through each path and creates Nodes at each path end and intersection.
    :param tmc_df: GeoPandas Dataframe of TMC Paths ('geometry', 'tmc_id')
    :return: N: dictionary of Nodes, E: dictionary of Edges
    """
    start_time = time.time()

    # set up empty N for Nodes
    N = {'y': [],
         'x': [],
         'index': [],
         'geometry': []}

    # set up empty E for edges
    E = {'start_node': [],
         'end_node': [],
         'geometry': [],
         'tmc_id': []}

    test = tmc_df  # need a copy of the dataframe
    node_num = 0  # track the node number we are on

    counter = 0

    # iterate through all paths
    for x, d_x in tmc_df.iterrows():
        counter += 1
        if (counter % 500) == 0:
            #print(counter)
            elapsed_time = time.time() - start_time
            #print(elapsed_time)

        # x_paths is a list of LineStrings, will split at intersections
        x_paths = [d_x['geometry']]

        # iterate through all paths and check for intersections with paths in x_paths
        for y, d_y in test.iterrows():
            for i in range(len(x_paths)):
                z = x_paths[i] # z is current path we are checking for intersections

                # check for intersection
                if (z.intersects(d_y['geometry'])) & (x != y):
                    splitter = z.intersection(d_y['geometry']) # splitter is intersection Point
                    if splitter.geom_type == 'Point':
                        try:
                            geom_temp = split(z, splitter) # geom_temp list of LineStrings after split
                            x_split = [geom_temp[zz] for zz in range(len(geom_temp))]

                            # now update x_paths by replacing current path with split paths
                            x_new = []
                            for j in range(len(x_paths)):
                                if j == i:
                                    for m in x_split:
                                        x_new.append(m)
                                else:
                                    x_new.append(x_paths[j])
                            x_paths = x_new
                            break
                        except:
                            print("Split did not work at ", x, " ", y)
                            print(z)
                            print(d_y['geometry'])

        # now we have the list of stored paths in x_paths
        # now add nodes and edges to N and E
        for a in x_paths:
            # start and end nodes will be first and last coordinates of linestring in x_paths
            start_node = list(a.coords)[0]
            end_node = list(a.coords)[-1]

            # add start node to N
            N['y'].append(start_node[1])
            N['x'].append(start_node[0])
            N['index'].append(node_num)
            N['geometry'].append(Point(start_node))
            node_num += 1

            # add end_node to N
            N['y'].append(end_node[1])
            N['x'].append(end_node[0])
            N['index'].append(node_num)
            N['geometry'].append(Point(end_node))

            # add edge between start_node and end_node to E
            E['start_node'].append(node_num - 1)
            E['end_node'].append(node_num)
            E['geometry'].append(a)
            E['tmc_id'].append(d_x['tmc_id'])
            node_num += 1

    print("done with Step One of graph creation")

    return N, E


def get_tmc_graph_part_two(N, E, buf, unit):
    '''
    Part two of graph creation. Here we take N and E from get_tmc_graph_part_one
    and generate the final NetworkX Graph.
    :param N: dictionary of Nodes (from get_tmc_graph_part_one)
    :param E: dictionary of Edges (from get_tmc_graph_part_two)
    :param buf: float, length of buffer, should be of type unit
    :param unit: units, either meters, kilometers, feet or miles
    :return: NetworkX MultiDiGraph (directed graph) with edge attributes of tmc_id, length, and geometry
    '''

    # turn N into geodataframe
    N_df = gp.GeoDataFrame(N)
    lat_c, lon_c = length_to_deg(buf, unit, lat=N['y'][0], lon=N['x'][0])
    buf = (lat_c + lon_c) / 2
    buffered_nodes = N_df.buffer(buf).unary_union # turn N points into circles with radius buf
    if isinstance(buffered_nodes, Polygon):
        # if only a single node results, make it iterable so we can turn it into a GeoSeries
        buffered_nodes = [buffered_nodes]

    # get the centroids of the merged intersection polygons
    unified_intersections = gp.GeoSeries(list(buffered_nodes))
    intersection_centroids = unified_intersections.centroid

    # nodes of final graph will be intersection_centroids
    G = nx.MultiDiGraph()
    node_num = 0
    for point in intersection_centroids:
        G.add_node(node_num, x=point.x, y=point.y)
        node_num += 1

    # Now add edges to G
    E_df = gp.GeoDataFrame(E)
    for row, data in E_df.iterrows():
        length = get_line_length(data['geometry'], unit=unit)
        initial_speed=1 # this is the default initial speed
        start = ox.get_nearest_node(G, (N_df.loc[data['start_node']]['y'], N_df.loc[data['start_node']]['x']))
        end = ox.get_nearest_node(G, (N_df.loc[data['end_node']]['y'], N_df.loc[data['end_node']]['x']))
        G.add_edge(start, end, tmc_id=data['tmc_id'], length=length, geometry=data['geometry'])
    G.graph['crs'] = {'init': 'epsg:4326'}
    G.graph['name'] = 'unnamed'

    return G


def haversine(lon1, lat1, lon2, lat2, unit='miles'):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    # Radius of earth in kilometers = 6371.
    # Radius of earth in miles = 3956.
    if unit == 'miles':
        r = 3956
    elif unit == 'kilometers':
        r = 6371
    elif unit == 'meters':
        r = 6371 * 1000
    elif unit == 'feet':
        r = 6371 * 3280.84
    return c * r


def get_line_length(line, unit='miles'):
    """
    Returns the length of a LineString
    :param line: LineString
    :return: length in unit as specified
    """
    numCoords = len(line.coords) - 1
    distance = 0
    for i in range(0, numCoords):
        point1 = line.coords[i]
        point2 = line.coords[i + 1]
        distance += haversine(point1[0], point1[1], point2[0], point2[1], unit=unit)
    return distance


def get_tmc_ids(tmc_json):
    """
    Return HERE API ID's
    :param tmc_json: dictionary, keys must be tmc ids
    :return: list
    """
    tmc_ids = []
    for key, val in tmc_json.items():
        tmc_ids.append(key)
    return tmc_ids


def length_to_deg(length, unit, lat=36.3, lon=-86.7):
    """
    Convert length to degrees
    :param length: float
    :param unit: 'feet', 'meters', 'miles', or 'kilometers'
    :param lat: a latitude of a point in the city, example 36.3 (nashville)
    :param lon: a longitude of a point in the city, example -86.7 (nashville)
    :return: lat, long of that length in degrees
    """

    # get the length of one degree latitude and one degree longitude
    lat_deg = haversine(lon, lat, lon, lat + 1, unit=unit)
    lon_deg = haversine(lon, lat, lon + 1, lat, unit=unit)

    # convert length to degrees
    lat = length / lat_deg
    lon = length / lon_deg
    return lat, lon


def deg_to_length(deg, unit, lat=36.3, lon=-86.7):
    # get the length of one degree latitude and one degree longitude
    lat_deg = haversine(lon, lat, lon, lat + 1, unit=unit)
    lon_deg = haversine(lon, lat, lon + 1, lat, unit=unit)

    # convert length to degrees
    length = deg * ((lat_deg + lon_deg)/2)
    return length


def get_bounding_box(bound_poly):
    """
    Get the bounding box from a Polygon
    :param bound_poly: Polygon
    :return: Polygon
    """
    b = bound_poly.bounds
    return Polygon([(b[0], b[1]),
                    (b[0], b[3]),
                    (b[2], b[3]),
                    (b[2], b[1])])


def clean_network_graph(G, sensor_to_rsu_map):
    G_two = G.copy()
    r = list(G_two.selfloop_edges(keys=True))
    G_two.remove_edges_from(r)

    rr = []
    for start, stop, k, vv in G_two.edges(data=True, keys=True):
        tmc_id = vv['tmc_id']
        if tmc_id not in list(sensor_to_rsu_map.keys()):
            rr.append((start, stop, k))
    print(len(rr))
    print(len(r))
    G_two.remove_edges_from(rr)
    return G_two



