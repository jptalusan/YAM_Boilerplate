import geopandas as gp
import pandas as pd
from shapely.geometry import *
from math import radians, sin, cos, sqrt, asin
from collections import OrderedDict
import multiprocessing as mp
#import pathos.multiprocessing as mp
import numpy as np
from scipy.spatial import distance_matrix
import os
import logging
from copy import deepcopy
from src.anomaly_detection import save_load
from src.anomaly_detection.RSU import RSU
from src.anomaly_detection.data_processing import *


### grid layout ###



def rsu_grid(config=None):
    """
    This method will generate the grid rsu network used in Sections VI and VII-C
    of the paper.

    :param config: dictionary object (must include boundary, tmc_gdf, speeds, num_rsus)

    :return dictionary
    """

    boundary, tmc_gdf, speeds, num_rsus = config['boundary'], config['tmc_gdf'], config['speeds'], config['num_rsus']
    
    grid_shapes = rsu_grid_locations(boundary, num_rsus)
    result = OrderedDict()

    init_args = {'training_speeds': [],
                 'sensor_shape_gdf': [],
                 'rsu_id': [],
                 'sensor_list': [],
                 'boundary_shape': [],
                 'location_type': 'grid'}
    i = 0
    for k, v in grid_shapes.iterrows():
        tmc_gdf_i = tmc_gdf[tmc_gdf.intersects(v['geometry'])]
        sensors_i = tmc_gdf_i['tmc_id'].unique().tolist()
        speeds_i = speeds[speeds['tmc_id'].isin(sensors_i)]

        init_args['training_speeds'].append(speeds_i)
        init_args['sensor_shape_gdf'].append(tmc_gdf_i)
        init_args['rsu_id'].append("rsu_{}".format(i))
        init_args['sensor_list'].append(sensors_i)
        init_args['boundary_shape'].append(v['geometry'])

        i += 1

    os.system("taskset -p 0xff %d" % os.getpid())
    processes = mp.cpu_count()
    if processes > 6:
        processes = 6
    with mp.Pool(processes=processes, initializer=child_initializer, initargs=(init_args,)) as pool:
        rsus = pool.map(init_rsu, range(num_rsus))
    for rsu in rsus:
        result[rsu.rsu_id] = rsu
    del init_args
    return result

### helper functions for grid RSU generation ###

def child_initializer(_init_args):
    global init_args
    init_args = _init_args


def init_rsu(i):
    """
    initializes a single RSU. Used for parallel initialization of RSUs.

    :param i: int
    
    :return RSU object
    """
    rsu = RSU(training_speeds=init_args['training_speeds'][i],
              sensor_shape_gdf=init_args['sensor_shape_gdf'][i],
              rsu_id=init_args['rsu_id'][i],
              sensor_list=init_args['sensor_list'][i])

    rsu.init_hist_meta()
    if init_args['boundary_shape'][i] != None:
        rsu.set_rsu_location(location_type=init_args['location_type'],
                             boundary=init_args['boundary_shape'][i])
    return rsu


def rsu_grid_locations(boundary, num_rsus):
    """
    Helper function for rsu_grid.

    :param boundary: shaply polygon
    :param num_rsus: int

    :return geopandas dataframe
    """

    grid_span = sqrt(num_rsus)
    try:
        grid_span = int(grid_span)
    except:
        raise Exception("num_rsus must be a square number")

    minx, miny, maxx, maxy = boundary.bounds

    x_delta = (maxx - minx) / float(grid_span)
    y_delta = (maxy - miny) / float(grid_span)

    result = {'geometry': []}
    x_left, y_bott = minx, miny

    buf_x, buf_y = length_to_deg(10, 'feet')
    while x_left < maxx - buf_x:
        x_right = x_left + x_delta
        while y_bott < maxy - buf_y:
            y_top = y_bott + y_delta
            temp = Polygon([(x_left, y_bott),
                            (x_left, y_top),
                            (x_right, y_top),
                            (x_right, y_bott)])
            result['geometry'].append(temp)
            y_bott = y_top
        y_bott = miny
        x_left = x_right

    l = len(result['geometry'])
    result['zone_id'] = range(l)
    result['centroid'] = [x.centroid for x in result['geometry']]
    return gp.GeoDataFrame(result)


def add_norm_speeds(training_speeds, sensor_data_map):
    training_speeds_t = deepcopy(training_speeds)
    training_speeds_t['SU_norm'] = np.nan
    sensor_list = training_speeds_t['tmc_id'].unique().tolist()
    for sensor in sensor_list:
        locs = training_speeds_t[training_speeds_t['tmc_id'] == sensor]
        y = [((x + 1 - sensor_data_map[sensor].lower) / sensor_data_map[sensor].range) for x in locs['SU'].values.tolist()]
        training_speeds_t.loc[locs.index, 'SU_norm'] = y
    return training_speeds_t


### Clustering ###





