import geopandas as gp
import pandas as pd
from shapely.geometry import *
from math import radians, sin, cos, sqrt, asin
from collections import OrderedDict
import multiprocessing as mp
#import pathos.multiprocessing as mp
import numpy as np
from scipy.spatial import distance_matrix
import geopandas as gpd
import os
import logging
from copy import deepcopy
from src.anomaly_detection import save_load
from src.anomaly_detection.RSU import RSU
from src.anomaly_detection.data_processing import *


class GreedyClustering:
    """
    Class for generating the cluster RSU network outlined in Section VI of paper.
    """

    def __init__(self,
                 config=None):
        """
        Initialize the clustering object.

        :param config: dictionary 
        (dictionary should contain 'tmc_gdf', 'speeds', 'norm', 'time_window', 
        'num_rsus', 'clust_max_sensors')

        :return None
        """
        
        self.speeds = config['speeds']
        self.tmc_gdf = config['tmc_gdf']
        self.norm = False
        self.time_window = config['time_window']
        self.parallel = config['parallel']
        self.clust_max_sensors = config['clust_max_sensors']
        self.max_num_clusters = config['num_rsus']
        self.days_of_week = self.speeds.index.dayofweek.unique().tolist()

        self.rsu_network, self.feature_matrix = self.init_clusters(self.tmc_gdf,
                                                                   self.speeds)
        self.nearest_clusters = self.init_nearest_clusters()
    
    def fit(self):
        """
        This method will generate the cluster RSU network.

        return dictionary
        """
        
        while len(self.feature_matrix) > self.max_num_clusters:
            rsu_one, rsu_two, cont, similarity = self.get_clusters_to_merge()
            if cont == False:
                print("same number of clusters after update step, stop optimization")
                break
            else:
                self.rsu_network, self.feature_matrix, self.nearest_clusters = self.update_step(rsu_one, rsu_two)
                print("length of rsu_network: {}".format(len(list(self.rsu_network.keys()))))
                print("length of feature_matrix: {}".format(len(self.feature_matrix)))
                print("length of nearest_clusters: {}".format(len(self.nearest_clusters)))
                print("loss: {}".format(similarity))
        return self.rsu_network
    
    def init_clusters(self,
                      tmc_gdf,
                      speeds):
        """
        Initialize clusters. Each sensor starts as its own cluster.

        :param tmc_gdf: geopandas dataframe
        :param speeds: pandas dataframe

        :return: dictionary, pandas dataframe
        """

        rsus = []

        sensors = speeds['tmc_id'].unique().tolist()
        init_args = {'training_speeds': [],
                     'sensor_shape_gdf': [],
                     'rsu_id': [],
                     'sensor_list': [],
                     'boundary_shape': [],
                     'location_type': 'clustering',
                     'norm': self.norm}
        i = 0

        # each unique sensor will be its own RSU to start clustering
        for sensor in sensors:
            tmc_gdf_i = tmc_gdf[tmc_gdf['tmc_id'] == sensor]
            speeds_i = speeds[speeds['tmc_id'] == sensor]

            init_args['training_speeds'].append(speeds_i)
            init_args['sensor_shape_gdf'].append(tmc_gdf_i)
            init_args['rsu_id'].append("rsu_{}".format(i))
            init_args['sensor_list'].append([sensor])
            init_args['boundary_shape'].append(None)
            i += 1

        if self.parallel is False:
            child_initializer(init_args)
            for r in range(len(sensors)):
                print(r)
                rsu = init_parallel_rsu(r)
                rsus.append(rsu)

        elif self.parallel is True:
            print("starting parallel")
            os.system("taskset -p 0xff %d" % os.getpid())
            processes = mp.cpu_count()
            if processes > 6:
                processes = 6
            with mp.Pool(processes=processes, initializer=child_initializer, initargs=(init_args,)) as pool:
                rsus = pool.map(init_parallel_rsu, range(len(sensors)))

        del init_args
        print("done with creating RSU network")
        print("starting feature matrix")
        feature_matrix = []
        rsu_list = []
        rsu_network = {}
        for rsu in rsus:
            feature_matrix.append(self.get_feature(rsu))
            rsu_list.append(rsu.rsu_id)
            rsu_network[rsu.rsu_id] = rsu
        return rsu_network, pd.DataFrame(feature_matrix, index=rsu_list)
    
    def update_step(self, rsu_one, rsu_two):
        # get new combine cluster
        new_cluster = self.combine_clusters(rsu_one, rsu_two)

        # remove old clusters from feature_matrix, nearest_clusters and rsu_network
        self.feature_matrix.drop([rsu_one, rsu_two], inplace=True)

        self.nearest_clusters.drop([rsu_one, rsu_two], inplace=True)

        self.rsu_network.pop(rsu_one)
        self.rsu_network.pop(rsu_two)

        # add new cluster to feature_matrix, nearest_clusters and rsu_network
        self.rsu_network[new_cluster.rsu_id] = new_cluster

        new_feature = self.get_feature(new_cluster)
        new_feature_row = pd.DataFrame([new_feature], index=[new_cluster.rsu_id])
        new_feature_matrix = self.feature_matrix.append(new_feature_row)

        new_nearest_clusters = self.init_nearest_clusters()
        return self.rsu_network, new_feature_matrix, new_nearest_clusters

    def init_nearest_clusters(self):
        rsu_list = []
        centroid_list = []
        for k, v in self.rsu_network.items():
            rsu_list.append(k)
            centroid_list.append(v.rsu_location)
        result = gpd.GeoDataFrame(data={'geometry': centroid_list}, index=rsu_list)
        result = self.add_nearest_clusters(result)
        return result

    def add_nearest_clusters(self, nearest_sensors_x):
        results = deepcopy(nearest_sensors_x)
        results['nearest_rsu'] = [self.get_nearest_cluster(results, x) for x in results.index.tolist()]
        return results

    def get_nearest_cluster(self, results, cluster_index):
        cluster_geom = results.loc[cluster_index, 'geometry']
        distances = [cluster_geom.distance(x) for x in results['geometry'].values.tolist()]
        dist_inds = np.argsort(np.array(distances))
        result = None
        cluster_index_i = results.index.get_loc(cluster_index)
        for i in dist_inds:
            if i != cluster_index_i:
                result = results.index[i]
                break
        return result

    def combine_clusters(self, cluster_one, cluster_two):
        result = RSU(init_sensor_data_map=False, days_of_week=self.days_of_week)
        result.merge_RSU(self.rsu_network[cluster_one], self.rsu_network[cluster_two])
        return result

    def get_clusters_to_merge(self):
        similarities = self.get_similarities()
        sorted_index = np.argsort(np.array(similarities)).tolist()
        rsu_one, rsu_two, similarity = None, None, None
        for x in sorted_index:
            rsu_one_temp = self.nearest_clusters.index[x]
            rsu_two_temp = self.nearest_clusters.loc[rsu_one_temp, 'nearest_rsu']
            test_max = self.check_max_cluster_constraint(rsu_one_temp, rsu_two_temp)
            if test_max is True:
                rsu_one, rsu_two = rsu_one_temp, rsu_two_temp
                similarity = similarities[x]
                break
        return rsu_one, rsu_two, test_max, similarity

    def check_max_cluster_constraint(self, rsu_one, rsu_two):
        if ((len(self.rsu_network[rsu_one].sensor_list) + len(self.rsu_network[rsu_two].sensor_list)) <= self.clust_max_sensors):
            return True
        else:
            return False

    def get_similarities(self):
        similarities = []
        for i in range(len(self.nearest_clusters)):
            rsu_o = self.nearest_clusters.index[i]
            rsu_t = self.nearest_clusters.loc[rsu_o, 'nearest_rsu']
            similarities.append(self.get_feature_dist(rsu_o, rsu_t))
        return similarities

    def get_feature_dist(self, rsu_o, rsu_t):
        feature_one = self.feature_matrix.loc[rsu_o].values
        feature_two = self.feature_matrix.loc[rsu_t].values
        return np.linalg.norm(feature_one-feature_two)

    def get_feature(self,
                    rsu):
        rsu_result = []
        for window, data in rsu.hist_meta['aggregate'].items():
            cur_time = time_from_time_window(float(window), rsu.num_daily_windows)
            if (cur_time.hour >= 7) & (cur_time.hour < 19):
                cur_window = nearest_non_nan_index(rsu.hist_meta['aggregate'], window)
                if self.norm is True:
                    rsu_result.append(rsu.hist_meta['aggregate'][str(cur_window)]['u_norm'])
                else:
                    rsu_result.append(rsu.hist_meta['aggregate'][str(cur_window)]['u'])
        return rsu_result
        

### helper functions for cluster RSU generation ###

def child_initializer_feature_matrix(_rsus):
    global RSUS
    RSUS = _rsus

def get_feature(i):
    rsu = RSUS[i]
    rsu_result = []
    for window, data in rsu.hist_meta['aggregate'].items():
        cur_time = time_from_time_window(float(window), rsu.num_daily_windows)
        if (cur_time.hour >= 7) & (cur_time.hour < 20):
            cur_window = nearest_non_nan_index(rsu.hist_meta['aggregate'], window)
            rsu_result.append(rsu.hist_meta['aggregate'][str(cur_window)]['u'])
    return rsu_result

def init_parallel_rsu(i):
    logging.info("i should be a child, my PID is {}".format(os.getpid()))
    rsu = RSU(training_speeds=init_args['training_speeds'][i],
              sensor_shape_gdf=init_args['sensor_shape_gdf'][i],
              rsu_id=init_args['rsu_id'][i],
              sensor_list=init_args['sensor_list'][i])
    rsu.set_rsu_location(location_type='clustering')
    rsu.init_hist_meta()
    if init_args['boundary_shape'][i] is not None:
        rsu.set_rsu_location(location_type=init_args['location_type'],
                             boundary=init_args['boundary_shape'][i])
    return rsu

def child_initializer(_init_args):
    global init_args
    init_args = _init_args