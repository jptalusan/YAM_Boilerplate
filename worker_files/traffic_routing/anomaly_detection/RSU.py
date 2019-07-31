from collections import OrderedDict
import pandas as pd
import numpy as np
import multiprocessing as mp
import scipy.stats as stats
from copy import deepcopy
from src.anomaly_detection import save_load
import os
from shapely.ops import cascaded_union
from shapely.geometry import *
import random
import time

from src.anomaly_detection.data_processing import time_window_from_time, nearest_non_nan_index, get_number_of_time_windows
from src.anomaly_detection.Sensor import Sensor
from src.anomaly_detection.data_processing import get_sensor_geometry


class RSU:
    def __init__(self,
                 training_speeds=None,
                 sensor_shape_gdf=None,
                 rsu_id=None,
                 sensor_list=None,
                 init_sensor_data_map=True,
                 days_of_week=None):
        os.system("taskset -p 0xff %d" % os.getpid())

        self.training_speeds = deepcopy(training_speeds)
        self.sensor_shape_gdf = sensor_shape_gdf
        self.rsu_id = rsu_id
        self.sensor_list = sensor_list

        if init_sensor_data_map == True:
            self.sensor_data_map = self.init_sensor_data_map()
        else:
            self.sensor_data_map = None

        params = save_load.get_simulation_parameters()
        if type(params['time_window']) == str:
            self.time_delta = pd.Timedelta(params['time_window'])
        else:
            self.time_delta = pd.Timedelta("{}m".format(params['time_window']))

        if days_of_week == None:
            self.days_of_week = self.training_speeds.index.dayofweek.unique().tolist()
        else:
            self.days_of_week = days_of_week
        self.num_daily_windows = get_number_of_time_windows(params['time_window'])
        #self.train_sensors = params['train_sensors']

        self.testing_speeds = None
        self.rsu_location = None
        self.hist_meta = None

    def add_norm_speeds(self):
        self.training_speeds = deepcopy(self.training_speeds)
        self.training_speeds['SU_norm'] = np.nan
        for sensor in self.sensor_list:
            locs = self.training_speeds[self.training_speeds['tmc_id'] == sensor]
            y = [((x + 1 - self.sensor_data_map[sensor].lower) / self.sensor_data_map[sensor].range) for x in
                 locs['SU'].values.tolist()]
            self.training_speeds.loc[locs.index, 'SU_norm'] = y

    def init_sensor_data_map(self):
        result = OrderedDict()
        for sensor in self.sensor_list:
            sensor_speeds_i = self.training_speeds[self.training_speeds['tmc_id'] == sensor]

            sensor_shape_i = get_sensor_geometry(self.sensor_shape_gdf,
                                                 sensor)

            result[sensor] = Sensor(training_speeds=sensor_speeds_i,
                                    sensor_shape=sensor_shape_i,
                                    sensor_id=sensor)
        return result

    def init_hist_meta(self,
                       exists=False):
        """
        will set self.hist_meta

        hist_meta structure
        {
            'u': float
            'std': float
            '0'...'num_days': {
                                '0'..'num_windows': {
                                                        'u': float
                                                        'std': float
                                                        'u_mean': float
                                                        'std_mean': float
                                                        'Q': float
                                                        'Q_std': float
                                                        'Q_box': float
                                                        'Q_box_std': float
                                                    }
                            }
            'aggregate': {
                                '0'...'num_windows': {
                                                        'u': float
                                                        'std': float
                                                        'u_mean': float
                                                        'std_mean': float
                                                        'Q': float
                                                        'Q_std': float
                                                        'Q_box': float
                                                        'Q_box_std': float
                                                        'speeds': pandas dataframe
                                                    }
                        }
        }


        :param time_delta: int
        :param days_of_week: list of ints
        :param train_sensors: bool
        :return: None, sets self.hist_meta
        """
        if exists == False:
            self.training_speeds = deepcopy(self.training_speeds)
            self.training_speeds['SU_norm'] = np.nan
            for sensor in self.sensor_list:
                self.sensor_data_map[sensor].init_hist_meta()
                locs = self.training_speeds[self.training_speeds['tmc_id'] == sensor]
                y = [((x + 1 - self.sensor_data_map[sensor].lower) / self.sensor_data_map[sensor].range) for x in locs['SU'].values.tolist()]
                self.training_speeds.loc[locs.index, 'SU_norm'] = y
        result = {'aggregate': OrderedDict()}

        if len(self.training_speeds) >= 2:
            result['u'] = np.mean(self.training_speeds['SU'].values)
            result['std'] = np.std(self.training_speeds['SU'].values)
        else:
            result['u'] = None
            result['std'] = None



        first_time, last_time = pd.to_datetime("2018-04-01 00:00:00"), pd.to_datetime("2018-04-01 23:59:59")
        start_time = deepcopy(first_time)
        window = 0
        while start_time < last_time:
            end_time = start_time + self.time_delta
            window_speeds = self.training_speeds.between_time(start_time.time(), end_time.time())
            u_window, u_window_norm, Q_window, Q_box_window, Q_window_norm, Q_box_window_norm = [], [], [], [], [], []
            for day in self.days_of_week:
                u_window_day, Q_window_day, Q_box_window_day, Q_window_day_norm, Q_box_window_day_norm = [], [], [], [], []
                daily_window_speeds = window_speeds[window_speeds.index.dayofweek == day]
                for date in np.unique(daily_window_speeds.index.date).tolist():
                    daily_window_speeds_day = daily_window_speeds[daily_window_speeds.index.date == date]['SU'].values
                    daily_window_speeds_day_norm = daily_window_speeds[daily_window_speeds.index.date == date]['SU_norm'].values
                    if len(daily_window_speeds_day) >= 2:
                        u_window.append(np.mean(daily_window_speeds_day))
                        u_window_day.append(np.mean(daily_window_speeds_day))
                        Q_window.append(self.get_Q(daily_window_speeds_day, boxcox=False))
                        Q_window_day.append(self.get_Q(daily_window_speeds_day, boxcox=False))
                        Q_window_day_norm.append(self.get_Q(daily_window_speeds_day_norm, boxcox=False))
                        Q_window_norm.append(self.get_Q(daily_window_speeds_day_norm, boxcox=False))

                        test1 = self.get_Q(daily_window_speeds_day, boxcox=False)
                        test2 = self.get_Q(daily_window_speeds_day_norm, boxcox=False)
                        if (test1 is None) or (test2 is None):
                            continue
                        else:
                            Q_box_window.append(test1)
                            Q_box_window_day.append(test1)
                            Q_box_window_day_norm.append(test2)
                            Q_box_window_norm.append(test2)
                            u_window_norm.append(np.mean(daily_window_speeds_day_norm))

                if str(day) not in result.keys():
                    result[str(day)] = OrderedDict()
                if len(Q_box_window_day) >= 2:
                    result[str(day)][str(window)] = {'u': np.mean(daily_window_speeds['SU'].values),
                                                     'std': np.std(daily_window_speeds['SU'].values),
                                                     'u_mean': np.mean(u_window_day),
                                                     'std_mean': np.std(u_window_day),
                                                     'Q': np.mean(Q_window_day),
                                                     'Q_std': np.std(Q_window_day),
                                                     'Q_box': np.mean(Q_box_window_day),
                                                     'Q_box_std': np.std(Q_box_window_day)}

                else:
                    result[str(day)][str(window)] = None

            if len(Q_box_window) >= 2:
                result['aggregate'][str(window)] = {'u': np.mean(window_speeds['SU'].values),
                                                    'u_median': np.percentile(window_speeds['SU'].values, 50),
                                                    'std': np.std(window_speeds['SU'].values),
                                                    'u_upper': np.percentile(window_speeds['SU'].values, 75),
                                                    'u_lower': np.percentile(window_speeds['SU'].values, 25),
                                                    'u_norm_window': np.mean(np.mean(u_window_norm)),
                                                    'u_norm_window_median': np.percentile(u_window_norm, 50),
                                                    'u_norm_window_std': np.std(u_window_norm),
                                                    'u_norm_window_upper': np.percentile(u_window_norm, 75),
                                                    'u_norm_window_lower': np.percentile(u_window_norm, 25),
                                                    'u_norm': np.mean(window_speeds['SU_norm'].values),
                                                    'u_norm_std': np.std(window_speeds['SU_norm'].values),
                                                    'u_norm_upper': np.percentile(window_speeds['SU_norm'].values, 75),
                                                    'u_norm_lower': np.percentile(window_speeds['SU_norm'].values, 25),
                                                    'Q_norm': np.mean(Q_window_norm),
                                                    'Q_norm_median': np.percentile(Q_window_norm, 50),
                                                    'Q_norm_std': np.std(Q_window_norm),
                                                    'u_windows': np.mean(u_window),
                                                    'std_windows': np.std(u_window),
                                                    'u_windows_median': np.percentile(u_window, 50),
                                                    'u_windows_upper': np.percentile(u_window, 75),
                                                    'u_windows_lower': np.percentile(u_window, 25),
                                                    'Q': np.mean(Q_window),
                                                    'Q_median': np.percentile(Q_window, 50),
                                                    'Q_std': np.std(Q_window),
                                                    'Q_box': np.mean(Q_box_window),
                                                    'Q_box_std': np.std(Q_box_window),
                                                    'Q_box_norm': np.mean(Q_box_window_norm),
                                                    'Q_box_norm_std': np.std(Q_box_window_norm),
                                                    'Q_upper': np.percentile(Q_window, 75),
                                                    'Q_lower': np.percentile(Q_window, 25),
                                                    'Q_norm_upper': np.percentile(Q_window_norm, 75),
                                                    'Q_norm_lower': np.percentile(Q_window_norm, 25),
                                                    'start_time': start_time,
                                                    'end_time': end_time,
                                                    'speeds': window_speeds}
            else:
                result['aggregate'][str(window)] = None
            start_time = end_time
            window += 1
        self.hist_meta = result

    def merge_RSU(self, rsu_one, rsu_two):
        self.training_speeds = pd.concat([rsu_one.training_speeds, rsu_two.training_speeds])
        self.sensor_shape_gdf = pd.concat([rsu_one.sensor_shape_gdf, rsu_two.sensor_shape_gdf])
        self.sensor_list = rsu_one.sensor_list + rsu_two.sensor_list
        self.rsu_id = rsu_one.rsu_id
        self.time_delta = rsu_one.time_delta
        self.days_of_week = rsu_one.days_of_week
        self.num_daily_windows = rsu_one.num_daily_windows
        #self.train_sensors = rsu_one.train_sensors

        # update sensors
        self.sensor_data_map = rsu_one.sensor_data_map
        for sensor, data in rsu_two.sensor_data_map.items():
            if sensor in self.sensor_data_map.keys():
                continue
            else:
                self.sensor_data_map[sensor] = data

        # update hist_meta
        self.init_hist_meta(exists=True)

        # update rsu_location
        self.set_rsu_location(location_type='clustering')

    def get_Q(self, speeds=None, boxcox=False):
        """
        will calculate Q value
        :param speeds: numpy array or listlike
        :return: float
        """
        if boxcox == True:
            speeds_temp, lmbda = stats.boxcox(speeds)
            speeds_temp = speeds_temp + 100
        else:
            speeds_temp = speeds
        if len(speeds_temp) == 0:
            return None
        elif np.min(speeds_temp) <= 0:
            return None
        else:
            hm = stats.hmean(speeds_temp)
            am = np.mean(speeds_temp)
            Q = hm/am
            return Q

    def set_rsu_location(self, location_type='clustering', boundary=None):
        if location_type == 'clustering':
            #print([self.sensor_shape_gdf.at[tmc_id, "geometry"] for tmc_id in self.sensor_shape_gdf.index.values.tolist()])
            shp_list = [asShape(self.sensor_shape_gdf.at[tmc_id, "geometry"]) for tmc_id in self.sensor_shape_gdf.index.values.tolist()]
            shp = cascaded_union(shp_list)
            self.rsu_location = shp.centroid
        else:
            self.rsu_location = boundary.centroid

    def testing_speeds_to_sensors(self):
        for sensor in self.sensor_data_map.keys():
            self.sensor_data_map[sensor].testing_speeds = self.testing_speeds[self.testing_speeds['tmc_id'] == sensor]

    def predict_anomaly_no_attack(self,
                                  epsilon,
                                  start_time,
                                  end_time,
                                  return_comp_time=False,
                                  norm=False):
        cur_df = self.testing_speeds[start_time:end_time]
        if len(cur_df) == 0:
            prediction, Q_r, speeds_r, comp_time = None, 0, 0, 0
        else:
            comp_start_time = time.time()
            if norm == True:
                Q_cur = self.get_Q(speeds=cur_df['SU_norm'].values,
                                boxcox=False)
            else:
                Q_cur = self.get_Q(speeds=cur_df['SU'].values,
                                boxcox=False)

            current_time_window = time_window_from_time(start_time, self.num_daily_windows)
            current_time_window = nearest_non_nan_index(self.hist_meta['aggregate'], current_time_window)

            if norm == True:
                Q_hist_mean = self.hist_meta['aggregate'][current_time_window]['Q_norm']
                Q_hist_std = self.hist_meta['aggregate'][current_time_window]['Q_norm_std']
            else:
                Q_hist_mean = self.hist_meta['aggregate'][current_time_window]['Q']
                Q_hist_std = self.hist_meta['aggregate'][current_time_window]['Q_std']

            prediction = self.detect_zone_anomaly(Q_cur, Q_hist_mean, Q_hist_std, epsilon)
            Q_r = Q_cur

            if norm == True:
                speeds_r = cur_df['SU_norm'].values
            else:
                speeds_r = cur_df['SU'].values

            comp_time = time.time() - comp_start_time

        if return_comp_time == True:
            return prediction, Q_r, speeds_r, comp_time
        else:
            return prediction, Q_r, speeds_r

    def predict_anomaly_attacked(self,
                                 epsilon,
                                 start_time,
                                 end_time,
                                 per_attacked,
                                 delta,
                                 camo,
                                 attacked_ids=None,
                                 return_comp_time=False):
        cur_df = self.testing_speeds[start_time:end_time]
        if len(cur_df) == 0:
            prediction, Q_r, speeds_r, comp_time = None, 0, 0, 0
        else:
            current_time_window = time_window_from_time(start_time, self.num_daily_windows)

            if attacked_ids == None:
                attacked_ids = self.get_attacked_ids(per_attacked)

            cur_speeds = self.attack_speeds(cur_df, current_time_window, per_attacked, delta, camo, attacked_ids)

            comp_start_time = time.time()
            Q_cur = self.get_Q(cur_speeds,
                           boxcox=False)

            Q_hist_mean = self.hist_meta['aggregate'][current_time_window]['Q']
            Q_hist_std = self.hist_meta['aggregate'][current_time_window]['Q_std']

            prediction = self.detect_zone_anomaly(Q_cur, Q_hist_mean, Q_hist_std, epsilon)
            Q_r = Q_cur
            speeds_r = cur_speeds
            comp_time = time.time() - comp_start_time

        if return_comp_time == True:
            return prediction, Q_r, speeds_r, comp_time
        else:
            return prediction, Q_r, speeds_r

    def attack_speeds(self,
                      cur_df,
                      current_time_window,
                      per_attacked,
                      delta,
                      camo,
                      attacked_ids,
                      norm=False):
        ids = cur_df['tmc_id'].unique().tolist()
        #rand_temp = int(len(ids) * per_attacked) + 1
        #attacked_ids = random.choices(ids, k=rand_temp)

        speeds = []

        for x in ids:
            if camo == True:
                delta_i = random.choice([-1, 1]) * deepcopy(delta)
            else:
                delta_i = deepcopy(delta)
            d = cur_df[cur_df['tmc_id'] == x]['SU'].values.tolist()
            if x in attacked_ids:
                cur_time_window_temp = nearest_non_nan_index(self.sensor_data_map[x].hist_meta['aggregate'], current_time_window)
                #std = self.sensor_data_map[x].hist_meta['aggregate'][cur_time_window_temp]['std']
                std = self.sensor_data_map[x].hist_meta['std']
                d = [y + delta_i * std for y in d]
                dd = [z if z >= 1 else 1 for z in d]
                '''
                if camo == False:
                    d = [y + delta * std for y in d]
                    dd = [z if z >= 1 else 1 for z in d]
                else:
                    d = [y + random.choice([-1, 1]) * delta * std for y in d]
                    dd = [z if z >= 1 else 1 for z in d]'''
            else:
                dd = d
            if norm == True:
                low = self.sensor_data_map[x].lower
                den = self.sensor_data_map[x].range
                ddd = [(((x - low) / den) + 1) for x in dd]
            else:
                ddd = dd
            speeds = speeds + ddd
        return np.array(speeds)

    def get_attacked_ids(self,
                         per_attacked):
        rand_temp = int(len(self.sensor_list) * per_attacked) + 1
        attacked_ids = random.choices(self.sensor_list, k=rand_temp)
        return attacked_ids

    def detect_zone_anomaly(self,
                            Q_cur,
                            Q_hist_mean,
                            Q_hist_std,
                            epsilon):
        #print("Q_cur: {}, Q_hist_mean: {}, epsilon: {}, Q_hist_std: {}".format(Q_cur, Q_hist_mean, epsilon, Q_hist_std))
        if Q_cur <= Q_hist_mean - epsilon * Q_hist_std:
            return 1
        elif Q_cur >= Q_hist_mean + epsilon * Q_hist_std:
            return 1
        else:
            return 0






