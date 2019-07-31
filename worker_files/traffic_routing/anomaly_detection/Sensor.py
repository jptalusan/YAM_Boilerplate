import pandas as pd
import numpy as np
from copy import deepcopy
import time
from collections import OrderedDict
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import RBF, ConstantKernel
import datetime
import os
from src.anomaly_detection import save_load
from src.anomaly_detection.data_processing import time_window_from_time, nearest_non_nan_index, get_number_of_time_windows
import os
import random


def train_sensors(training_speeds,
                  tmc_gdf,
                  time_window,
                  num_nearest,
                  sensor_data_map,
                  alpha=10**-10,
                  n_restarts_optimizer=10):
    sensors = tmc_gdf['tmc_id'].unique().tolist()
    print("total number of sensors to be trained: {}".format(len(sensors)))

    sensor_kernels = {}
    start_time = time.time()
    i = 0
    for sensor in sensors:
        other_sensors = get_nearest_sensors(sensor,
                                            tmc_gdf,
                                            num_nearest)
        temp = other_sensors + [sensor]
        group_speeds = training_speeds[training_speeds['tmc_id'].isin(temp)]

        X, y, good, ok, bad = get_X_y(sensor,
                                      other_sensors,
                                      group_speeds,
                                      time_window,
                                      sensor_data_map)

        kernel = RBF()
        gp = GaussianProcessRegressor(kernel=kernel,
                                      n_restarts_optimizer=n_restarts_optimizer,
                                      copy_X_train=True,
                                      random_state=501,
                                      normalize_y=True,
                                      alpha=alpha)
        gp.fit(X, y)

        sensor_kernels[sensor] = {'kernel': gp, 'X_order': other_sensors, 'good': good, 'ok': ok, 'bad': bad}
        if (i % 10) == 0:
            print("done with {} sensors".format(i))
            print("number of good {}, number of ok {}, number of bad {}".format(good, ok, bad))
            end_time = time.time() - start_time
            print("total time since training start: {}".format(end_time))
        i += 1
    return sensor_kernels


def get_X_y(sensor,
            other_sensors,
            group_speeds,
            time_window,
            sensor_data_map):
    X = []
    y = []
    group_speeds_groups = group_speeds.resample("{}T".format(time_window[0:-1]))
    good, ok, bad = 0, 0, 0
    for name, group in group_speeds_groups:
        if (name.time() >= datetime.time(7, 0, 0)) & (name.time() < datetime.time(19, 0, 0)):
            sensor_speeds = group[group['tmc_id'] == sensor]
            if len(sensor_speeds) >= 2:
                y.append(np.mean(sensor_speeds['SU'].values))

                x_i = []

                for other_sensor in other_sensors:
                    other_sensor_speeds = group[group['tmc_id'] == other_sensor]
                    if len(other_sensor_speeds) >= 2:
                        good += 1
                        x_i.append(np.mean(other_sensor_speeds['SU'].values))
                    elif len(other_sensor_speeds) == 1:
                        ok += 1
                        x_i.append(other_sensor_speeds['SU'].values.tolist()[0])
                    else:
                        bad += 1
                        num_windows = get_number_of_time_windows("30m")
                        cur_time_window_temp = time_window_from_time(name, num_windows)
                        cur_time_window_temp = nearest_non_nan_index(sensor_data_map[other_sensor].hist_meta['aggregate'], cur_time_window_temp)
                        x_i.append(sensor_data_map[other_sensor].hist_meta['aggregate'][cur_time_window_temp]['u'])
                X.append(x_i)
    #print("number of good {}, number of ok {}, number of bad {}".format(good, ok, bad))
    return X, y, good, ok, bad


def get_nearest_sensors(sensor, tmc_gdf, num_nearest):
    tmcs = tmc_gdf.copy(deep=True)
    sensor_shape = tmcs.loc[sensor, 'geometry']

    shapes = tmcs['geometry'].values.tolist()
    tmcs['distances'] = [sensor_shape.distance(x) for x in shapes]
    tmcs.sort_values(by=['distances'], inplace=True, ascending=True)
    #print(tmcs)
    return tmcs.index.values.tolist()[0:num_nearest]


class Sensor:
    def __init__(self,
                 training_speeds=None,
                 sensor_shape=None,
                 sensor_id=None,
                 init_meta=False):
        os.system("taskset -p 0xff %d" % os.getpid())

        # get training speeds and normalized training speeds
        self.training_speeds = deepcopy(training_speeds)
        self.u = np.mean(self.training_speeds['SU'].values)
        self.std = np.std(self.training_speeds['SU'].values)
        self.upper = np.percentile(self.training_speeds['SU'].values, 90)
        self.lower = np.min(self.training_speeds['SU'].values)
        self.range = self.upper - self.lower
        self.training_speeds['SU_norm'] = [((x + 1 - self.lower) / self.range) for x in self.training_speeds['SU'].values.tolist()]

        self.sensor_shape = sensor_shape
        self.sensor_id = sensor_id

        params = save_load.get_simulation_parameters()
        if type(params['time_window']) == str:
            self.time_delta = pd.Timedelta(params['time_window'])
        else:
            self.time_delta = pd.Timedelta("{}m".format(params['time_window']))
        self.days_of_week = self.training_speeds.index.dayofweek.unique().tolist()
        self.num_daily_windows = get_number_of_time_windows(params['time_window'])

        self.testing_speeds = None

        if init_meta == False:
            self.hist_meta = None
        elif init_meta == True:
            self.init_hist_meta()

    def get_u(self, start_time, end_time, norm=False):
        temp_speeds = self.testing_speeds[start_time:end_time]

        if len(temp_speeds) >= 2:
            if norm == True:
                return np.mean(temp_speeds['SU_norm'].values), 1
            else:
                return np.mean(temp_speeds['SU'].values), 1
        else:
            num_windows = get_number_of_time_windows("30m")
            cur_time_window_temp = time_window_from_time(start_time, num_windows)
            cur_time_window_temp = nearest_non_nan_index(self.hist_meta['aggregate'],
                                                         cur_time_window_temp)
            if norm == True:
                return self.hist_meta['aggregate'][cur_time_window_temp]['u_norm'], 0
            else:
                return self.hist_meta['aggregate'][cur_time_window_temp]['u'], 0

    def get_speeds(self, start_time, end_time, norm=False):
        temp_speeds = self.testing_speeds[start_time:end_time]
        if norm == True:
            return temp_speeds['SU_norm'].values
        else:
            return temp_speeds['SU'].values

    def get_speeds_attacked(self, start_time, end_time, camo, delta, norm=False):
        temp_speeds = self.testing_speeds[start_time:end_time]
        d = temp_speeds['SU'].values.tolist()
        std = self.hist_meta['std']
        if camo == True:
            delta_i = random.choice([-1, 1]) * deepcopy(delta)
        else:
            delta_i = deepcopy(delta)
        d = [y + delta_i * std for y in d]
        dd = [z if z >= 1 else 1 for z in d]
        '''
        if camo == False:
            d = [y + delta * std for y in d]
            dd = [z if z >= 1 else 1 for z in d]
        else:
            d = [y + random.choice([-1, 1]) * delta * std for y in d]
            dd = [z if z >= 1 else 1 for z in d]
        if norm == True:
            ddd = [((x - self.lower) / self.range) for x in dd]
        else:
            ddd = dd'''
        if norm == True:
            ddd = [((x - self.lower) / self.range) for x in dd]
        else:
            ddd = dd
        return np.array(ddd)

    def init_hist_meta(self):
        """
        Will set self.hist_meta

        hist_meta structure
        {
            'u': float
            'std': float
            '0',...'num_days': {
                                '0'...'num_windows': {
                                                        'u': float
                                                        'std': float
                                                    }
                                }
            'aggregate': {
                                '0'...'num_windows': {
                                                        'u': float
                                                        'std': float
                                                    }
                        }
        }

        example hist_meta search
        sensor.hist_meta[day][time_window]['u']
        :param time_delta: int
        :param days_of_week: list of ints
        :param train_sensors: bool
        :return: None, sets self.hist_meta
        """

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
            u_window = []
            for day in self.days_of_week:
                u_window_day = []
                daily_window_speeds = window_speeds[window_speeds.index.dayofweek == day]
                for date in np.unique(daily_window_speeds.index.date).tolist():
                    daily_window_speeds_day = daily_window_speeds[daily_window_speeds.index.date == date]['SU'].values
                    if len(daily_window_speeds_day) >= 2:
                        u_window.append(np.mean(daily_window_speeds_day))
                        u_window_day.append(np.mean(daily_window_speeds_day))

                if str(day) not in result.keys():
                    result[str(day)] = OrderedDict()
                if len(u_window_day) >= 2:
                    result[str(day)][str(window)] = {'u': np.mean(daily_window_speeds['SU'].values),
                                                     'std': np.std(daily_window_speeds['SU'].values),
                                                     'u_mean': np.mean(u_window_day),
                                                     'std_mean': np.std(u_window_day)}
                else:
                    result[str(day)][str(window)] = None
            if len(u_window) >= 2:
                result['aggregate'][str(window)] = {'u': np.mean(window_speeds['SU'].values),
                                                    'std': np.std(window_speeds['SU'].values),
                                                    'u_mean': np.mean(u_window),
                                                    'std_mean': np.std(u_window),
                                                    'u_norm': np.mean(window_speeds['SU_norm'].values),
                                                    'std_norm': np.std(window_speeds['SU_norm'].values)}
            else:
                result['aggregate'][str(window)] = None
            start_time = end_time
            window += 1

        self.hist_meta = result


##### Void
class Sensor_Detection:
    def __init__(self,
                 sensor_data):
        self.sensor_data = sensor_data
        self.results = None




