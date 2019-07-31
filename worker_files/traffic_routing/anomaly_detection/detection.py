from copy import deepcopy
import pandas as pd
import numpy as np
import scipy.stats as stats
import random

### done ###

def cluster_features(speeds_list):
    return [np.median(speeds_list),
            np.mean(speeds_list),
            np.percentile(speeds_list, 10),
            np.percentile(speeds_list, 25),
            np.percentile(speeds_list, 75),
            np.percentile(speeds_list, 90)]


def get_attacked_ids(speeds, p, seed=None):
    if seed is not None:
        random.seed(seed)
    ids = speeds['tmc_id'].unique().tolist()
    num_attacked = int(len(ids) * p) + 1
    return random.choices(ids, k=num_attacked)


def get_Q(speeds, transform=None, feature_scale=None):
    """
    :param speeds: list or numpy array
    :param transform: either None, 'boxcox', or 'log'
    :param feature_scale: either None, 'min_max_scale'
    :return float
    """

    # if 0 or 1 speeds, return 0 for Q
    if len(speeds) < 2:
        return 0
    else:
        # first apply feature scaling
        if scale == "min_max_scale":
            x_min, x_max = np.minimum(speeds), np.maximum(speeds)
            scaled_speeds = [((x - x_min) / (x_max - x_min)) for x in speeds]
        else:
            scaled_speeds = speeds
        
        # next apply transformation
        if transform == 'boxcox':
            transformed_speeds = stats.boxcox(scaled_speeds)
        elif transform == 'log':
            transformed_speeds = np.log(scaled_speeds)
        else:
            transformed_speeds = scaled_speeds
        
        # shift distribution so min value is at 0
        x_min = np.minimum(transformed_speeds)
        final_speeds = [x - x_min for x in transformed_speeds]
        Q = stats.hmean(final_speeds) / np.mean(final_speeds)
        return Q


def time_range_distribution(rsu_label,
                            rsu_network,
                            speeds,
                            start_time="00:00:00",
                            end_time="23:59:59",
                            daysofweek=[0, 1, 2, 3, 4, 5, 6]):
    cur_speeds = speeds.between_time(start_time, end_time)

    rsu = rsu_network[rsu_label]
    rsu_speeds = cur_speeds[cur_speeds['tmc_id'].isin(rsu.sensor_list)]
    rsu_speeds = rsu_speeds[rsu_speeds.index.dayofweek.isin(daysofweek)]
    return rsu_speeds['SU'].values.tolist()


### in progress ###

def attack_speeds(speeds=None, delta=None, sigma=None, p=0.0, camo=False):
    # TODO
    attacked_ids = get_attacked_ids(speeds, p)


def sim_attack_response(config):
    # TODO
    # get RSU and attack parameters (sigma and delta
    rsu = deepcopy(config['rsu_network'][config['rsu']])
    sigma, delta, p = rsu.hist_meta['std'], config['delta'], config['per_attacked']
    print("sigma = {}, delta = {}".format(sigma, delta))

    # setup simulation start time, end time and time window
    sim_start = pd.to_datetime(config['sim_start']).tz_localize('US/Central')
    sim_end = pd.to_datetime(config['sim_end']).tz_localize('US/Central')
    time_delta = pd.Timedelta(config['time_window'])

    # get all the speeds for this RSU within the simulation start and end time
    speeds = deepcopy(config['testing_speeds'][sim_start:sim_end])
    speeds = speeds[speeds['tmc_id'].isin(rsu.sensor_list)]

    # setup result dictionary (will be converted to a pandas dataframe at the end)
    result = {'no_attack': [],
              'deductive_attack': [],
              'additive_attack': [],
              'camouflage_attack': [],
              'start_time': [],
              'end_time': [],
              'time_window_k': []}

    # run simulation
    start_time = deepcopy(sim_start)
    k = 0
    while start_time < sim_end:
        end_time = start_time + time_delta
        result['start_time'].append(start_time)
        result['end_time'].append(end_time)
        result['time_window_k'].append(k)

        cur_speeds = speeds[start_time:end_time]
        result['no_attack'].append(get_Q(cur_speeds))

        deductive_speeds = attack_speeds(speeds=cur_speeds, delta=-delta, sigma=sigma, p=p)
        result['deductive_attack'].append(get_Q(deductive_speeds))

        additive_speeds = attack_speeds(speeds=cur_speeds, delta=delta, sigma=sigma, p=p)
        result['additive_attack'].append(get_Q(additive_speeds))

        camo_speeds = attack_speeds(speeds=cur_speeds, delta=delta, sigma=sigma, p=p, camo=True)
        result['additive_attack'].append(get_Q(camo_speeds))

        k += 1
        start_time = start_time + time_delta







