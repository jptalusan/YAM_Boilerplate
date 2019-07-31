import numpy as np
from copy import deepcopy
import random
import math
import sys
import time

sys.path.append('..')
from src import anomaly_detection as ad


# Needs to get "realtime" data from the speeds (which is stored in rsu_cluster_network)
# Get the last 30 min data and average it, then update the hash
# Get the last available time (preferably NOW) and then get the data from that to 30 min before
# Calculate the mean and then update a copy of the hash_map, i guess we have to deepcopy, since this is supposed
# to be available only to the particular rsu_id that we are in

# Test data is used for the "realtime" data
# Since speeds.pkl was divided into training and testing
DEBUG = 0
def update_cluster_hash_for_rsu(rsu_id, rsu_hash, speed_data, day_of_week, hourofday):
    start = time.time()
    
#     Get current day of week now
#     Fix this after
    valid_weekdays = [0, 1, 2, 3]
    test_weekday = day_of_week
    if day_of_week >= 4:
        test_weekday = random.choice(valid_weekdays)
    test_weekday = 26 + test_weekday
    
#     print("Test weekday: {}".format(test_weekday))
    start_test_time = "2018-03-{} {}:00:00".format(test_weekday, hourofday)
    end_test_time   = "2018-03-{} {}:59:59".format(test_weekday, hourofday)

    speed_data = speed_data[start_test_time:end_test_time]
    local_view_hash = deepcopy(rsu_hash)
    
    for rsu, sensors in local_view_hash.items():
        if rsu == rsu_id:
            for sensor, mean_speed in sensors.items():
                if __debug__ == DEBUG:
                    print(sensor)
                tmc_mean_speed = np.mean(speed_data.loc[speed_data['tmc_id'] == sensor]['SU'].values)
                
#                 TODO: This is just for testing
                if math.isnan(tmc_mean_speed):
                    if __debug__ == DEBUG:
                        print("Something is wrong, Test data has NaN, speed set to 20.0 instead")
                    rand_multipler = [0.70, 0.80, 1.20, 1.30]
                    tmc_mean_speed = rsu_hash[rsu][sensor][hourofday] * random.choice(rand_multipler)
                    
                old_local_view = local_view_hash[rsu][sensor][hourofday]
                local_view_hash[rsu][sensor][hourofday] = tmc_mean_speed
                if __debug__ == DEBUG:
                    print("Historical view: {}, Local view:{}".format(old_local_view, tmc_mean_speed))
    elapsed = time.time() - start
    if __debug__ == DEBUG:
        print(elapsed)
    
    return local_view_hash

# Needs to get "realtime" data from the speeds (which is stored in rsu_cluster_network)
# Get the last 30 min data and average it, then update the hash
# Get the last available time (preferably NOW) and then get the data from that to 30 min before
# Calculate the mean and then update a copy of the hash_map, i guess we have to deepcopy, since this is supposed
# to be available only to the particular rsu_id that we are in

# This is slower than meth:update_cluster_hash_for_rsu()
DEBUG = 0
def update_cluster_hash_with_realtime_data_for_rsu(rsu_id, rsu_hash, rt_spd_data, dayofweek, hourofday):
    if __debug__ == DEBUG:
        print("RSU_ID: {}".format(rsu_id))
        print("Day of week: {}".format(dayofweek))
    
#     speed_data = realtime_speed_data[realtime_speed_data.index.dayofweek == dayofweek]
    
    speed_data = rt_spd_data.loc[(rt_spd_data.index.dayofweek == dayofweek) & (rt_spd_data.index.hour == hourofday)]
    if __debug__ == DEBUG:
        display(speed_data.head())
    local_view_hash = deepcopy(rsu_hash)
    
    for rsu, sensors in local_view_hash.items():
        if rsu == rsu_id:
            for sensor, mean_speed in sensors.items():
                if __debug__ == DEBUG:
                    print(sensor)
                # Getting the "real-time" mean speed from the speed_data df
                # for all sensors in the RSU id.
                tmc_mean_speed = np.mean(speed_data.loc[speed_data['tmc_id'] == sensor]['SU'].values)
                
                # Because there are blanks in the dataset resulting in NaN data
                if math.isnan(tmc_mean_speed):
                    rand_multipler = [0.70, 0.80, 1.20, 1.30]                    
                    tmc_mean_speed = rsu_hash[rsu][sensor][hourofday] * random.choice(rand_multipler)
#                     tmc_mean_speed = 20.0
                    if __debug__ == DEBUG:
                        print("Something is wrong, data has NaN, speed set to {:4.2f} instead".format(tmc_mean_speed))
                    
                # For comparison
                global_view = rsu_hash[rsu][sensor][hourofday]
                local_view_hash[rsu][sensor][hourofday] = tmc_mean_speed
                if __debug__ == DEBUG:
                    print("Global view: {:4.2f}, Local view:{:4.2f}".format(global_view, tmc_mean_speed))
    return local_view_hash

def check_which_cluster_sensor_belongs_to(sensor, rsu_hash):
    for rsu, val in rsu_hash.items():
        for sen, _ in val.items():
            if sen == sensor:
                return rsu
    return None

# For use with the networkx route generation which uses Djikstra's Algorithm
# This generates random weights for the edges instead of using actual data
def random_speeds(start, end, attr, rsu_hash=None, time_window=None):
    return random.uniform(1, 10)

# Same usage as above but uses actual historical data as the weights of the edges
def historical_speeds(start, end, attr, rsu_hash=None, time_window=None):
    tmc_id = attr[0]['tmc_id']
    rsu_id = check_which_cluster_sensor_belongs_to(tmc_id, rsu_hash)

# This does not seem right. Im getting all the data from all sensors every time an edge is passed here
#     speed_arr = []
#     for _, v in rsu_hash[rsu_id].items():
#         speed_arr.append(v[time_window])
#     mean_speed = np.mean(np.asarray(speed_arr))
   
    mean_speed = rsu_hash[rsu_id][tmc_id][time_window]
    
    if __debug__ == DEBUG:
        print("Historical Speed: ", start, end, rsu_id, tmc_id, mean_speed)
        
    edge_length = attr[0]['length']
    time = abs(edge_length/mean_speed)
    return time

def check_which_cluster_node_belongs_to(G, node, rsu_cluster_network):
    node_attr = G[node]
    for k, _ in node_attr.items():
        if __debug__ == DEBUG:
            print(node_attr)
            print(node_attr[k][0]['geometry'].coords[0])
        lon = node_attr[k][0]['geometry'].coords[0][0]
        lat = node_attr[k][0]['geometry'].coords[1][1]
    
        distance = math.inf
        closest_rsu = ''
        
        for rsu, v in rsu_cluster_network.items():
            rsu_lon = v.rsu_location.x
            rsu_lat = v.rsu_location.y
            temp = ad.haversine(lon, lat, rsu_lon, rsu_lat)
            if temp < distance:
                distance = temp
                closest_rsu = rsu
    #         print()
        break

    return closest_rsu

# TODO: How can I prevent suddent jumps when it changes RSU?
def check_rsu_cluster_routing(G, route, rsu_cluster_network, rsu_hash):
    edge_list = []
    for idx, _ in enumerate(route):
        if idx == len(route) - 1:
            break
        start = route[idx]
        end = route[idx + 1]
        edge_list.append((start, end))
    
    curr_rsu = None
    next_rsu = None
    next_node = None
    for edge in edge_list:
        curr_attr = G.get_edge_data(edge[0], edge[1])
        curr_tmc_id = curr_attr[0]['tmc_id']
        rsu = check_which_cluster_sensor_belongs_to(curr_tmc_id, rsu_hash)
        
#         Must change this part
        if __debug__ == DEBUG:
            print(rsu, edge[0])
            
        if curr_rsu == None:
            curr_rsu = rsu
        else:
#             if rsu != curr_rsu and next_rsu == None:
            if next_rsu == None:
                next_rsu = rsu
                next_node = edge[0]
                break
                
    return (curr_rsu, next_rsu, next_node) 

# I have to remove past TMCs from the list so the vehicle will not go back
# Or better move is to deepcopy the hash and modify it as the route progresses
# Change the speed data to Infinite so it will never be used
def disable_edges_of_traveresed_rsu(traveresed_rsu, rsu_hash):
    rsu_hash_copy = deepcopy(rsu_hash) #For testing only
#     rsu_hash_copy = rsu_hash #For testing only
    for rsu, sensors in rsu_hash_copy.items():
        if rsu == traveresed_rsu:
            if __debug__ == DEBUG:
                print(len(sensors))
            for sensor, speeds in sensors.items():
                if __debug__ == DEBUG:
                    print(sensor)
                for speed in speeds:
                    rsu_hash_copy[rsu][sensor][speed] = 0.000001 #It needs to be very low because length/speed
                if __debug__ == DEBUG:
                    print(rsu_hash_copy[rsu][sensor])
    return rsu_hash_copy