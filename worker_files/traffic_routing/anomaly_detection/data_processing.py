from shapely.geometry import Polygon
import geopandas as gp
import math
import datetime
from math import radians, sin, cos, sqrt, asin
import pandas as pd


def data_from_bounds(boundary=None,
                     tmc_gdf=None,
                     speeds=None,
                     contains=False):
    """
    Takes a boundary and returns the data within boundary.
    :param boundary: shapely polygon
    :param tmc_gdf: geopandas geodataframe
    :param speeds: pandas dataframe
    :param contains: bool
    :return: geopandas dataframe (tmc data), pandas dataframe (speed data)
    """

    assert type(boundary) == Polygon, "boundary must be type polygon"
    assert type(tmc_gdf) == gp.GeoDataFrame, "tmc_gdf must be type geopandas GeoDataFrame"

    if contains == False:
        tmc_gdf_r = tmc_gdf[tmc_gdf.intersects(boundary)]
    else:
        tmc_gdf_r = tmc_gdf[tmc_gdf.within(boundary)]

    speeds_r = speeds[speeds['tmc_id'].isin(tmc_gdf_r.index.unique().tolist())]
    tmc_gdf_r = tmc_gdf_r[tmc_gdf_r.index.isin(speeds_r['tmc_id'].unique().tolist())]
    return tmc_gdf_r, speeds_r



def get_sensor_geometry(tmc_gdf,
                        sensor):
    result = tmc_gdf[tmc_gdf['tmc_id'] == sensor]
    if len(result) > 1:
        raise Exception('more than 1 shape returned for sensor {}'.format(sensor))
    elif len(result) == 0:
        raise Exception('zero shapes returned for sensor {}'.format(sensor))
    else:
        return result['geometry'].values.tolist()[0]


def time_window_from_time(current_time, num_windows):
    #result = math.floor((current_time.hour * num_windows)/24.0)
    result = math.floor(((current_time.hour + (current_time.minute/60.0)) * num_windows) / 24.0)
    return str(result)


def time_from_time_window(time_window, num_windows):
    result_hours = (time_window * 24.0) / num_windows
    hour = math.floor(result_hours)
    minute = (result_hours % 1) * 60
    return datetime.time(hour=int(hour), minute=int(minute))


def train_test_split(df, test_start_time, test_end_time):
    """
    Splits pandas dataframe into training and testing data sets.
    :param test_start_time: string
    :param test_end_time: string
    :return: pandas dataframe, pandas dataframe
    """

    test_speeds = df[test_start_time:test_end_time]
    training_speeds = df[~df.index.isin(test_speeds.index)]
    return training_speeds, test_speeds

def get_number_of_time_windows(time_window):
    if type(time_window) == str:
        time_w = float(time_window[0:-1])
    else:
        time_w = time_window
    windows = (24.0 * 60.0) / time_w
    return windows


def nearest_non_nan_index(my_list, cur_index):
    if my_list[cur_index] is not None:
        return cur_index
    else:
        inds = []
        dist = []
        for i in range(len(list(my_list.keys()))):
            if my_list[list(my_list.keys())[i]] is not None:
                inds.append(i)
                dist.append(abs(float(cur_index)-i))
        if len(inds) == 0:
            return -1
        else:
            x = dist.index(min(dist))
            return str(inds[x])



def deg_to_length(deg, unit, lat=36.3, lon=-86.7):
    # get the length of one degree latitude and one degree longitude
    lat_deg = haversine(lon, lat, lon, lat + 1, unit=unit)
    lon_deg = haversine(lon, lat, lon + 1, lat, unit=unit)

    # convert length to degrees
    length = deg * ((lat_deg + lon_deg)/2)
    return length


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


def alter_epsilon_map(epsilon_map, delta):
    result = {}
    for k, v in epsilon_map.items():
        result[k] = v + delta
    return result


def load_epsilon_map(file_path, alter):
    """
    This function will load the epsilon values.
    :param file_path: string
    :param alter: float
    :return dictionary
    """

    optimal = pd.read_csv(file_path)
    epsilon_map = {}
    for k, v in optimal.iterrows():
        epsilon_map[v['rsu_id']] = v['epsilon']

    epsilon_map = alter_epsilon_map(epsilon_map, .25)
    return epsilon_map