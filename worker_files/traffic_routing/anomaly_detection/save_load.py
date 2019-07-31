from fastparquet import ParquetFile
import pandas as pd
import geopandas as gp
import json
import os
from shapely.geometry import *
from shapely.ops import split, linemerge
from shapely.ops import cascaded_union
import folium
import random
import requests, zipfile, io


def load_data_csv(file_path, daysofweek=[0, 1, 2, 3]):
    data = pd.read_csv(file_path,
                       parse_dates=['datetime'],
                       date_parser=lambda col: pd.to_datetime(col, utc=True))

    data.set_index(pd.DatetimeIndex(data['datetime']), inplace=True)
    data.index = data.index.tz_convert('America/Chicago')

    data.drop(['CN', 'FF', 'JF', 'SP'], axis=1, inplace=True)
    data = data[data.index.dayofweek.isin(daysofweek)]
    data = data[data['SU'] > 0]
    data = data[~data.index.duplicated(keep='first')]
    return data


def load_tmc_geo(file_path):
    """ 
    Load TMC shape information into a geopandas dataframe.

    :param file_path: string

    :return geopandas dataframe
    """

    # load raw TMC data from json file
    with open(file_path, 'r') as file:
        tmc_json = json.load(file)

    # Turn raw JSON data into a geopandas dataframe
    tmc_gdf = json_to_geopandas(tmc_json)
    tmc_gdf = tmc_gdf.drop_duplicates(subset='tmc_id')
    return tmc_gdf


def json_to_geopandas(tmc_json):
    """
    Convert raw JSON data to a geopandas dataframe.

    :param tmc_json: dictionary
    
    :return geopandas dataframe
    """

    results = {'geometry': [],
               'tmc_id': [],
               'type': []}
    for key, val in tmc_json.items():
        results['tmc_id'].append(key)
        results['type'].append(val['tmc_shp']['type'])
        results['geometry'].append(MultiLineString(val['tmc_shp']['coordinates']))
    geo_data = gp.GeoDataFrame(results)

    geo_data.set_index(geo_data['tmc_id'], inplace=True)
    return geo_data


def set_simulation_parameters(config=None):
    conf_dir = os.path.join(os.getcwd(), 'src', 'conf')
    config['conf_dir'] = conf_dir
    with open(os.path.join(conf_dir, 'simulation_parameters.txt'), 'w') as f:
        json.dump(config, f)


def get_simulation_parameters():
    conf_dir = os.path.join(os.getcwd(), 'src', 'conf')
    with open(os.path.join(conf_dir, 'simulation_parameters.txt'), 'r') as f:
        config = json.load(f)
    return config
