import json
from shapely.geometry import *
from shapely.ops import split, linemerge
from shapely.ops import cascaded_union
import folium
import random
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import osmnx as ox


def plot_tmc_ids(tmc_data, tmc_id_list='all', color="red"):
    """
    Function for plotting TMCs.

    :param tmc_data: geopandas dataframe
    :param tmc_id_list: list of tmc ids to plot, or 'all' to plot all tmcs in tmc_data
    :color: string

    return folium Map object
    """
    tmc_data = tmc_data.drop_duplicates(subset='tmc_id')

    if tmc_id_list == 'all':
        tmc_id_list = tmc_data['tmc_id'].values.tolist()

    shp_list = [asShape(tmc_data.at[tmc_id, "geometry"]) for tmc_id in tmc_id_list]
    shp = cascaded_union(shp_list)
    center = shp.centroid
    m = folium.Map(location=[center.y, center.x], zoom_start=13)
    bounds = shp.bounds
    m.fit_bounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]])
    if color == 'rand':
        style = lambda x: {
            'color': random.choice(['red', 'green', 'blue', 'black', 'yellow', 'purple', 'maroon', 'olive'])}
    else:
        style = lambda x: {'color': color}
    folium.GeoJson(mapping(shp), style_function=style).add_to(m)
    folium.LayerControl().add_to(m)
    return m

def plot_rsu_locations(rsu_data_network, color='blue'):
    """
    Will plot rsu locations along with TMC ids.

    :param rsu_data_network: dictionary
    :param color: string

    :return folium Map object
    """
    rsu_list = rsu_data_network.values()
    centers = []
    tmc_id_list = []
    for rsu in rsu_list:
        centers.append(rsu.rsu_location)
        tmc_id_list += rsu.sensor_shape_gdf['geometry'].tolist()

    shp_list = [asShape(c) for c in centers]
    shp_list = shp_list + tmc_id_list
    shp = cascaded_union(shp_list)
    center = shp.centroid
    m = folium.Map(location=[center.y, center.x], zoom_start=13)
    bounds = shp.bounds
    m.fit_bounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]])
    if color == 'rand':
        style = lambda x: {
            'color': random.choice(['red', 'green', 'blue', 'black', 'yellow', 'purple', 'maroon', 'olive'])}
    else:
        style = lambda x: {'color': color}

    folium.GeoJson(mapping(shp), style_function=style).add_to(m)
    folium.LayerControl().add_to(m)
    return m


def embed_map(m):
    """
    Function that is needed to show folium plots in jupyter notebook.

    :param m: folium Map object

    :return Ipython IFrame object
    """
    from IPython.display import IFrame

    m.save('index.html')
    return IFrame('index.html', width='100%', height='750px')


def plot_response_to_attack(result):
    """
    This will plot Q response to attack (see figure 7 in the paper).

    :param result: pandas dataframe

    :return matplotlib ax object
    """

    x = result['time'].values.tolist()

    fig, ax = plt.subplots(figsize=(10, 6))

    ax.plot(x, result['additive_attack'].values.tolist(), label="Additive Attack")
    ax.plot(x, result['deductive_attack'].values.tolist(), label="Deductive Attack")
    ax.plot(x, result['camouflage_attack'].values.tolist(), label="Camouflage Attack")
    ax.plot(x, result['no_attack'].values.tolist(), label="No Attack")

    ax.set_xlabel("Time")
    ax.set_ylabel("Q")

    for index, label in enumerate(ax.xaxis.get_ticklabels()):
        if index % 2 != 0:
            label.set_visible(False)

    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles, labels)
    return ax


def plot_recall_epsilon(result):
    """
    This will plot figure 8 in the paper.
    :param results: pandas dataframe
    :return matplotlib ax object
    """

    fig, ax = plt.subplots(figsize=(10, 6))

    ax.plot(result['FPR_deductive'].values.tolist(),
            result['TPR_deductive'].values.tolist(),
            label="Deductive")

    ax.plot(result['FPR_camouflage'].values.tolist(),
            result['TPR_camouflage'].values.tolist(),
            label="Camouflage")

    ax.set_xlabel("False Positive Rate", fontsize=18)
    ax.set_ylabel("Recall", fontsize=18)

    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles, labels)
    return ax


def plot_sensor_distribution(grid_cluster_df, 
                            fig_col='grid_sensor_count',
                            bins=10,
                            fig_title='Sensor Count'):
    """
    This will generate a matplotlib figure for sensor distribution.
    See Figures 5 and 6 in the paper.

    :param grid_cluster_df: pandas dataframe
    :param fig_col: string
    :param binwidth: int
    :param title: string

    returns matplotlib ax object
    """

    sensor_col = grid_cluster_df[fig_col]

    fig, ax = plt.subplots(figsize=(10, 6))

    ax.hist(sensor_col, 
            bins=bins,
            density=False)

    ax.set_xlabel("Number of Sensors Per RSU", fontsize=18, fontweight='bold')
    ax.set_ylabel("Count", fontsize=18, fontweight='bold')
    ax.set_title(fig_title, fontsize=18)
    ax.tick_params(axis='both', labelsize=18)
    ax.grid(True)
    return ax


def plot_recall_delta(recall_delta_results):
    """
    This will generate a matplotlib figure for Recall vs Delta.
    See Figure 9 in paper
    """

    fig, ax = plt.subplots(figsize=(10, 6))

    ax.plot(recall_delta_results['delta'], recall_delta_results['recall_d'], label='Deductive')
    ax.plot(recall_delta_results['delta'], recall_delta_results['recall_c'], label='Camouflage')

    ax.set_title("Recall vs Delta", fontsize=18)
    ax.set_xlabel("Delta")
    ax.set_ylabel("Recall")

    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles, labels)
    return ax


def plot_recall_p(recall_p_results):
    """
    This will generate a matplotlib figure for Recall vs Delta.
    See Figure 9 in paper
    """

    fig, ax = plt.subplots(figsize=(10, 6))

    ax.plot(recall_p_results['per_att'], recall_p_results['recall_d'], label='Deductive')
    ax.plot(recall_p_results['per_att'], recall_p_results['recall_c'], label='Camouflage')

    ax.set_title("Recall vs Percentage Sensors Attacked (p)", fontsize=18)
    ax.set_xlabel("Percentage of Sensors Attacked", fontsize=18)
    ax.set_ylabel("Recall", fontsize=18)

    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles, labels)
    return ax


def simulation_box_plot(grid_csv, cluster_csv, labels, metric='recall', title="Add Title"):
    fig, ax = plt.subplots(figsize=(10, 6))

    x_a = grid_csv["{}_deductive".format(metric)]
    x_b = cluster_csv["{}_deductive".format(metric)]
    x_c = grid_csv["{}_camouflage".format(metric)]
    x_d = cluster_csv["{}_camouflage".format(metric)]

    ax.boxplot([x_a, x_b, x_c, x_d], vert=False)

    vals = ax.get_xticks()
    ax.set_xticklabels(['{:,.0%}'.format(x) for x in vals])
    ax.tick_params(axis='both', labelsize=12)
    ax.set_yticklabels([labels[0],
                        labels[1],
                        labels[2],
                        labels[3]])
    ax.set_title(title, fontsize=18)
    ax.set_xlabel(metric, fontsize=18)
    return ax


def computation_time_plot(sensor_only_csv, tiered_csv, title="Add Title"):
    fig, ax = plt.subplots(figsize=(10, 6))

    x_a = sensor_only_csv['comp_time']
    x_b = tiered_csv['comp_time_zone']
    x_c = tiered_csv['comp_time_tiered']

    ax.boxplot([x_a, x_b, x_c], vert=False)

    ax.tick_params(axis='both', labelsize=12)
    ax.set_yticklabels(['Sensor Only', 'Zone Level Detection', 'Tiered'])
    ax.set_xlabel("Computation Time", fontsize=18)

    ax.set_title("Seconds", fontsize=18)
    return ax


def plot_graph(G, fig_height=20, node_size=15, node_alpha=1, edge_linewidth=.5, save=True, dpi=100):
    '''
    Will plot our network graph
    :param G: NetworkX MultiDiGraph
    :param fig_height: high of image
    :param node_size: size of nodes
    :param node_alpha:
    :param edge_linewidth: thickness of edges
    :param save: save image
    :param dpi:
    :return: fig, ax
    '''
    fig, ax = ox.plot_graph(G, fig_height=fig_height, node_size=node_size, node_alpha=node_alpha,
                            edge_linewidth=edge_linewidth, save=save, dpi=dpi, use_geom=True)
    return fig, ax




