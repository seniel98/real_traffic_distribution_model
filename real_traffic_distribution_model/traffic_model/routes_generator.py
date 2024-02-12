import sys
import time

import pandas as pd
import sqlite3
import random
import numpy as np
from geopy.distance import geodesic
from datetime import datetime
sys.path.append("/home/josedaniel/real_traffic_distribution_model")

import real_traffic_distribution_model as rtdm

is_reiterating = False

percentage = 25
tolerated_error = 1.1

primary_count = 0
roundabouts = set()
def is_n_vehicles_ok(options, ata, df, is_src_or_des=False):
    """
    It checks if the number of vehicles for a given ATA is within the tolerated error

    Args:
      options: the command line options
      ata: The ATA code
      df: the dataframe containing the data
      is_src_or_des: If True, then the function is being called for the source or destination ATA. If False, then the
    function is being called for an intermediate ATA. Defaults to False

    Returns:
      The function is_n_vehicles_ok() is returning a boolean value.
    """
    n_vehicles = df[df['ATA'] == ata]['n_vehicles'].to_list()
    n_vehicles_copy = get_n_vehicles_from_db(sqlite3.connect(options.traffic_db), ata)
    if n_vehicles and n_vehicles_copy:
        if float(n_vehicles_copy) != 0.0:
            if is_src_or_des and abs((n_vehicles_copy - n_vehicles[0]) / float(n_vehicles_copy)) >= 1.0:
                # print("Number of vehicles for ATA source or destination exceeds limit")
                return False
            elif not is_src_or_des and abs(
                    (n_vehicles_copy - n_vehicles[0]) / float(n_vehicles_copy)) >= tolerated_error:
                # print("Number of vehicles for ATA intermediate exceeds limit")
                return False
            else:
                return True
        else:
            return False
    else:
        return True


def create_od_routes(options, net):
    """
    It takes a traffic file and a traffic database, and generates a
    routes file and a modified traffic file

    Args:
      options: the options object that contains the path to the database and the path to the traffic file
    """
    global exec_time_start, is_reiterating, primary_count

    traffic_df = pd.read_csv(options.traffic_file)

    passenger_cars = 0.72
    vehicle_not_parking = 0.85

    # Apply vectorized operations for efficiency
    traffic_df['n_vehicles'] = (traffic_df['n_vehicles'] * passenger_cars * vehicle_not_parking).astype(int)

    # Get the edges for the roundabouts
    for roundabout in net.getRoundabouts():
        roundabouts.update(roundabout.getEdges())

    # Connect to the database once, outside the loop
    with sqlite3.connect(options.traffic_db) as conn:
        total_vehicles_copy = get_n_vehicles_from_db(conn, all_vehicles=True)

    total_vehicles = traffic_df['n_vehicles'].sum()

    route_id_list = []
    route_list = []
    coord_route_list = []
    exec_time_list = []
    global_ata_list = []
    print_number = 0
    total_exec_time = 0
    # The above code is generating routes for the given percentage of vehicles.
    while total_vehicles > int((total_vehicles_copy * (1 - (percentage / 100)))):

        if not is_reiterating:
            exec_time_start = datetime.now()

        src_ata, src_point = select_point(options, df=traffic_df)
        traffic_df_filtered = traffic_df.copy()

        for index, row in traffic_df.iterrows():
            if geodesic(src_point, eval(row['coord_node'])).m < 2000:
                traffic_df_filtered.drop(index, axis=0, inplace=True)

        des_ata, des_point = select_point(options, df=traffic_df_filtered, is_filtered=True)

        src_lat, src_lon = src_point
        des_lat, des_lon = des_point

        # coord_route = generate_route(options, src_lat, src_lon, des_lat, des_lon, process_route)
        ways_id = rtdm.get_route_from_ABATIS(options, src_lat, src_lon, des_lat, des_lon, process_route)
        if ways_id is None:
            is_reiterating = True
            continue

        coords_route, nodes_route, edges_route, primary_count = rtdm.coordinates_to_edge(ways_id, net, primary_count, roundabouts)
        if coords_route is None or nodes_route is None or edges_route is None:
            is_reiterating = True
            continue
        if primary_count >= 3:
            is_reiterating = True
            primary_count = 0
            continue

        route_ata_list = []
        if is_n_vehicles_ok(options, src_ata, traffic_df, is_src_or_des=True) and is_n_vehicles_ok(options,
                                                                                                   des_ata,
                                                                                                   traffic_df,
                                                                                                   is_src_or_des=True):
            route_ata_list.append(src_ata)
            for i in range(0, len(nodes_route) - 1):
                # node_int = nodes_route[i][0]
                node_int = nodes_route[i]
                ata = get_ATA_from_db(sqlite3.connect(options.traffic_db), str(node_int))
                if is_n_vehicles_ok(options, ata, traffic_df):
                    if ata is not None and ata not in route_ata_list and ata != des_ata:
                        route_ata_list.append(ata)
                    if i == (len(nodes_route) - 2):
                        route_ata_list.append(des_ata)

                        for ata in route_ata_list:
                            row_index = traffic_df.loc[traffic_df['ATA'] == ata].index
                            traffic_df.at[row_index[0], 'n_vehicles'] = np.int64(
                                (traffic_df.at[row_index[0], 'n_vehicles'].item() - 1))

                        route_id_list.append(f'{edges_route[0]}_to_{edges_route[len(edges_route) - 1]}')
                        route_list.append(edges_route)
                        coord_route_list.append(coords_route)
                        global_ata_list.append(route_ata_list)
                        total_vehicles = np.sum(traffic_df['n_vehicles'].to_numpy())

                        vehicles_remaining = int(total_vehicles) - int(
                            (total_vehicles_copy * (1 - (percentage / 100))))

                        exec_time_end = datetime.now()
                        exec_time = exec_time_end.timestamp() * 1000 - exec_time_start.timestamp() * 1000
                        total_exec_time += exec_time
                        exec_time_list.append(exec_time)

                        if print_number % 10 == 0:
                            print(f'Vehicles remaining: {vehicles_remaining}')
                            print(f'Total v{percentage}p_{tolerated_error} routes generated: {len(route_list)}')
                            # route_is_possible = True
                            print(f'Total execution time: {(total_exec_time/1000):.2f} seconds')
                        print_number += 1

                        is_reiterating = False
                else:
                    is_reiterating = True
                    break

        else:
            is_reiterating = True
            continue

    gen_routes_data = {'route_id': route_id_list, 'route': route_list, 'coord': coord_route_list,
                       'ATA': global_ata_list,
                       'exec_time': exec_time_list}
    gen_routes_df = pd.DataFrame.from_dict(gen_routes_data)
    gen_routes_df.to_csv(
        f'/home/josedaniel/Modelo_distrib_trafico_real/routes_data/gen_routes_data_{str(percentage)}p_{str(tolerated_error)}_net_edited.csv',
        index=False)
    gen_routes_df.drop(columns={"exec_time", "coord"}, inplace=True)
    gen_routes_df_clean = gen_routes_df['route_id'].value_counts().to_frame().reset_index()
    gen_routes_df_clean.rename(columns={'index': 'route_id', 'route_id': 'n_vehicles'}, inplace=True)
    gen_routes_df_clean.to_csv(
        f'/home/josedaniel/Modelo_distrib_trafico_real/veh_per_route/routes_{str(percentage)}p_{str(tolerated_error)}_net_edited.csv',
        index=False)
    traffic_df.to_csv(
        f'/home/josedaniel/Modelo_distrib_trafico_real/traffic_data/csv/traffic_df_modified_{str(percentage)}p_{str(tolerated_error)}_net_edited.csv',
        index=False)


def process_route(data):
    return data


def select_point(options, df=None, is_filtered=False):
    """
    This function takes a dataframe and a list of ATA codes, and returns the ATA code and the coordinates of the point
    that is closest to the center of the dataframe

    Args:
      options: the options object from the command line
      df: the dataframe containing the data
      is_filtered: if True, the function will only return points that are in the filtered dataframe. Defaults to False

    Returns:
      ata, point
    """

    ata_list = get_ATA_list_from_db(sqlite3.connect(options.traffic_db))
    ata, point = get_coord_for_ata(df, ata_list, is_filtered=is_filtered, options=options)
    return ata, point


def softmax(x):
    """Compute softmax values for each set of scores in x."""
    e_x = np.exp(x - np.max(x))
    return e_x / e_x.sum(axis=0)


def get_coord_for_ata(df, ata_list, is_filtered=False, options=None, prev_selected_ata=None):
    """
    The function takes in a dataframe, a list of ATA codes, a boolean value indicating whether the dataframe is filtered
    or not, a dictionary of options, and the previously selected ATA code. It returns the selected ATA code and the point
    (latitude and longitude) of the selected ATA, with a more distributed selection process.

    Args:
      df: The dataframe containing the data
      ata_list: list of ATA codes
      is_filtered: If True, the function will use the ATA list from the dataframe. If False, it will use the list passed as
                   an argument. Defaults to False
      options: The options object that contains the path to the database.
      prev_selected_ata: The ATA code selected in the previous call to this function, to prevent consecutive selections.

    Returns:
      the selected ATA and the point (lat, lon)
    """
    if is_filtered:
        ata_list = df['ATA'].to_list()
    n_vehicles = df['n_vehicles'].to_list()
    abs_n_vehicles = np.abs(n_vehicles)
    total_vehicles = np.sum(abs_n_vehicles)
    n_vehicles_prob = np.true_divide(abs_n_vehicles, total_vehicles)
    n_vehicles_prob = softmax(n_vehicles_prob)  # Apply softmax to smooth probabilities

    # Reduce the probability of previously selected ATA to distribute the selection
    if prev_selected_ata is not None and prev_selected_ata in ata_list:
        index = ata_list.index(prev_selected_ata)
        n_vehicles_prob[index] *= 0.5  # Halve the probability of the previously selected ATA

    point = None
    selected_ata = None
    while point is None:
        selected_ata = np.random.choice(ata_list, 1, p=n_vehicles_prob)
        nodes = get_nodes_from_db(sqlite3.connect(options.traffic_db), selected_ata[0])
        nodes = nodes[0].split(" ")
        if nodes:  # Ensure nodes list is not empty
            node = random.choice(nodes)
            point = rtdm.get_coord_from_node(sqlite3.connect(options.dbPath), node)

    return selected_ata[0], point


def get_coord_for_ata_v_old(df, ata_list, is_filtered=False, options=None):
    """
    The function takes in a dataframe, a list of ATA codes, and a boolean value indicating whether the dataframe is filtered
    or not. It also takes in a dictionary of options. The function returns the selected ATA code and the point (latitude and
    longitude) of the selected ATA

    Args:
      df: The dataframe containing the data
      ata_list: list of ATA codes
      is_filtered: If True, the function will use the ATA list from the dataframe. If False, it will use the list passed as
    an argument. Defaults to False
      options: The options object that contains the path to the database.

    Returns:
      the selected ATA and the point (lat, lon)
    """
    global selected_ata
    if is_filtered:
        ata_list = df['ATA'].to_list()
    n_vehicles = df['n_vehicles'].to_list()
    abs_n_vehicles = np.abs(n_vehicles)
    total_vehicles = np.sum(abs_n_vehicles)
    n_vehicles_prob = np.true_divide(abs_n_vehicles, total_vehicles)
    point = None
    # Selecting a random node from the list of nodes in the selected ATA.
    while point is None:
        selected_ata = np.random.choice(ata_list, 1, p=n_vehicles_prob)
        nodes = get_nodes_from_db(sqlite3.connect(options.traffic_db), selected_ata[0])
        nodes = nodes[0].split(" ")
        node = random.choice(nodes)
        point = rtdm.get_coord_from_node(sqlite3.connect(options.dbPath), node)
    return selected_ata[0], point


def generate_route(options, src_lat, src_lon, dest_lat, dest_lon, callback):
    """
    > Given a set of options, a source latitude and longitude, and a destination latitude and longitude, return a route

    Args:
      options: a dictionary of options for the route.
      src_lat: latitude of the source
      src_lon: longitude of the source
      dest_lat: latitude of destination
      dest_lon: longitude of destination

    Returns:
      A list of tuples, each tuple is a point on the route.
    """
    return rtdm.get_route_from_ABATIS(options, src_lat, src_lon, dest_lat, dest_lon, callback)


def get_n_vehicles_from_db(db, ata="", all_vehicles=False):
    """
    It returns the number of vehicles in a given ATA, or the total number of vehicles in the database if the all_vehicles
    parameter is set to True

    Args:
      db: the database connection
      ata: the ATA code of the aircraft
      all_vehicles: if True, the function will return the total number of vehicles in the database. If False, it will return
    the number of vehicles for a specific ATA. Defaults to False

    Returns:
      The number of vehicles in the database for a given ATA code.
    """

    if not all_vehicles:
        cursor = db.cursor()
        sql_sentence = f'select traffic.n_vehicles from traffic where traffic.ATA="{ata}"'
        cursor.execute(sql_sentence)
        result_row = cursor.fetchall()
        if result_row:
            return result_row[0][0]
        else:
            return None
    else:
        cursor = db.cursor()
        sql_sentence = f'select traffic.n_vehicles from traffic'
        cursor.execute(sql_sentence)
        result_row = cursor.fetchall()
        if result_row:
            return np.sum(np.array(result_row))
        else:
            return None


def get_ATA_list_from_db(db):
    """
    It returns a list of all the ATA codes in the database

    Args:
      db: the database connection

    Returns:
      A list of all the ATA codes in the database.
    """

    cursor = db.cursor()
    sql_sentence = f'select traffic.ATA from traffic'
    cursor.execute(sql_sentence)
    result_row = cursor.fetchall()
    if result_row:
        return [x[0] for x in result_row]
    else:
        return None


def get_ATA_from_db(db, node):
    """
    It takes a database connection and a node name as input, and returns the ATA of the node

    Args:
      db: the database connection
      node: the node name

    Returns:
      A list of tuples.
    """
    cursor = db.cursor()
    sql_sentence = f'select traffic.ATA from traffic where traffic.node like "%{node}%"'
    cursor.execute(sql_sentence)
    result_row = cursor.fetchall()
    if result_row:
        return result_row[0][0]
    else:
        return None


def get_nodes_from_db(db, ata):
    """
    It takes in a database connection and an ATA code, and returns the node associated with that ATA code

    Args:
      db: the database connection
      ata: the ATA code of the aircraft

    Returns:
      A tuple of the node name.
    """
    cursor = db.cursor()
    sql_sentence = f'select traffic.node from traffic where traffic.ATA="{ata}"'
    cursor.execute(sql_sentence)
    result_row = cursor.fetchall()
    if result_row:
        return result_row[0]
    else:
        return None
