import heapq
import sys
from shapely.geometry import Point, Polygon
import pandas as pd
import sqlite3
import random
import numpy as np
from geopy.distance import geodesic
from geopy.point import Point
import json
from numpy.random import randint
import ast
from tqdm import tqdm
import sumolib as sumo

sys.path.append("/home/josedaniel/real_traffic_distribution_model")

import real_traffic_distribution_model as rtdm

is_reiterating = False

percentage = 50
tolerated_error = 1.1
MAX_N_VEHICLES = 30000

primary_count = 0


# Convert the 'geo_shape' column to geometries
def parse_polygon(geojson_str):
    try:
        if geojson_str == "0":
            return None
        else:
            return Polygon(json.loads(geojson_str)['coordinates'][0])
    except (ValueError, KeyError):
        return None


# Convert 'coord_node' strings to Point geometries
def parse_point(coord_str):
    try:
        lat, lon = map(float, coord_str.strip('()').split(','))
        return Point(lon, lat)  # Ensure it's (longitude, latitude)
    except ValueError:
        return None


def is_n_vehicles_ok(conn, ata, df, is_src_or_des=False):
    """
    It checks if the number of vehicles for a given ATA is within the tolerated error

    Args:
      conn: Db conn
      ata: The ATA code
      df: the dataframe containing the data
      is_src_or_des: If True, then the function is being called for the source or destination ATA. If False, then the
    function is being called for an intermediate ATA. Defaults to False

    Returns:
      The function is_n_vehicles_ok() is returning a boolean value.
    """
    n_vehicles = df[df['ATA'] == ata]['n_vehicles'].to_list()
    n_vehicles_copy = get_n_vehicles_from_db(conn, ata)
    if n_vehicles_copy is not None and n_vehicles is not None:
        # if (is_src_or_des and abs((n_vehicles_copy - n_vehicles[0]) / float(n_vehicles_copy)) >= 1.0):
        #     return False
        if abs((n_vehicles_copy - n_vehicles[0]) / float(n_vehicles_copy)) >= tolerated_error:
            return False

    return True


def filter_not_suitable_edges(net, roundabouts):
    not_suitable_edges = []
    not_suitable_edges_set = set()
    for edge in net.getEdges():
        if edge.getID() in roundabouts or edge.getLength() < 75 or edge.getType() == "highway.primary_link" or edge.getType() == "highway.track" or edge.getType() == "highway.motorway_link":
            not_suitable_edges.append(str(edge.getID()))

    not_suitable_edges_set.update(not_suitable_edges)
    return not_suitable_edges_set


def filter_suited_edges(net, roundabouts):
    suited_rows = []  # List to hold all rows of edge_id and coord_node pairs
    for edge in net.getEdges():
        if edge.getID() not in roundabouts and edge.getLength() > 75 and edge.getType() != "highway.primary_link" and edge.getType() != "highway.track" and edge.getType() != "highway.motorway_link":
            edge_id = str(edge.getID())
            for coord in edge.getRawShape():
                # Convert the coordinates to lat, lon
                lon, lat = net.convertXY2LonLat(coord[0], coord[1])
                # Round the coordinates to 5 decimal places
                lat = round(lat, 4)
                lon = round(lon, 4)
                suited_rows.append({'edge_id': edge_id, 'coord_node': (lat, lon)})
            for coord in edge.getShape():
                # Convert the coordinates to lat, lon
                lon, lat = net.convertXY2LonLat(coord[0], coord[1])
                # Round the coordinates to 5 decimal places
                lat = round(lat, 4)
                lon = round(lon, 4)
                suited_rows.append({'edge_id': edge_id, 'coord_node': (lat, lon)})
            # Append from node
            x, y = edge.getFromNode().getCoord()
            lon, lat = net.convertXY2LonLat(x, y)
            suited_rows.append({'edge_id': edge_id, 'coord_node': (round(lat, 4), round(lon, 4))})
            # Append to node
            x, y = edge.getToNode().getCoord()
            lon, lat = net.convertXY2LonLat(x, y)
            suited_rows.append({'edge_id': edge_id, 'coord_node': (round(lat, 4), round(lon, 4))})

    return pd.DataFrame(suited_rows)


def edge_to_coordinates(edge, net):
    # Using the starting node of the edge for simplicity
    x, y = edge.getFromNode().getCoord()
    lon, lat = net.convertXY2LonLat(x, y)
    return round(lat, 5), round(lon, 5)


def select_edge(suitable_edges):
    return random.choice(suitable_edges)


def get_district_of_ATA(ata, df):
    if ata:
        return df[df['ATA'] == ata]['district_code'].values[0]
    else:
        return None


def select_district(df, consider_population=False, src_district=None):
    # if src_district is not None:
    #    df = df[df['district_code'] != src_district]
    district_list = df['district_code'].to_list()
    n_vehicles = df['n_vehicles'].to_list()
    abs_n_vehicles = np.abs(n_vehicles)
    total_vehicles = np.sum(abs_n_vehicles)
    selection_prob = np.true_divide(abs_n_vehicles, total_vehicles)

    if consider_population and 'population' in df.columns:
        population = df['population'].to_list()
        abs_population = np.abs(population)
        total_population = np.sum(abs_population)
        population_prob = np.true_divide(abs_population, total_population)

        # Adjust selection probabilities by combining vehicle and population factors
        selection_prob = (selection_prob + population_prob*2) / 2
        selection_prob /= np.sum(selection_prob)  # Normalize probabilities

    selected_district = np.random.choice(district_list, 1, p=selection_prob)
    return selected_district[0]


def filter_distance(df, src_point, src_district=None):
    df_coord_filtered = df.copy()

    for index, row in df.iterrows():
        # 1,25 km is based on the assumption that a person walks 1,25 km in 15 minutes and in bicycle 1,25 km in 6 minutes
        if geodesic(src_point, eval(row['coord_node'])).m < 1250:
            df_coord_filtered.drop(index, axis=0, inplace=True)
        # # Drop all the nodes that are in the same district as the source
        # if src_district and row['district_code'] == src_district:
        #     df_coord_filtered.drop(index, axis=0, inplace=True)

    if len(df_coord_filtered) > 0:
        return True


def select_point_from_kriging(df):
    coord_node_list = df['coord_node'].to_list()
    n_vehicles = df['n_vehicles'].to_list()
    total_vehicles = np.sum(n_vehicles)
    n_vehicles_prob = np.true_divide(n_vehicles, np.abs(total_vehicles))
    selected_points = np.random.choice(coord_node_list, 1, p=n_vehicles_prob)
    return eval(selected_points[0])


def select_origin_destination_from_kriging(options, kriging_df, veh_per_district_df, is_dest=False, src_point=None,
                                           src_district=None):
    global selected_point
    df_district_filtered = kriging_df.copy()

    if not is_dest:
        # Select district
        district = select_district(veh_per_district_df, consider_population=True)

        # Filter the dataframe by the selected district
        df_district_filtered = df_district_filtered[df_district_filtered['district_code'] == district]
        selected_point = select_point_from_kriging(df_district_filtered)
    else:
        # Select district
        district = select_district(veh_per_district_df, consider_population=False)

        # Filter the dataframe by the selected district
        df_district_filtered = df_district_filtered[df_district_filtered['district_code'] == district]
        # Selecting destination
        df_coord_filtered = df_district_filtered.copy()

        distance = 0
        iteration = 0
        while distance < 1250 and iteration < 1000:
            selected_point = select_point_from_kriging(df_coord_filtered)
            distance = geodesic(src_point, selected_point).m
            iteration += 1

        # for index, row in df_district_filtered.iterrows():
        #     # 1,25 km is based on the assumption that a person walks 1,25 km in 15 minutes and in bicycle 1,25 km in 6 minutes
        #     if geodesic(src_point, eval(row['coord_node'])).m < 1250:
        #         df_coord_filtered.drop(index, axis=0, inplace=True)
        #
        # if len(df_coord_filtered) > 0:
        #     selected_point = select_point_from_kriging(df_coord_filtered)
        # else:
        #     return None, None, None

    return selected_point, district


def select_origin_destination(options, df, veh_per_district_df, is_dest=False, src_point=None, src_district=None):
    if not is_dest:
        df_district_filtered = df.copy()
        # Select district
        district = select_district(veh_per_district_df)
        # Filter the dataframe by the selected district
        df_district_filtered = df_district_filtered[df_district_filtered['district_code'] == district]
        # Selecting origin
        ata, point = select_point(options, df_district_filtered)
    else:
        df_district_filtered = df.copy()
        # Select district
        district = select_district(veh_per_district_df, src_district=src_district)
        # Filter the dataframe for the selected district
        df_district_filtered = df_district_filtered[df_district_filtered['district_code'] == district]
        # Selecting destination
        df_coord_filtered = df_district_filtered.copy()

        for index, row in df_district_filtered.iterrows():
            # 1,25 km is based on the assumption that a person walks 1,25 km in 15 minutes and in bicycle 1,25 km in 6 minutes
            if geodesic(src_point, eval(row['coord_node'])).m < 1250:
                df_coord_filtered.drop(index, axis=0, inplace=True)
            # # Drop all the nodes that are in the same district as the source
            # if src_district and row['district_code'] == src_district:
            #     df_coord_filtered.drop(index, axis=0, inplace=True)

        if len(df_coord_filtered) > 0:
            ata, point = select_point(options, df=df_coord_filtered)
        else:
            return None, None, None
    return ata, point, district


def prepare_kriging_df(df):
    # Convert string representations of tuples in 'coord_node' to actual tuples
    df['coord_node'] = df['coord_node'].apply(ast.literal_eval)

    # Round the coordinates to 6 decimal places
    df['coord_node'] = df['coord_node'].apply(lambda x: (round(x[0], 4), round(x[1], 4)))

    # Join the duplicate the rows with the same coordinates
    df = df.groupby('coord_node').agg(
        {'n_vehicles': 'mean', 'district_code': 'first', 'population': 'first'}).reset_index()

    # Make n_vehicles an integer
    df['n_vehicles'] = df['n_vehicles'].astype(int)

    # Make sure coord node is a string
    df['coord_node'] = df['coord_node'].astype(str)

    return df


def create_od_routes(options, net):
    """
    It takes a traffic file and a traffic database, and generates a
    routes file and a modified traffic file

    Args:
      options: the options object that contains the path to the database and the path to the traffic file
    """
    global exec_time_start, is_reiterating, primary_count

    traffic_df = pd.read_csv(options.traffic_file)

    kriging_ata_df = pd.read_csv(options.kriging_ata_file)

    kriging_ata_df = prepare_kriging_df(kriging_ata_df)

    roundabouts = set()

    passenger_cars = 0.72
    vehicle_not_parking = 0.85

    # Apply vectorized operations for efficiency
    traffic_df['n_vehicles'] = (traffic_df['n_vehicles'] * passenger_cars * vehicle_not_parking).astype(int)

    # To csv
    traffic_df.to_csv("/home/josedaniel/Algoritmo_rutas_eco/TrafficData/way_nodes_relation_adapted.csv", index=False)

    # Crate dbPath and traffic_db connection
    db_path_conn = sqlite3.connect(options.dbPath)
    traffic_db_conn = sqlite3.connect(options.traffic_db)

    # # Copy the original traffic dataframe to compare it later
    # real_traffic_df = traffic_df.copy()

    # # Sort real_traffic and traffic_df by ATA
    # traffic_df.sort_values(by='ATA', inplace=True)
    # real_traffic_df.sort_values(by='ATA', inplace=True)
    # Get the edges for the roundabouts
    for roundabout in net.getRoundabouts():
        roundabouts.update(roundabout.getEdges())

    # Connect to the database once, outside the loop

    total_vehicles_copy = get_n_vehicles_from_db(traffic_db_conn, all_vehicles=True)

    total_vehicles = traffic_df['n_vehicles'].sum()

    route_id_list = []
    route_list = []
    coord_route_list = []
    exec_time_list = []
    global_ata_list = []
    routes_count = 0
    departures_per_district = {}  # Tracks vehicles departing from each district
    arrivals_per_district = {}  # Tracks vehicles arriving at each district
    total_exec_time = 0

    not_suitable_edges = filter_not_suitable_edges(net, roundabouts)

    # Use the modified function to get the DataFrame
    suitable_edges_df = filter_suited_edges(net, roundabouts)

    # Make sure coord_node is a string
    suitable_edges_df['coord_node'] = suitable_edges_df['coord_node'].astype(str)

    # Join the kriging_ata_df with the suitable_edges_df
    kriging_ata_df = kriging_ata_df.merge(suitable_edges_df, on='coord_node', how='inner')

    # Drop the duplicates on coord_node
    kriging_ata_df.drop_duplicates(subset='coord_node', inplace=True)

    kriging_ata_df.reset_index(drop=True, inplace=True)

    kriging_ata_df.to_csv("/home/josedaniel/Algoritmo_rutas_eco/kriging_ata_df.csv", index=False)

    # Aggregate the number of vehicles per district
    veh_per_district_df = kriging_ata_df.groupby('district_code').agg(
        {'n_vehicles': 'mean', 'population': 'first'}).reset_index()
    # veh_per_district_df = traffic_df.groupby('district_code').agg(
    #     {'n_vehicles': 'mean', 'population': 'first'}).reset_index()

    # print(veh_per_district_df)

    # The above code is generating routes for the given percentage of vehicles.
    max_n_vehicles_percentage = int((MAX_N_VEHICLES * ((percentage / 100))))
    print(f"Max vehicles percentage: {max_n_vehicles_percentage}")
    pbar = tqdm(total=int((total_vehicles_copy * ((percentage / 100)))))
    # pbar_vehicles = tqdm(total=max_n_vehicles_percentage)
    while total_vehicles > int(
            (total_vehicles_copy * (1 - (percentage / 100)))) and routes_count < max_n_vehicles_percentage:
        # if not is_reiterating:
        #     exec_time_start = datetime.now()

        src_point, src_district = select_origin_destination_from_kriging(options, kriging_ata_df,
                                                                         veh_per_district_df)
        # if src_point is None:
        #     is_reiterating = True
        #     continue

        # Make sure column coord_node is a string
        traffic_df['coord_node'] = traffic_df['coord_node'].astype(str)

        des_point, des_district = select_origin_destination_from_kriging(options, kriging_ata_df,
                                                                         veh_per_district_df, is_dest=True,
                                                                         src_point=src_point,
                                                                         src_district=src_district)
        # if des_point is not None:
        coords_route, nodes_route, edges_route = calculate_route(src_point, des_point, net, not_suitable_edges)

        # Check if any of the route components is not None to proceed
        if any(route is not None for route in [coords_route, nodes_route, edges_route]):
            route_ata_list = set()
            is_n_vehicles_exceeded = False

            for i, node in enumerate(nodes_route):
                ata = get_ATA_from_db(traffic_db_conn, node, edges_route[i // 2],
                                      coords_route[i])
                if ata is not None:
                    if is_n_vehicles_ok(traffic_db_conn, ata, traffic_df):
                        route_ata_list.add(ata)
                    else:
                        is_n_vehicles_exceeded = True
                        break

            if not is_n_vehicles_exceeded:
                for i, ata in enumerate(route_ata_list):
                    row_index = traffic_df.loc[traffic_df['ATA'] == ata].index
                    traffic_df.at[row_index[0], 'n_vehicles'] -= 1
                    # district = traffic_df.at[row_index[0], 'district_code']
                    # row_index_district = veh_per_district_df.loc[
                    #     veh_per_district_df['district_code'] == district].index
                    # veh_per_district_df.at[row_index_district[0], 'n_vehicles'] -= 1

                route_id_list.append(f'{edges_route[0]}_to_{edges_route[-1]}')
                route_list.append(edges_route)
                coord_route_list.append(coords_route)
                global_ata_list.append(route_ata_list)

                # Update vehicles count and execution time
                total_vehicles -= len(route_ata_list)
                # exec_time_end = datetime.now()
                # exec_time = exec_time_end.timestamp() * 1000 - exec_time_start.timestamp() * 1000
                # total_exec_time += exec_time
                # exec_time_list.append(exec_time)

                pbar.update(len(route_ata_list))
                # pbar_vehicles.update(len(route_list))

                is_reiterating = False
                routes_count += 1

    db_path_conn.close()
    traffic_db_conn.close()
    print(f'Total v{percentage}p_{tolerated_error} routes generated: {len(route_list)}')
    gen_routes_data = {'route_id': route_id_list, 'route': route_list, 'coord': coord_route_list,
                       'ATA': global_ata_list}
    gen_routes_df = pd.DataFrame.from_dict(gen_routes_data)
    gen_routes_df.to_csv(
        f'/home/josedaniel/Modelo_distrib_trafico_real/routes_data/gen_routes_data_{str(percentage)}p_{str(tolerated_error)}_net_edited_kr.csv',
        index=False)
    gen_routes_df.drop(columns={"coord"}, inplace=True)
    gen_routes_df_clean = gen_routes_df['route_id'].value_counts().to_frame().reset_index()
    gen_routes_df_clean.rename(columns={'index': 'route_id', 'route_id': 'n_vehicles'}, inplace=True)
    gen_routes_df_clean.to_csv(
        f'/home/josedaniel/Modelo_distrib_trafico_real/veh_per_route/routes_{str(percentage)}p_{str(tolerated_error)}_net_edited_kr.csv',
        index=False)
    traffic_df.to_csv(
        f'/home/josedaniel/Modelo_distrib_trafico_real/traffic_data/csv/traffic_df_modified_{str(percentage)}p_{str(tolerated_error)}_net_edited_kr.csv',
        index=False)
    pbar.close()


def calculate_route(src_point, des_point, net, not_suitable_edges):
    src_lat, src_lon = src_point
    des_lat, des_lon = des_point
    src_x, src_y = net.convertLonLat2XY(src_lon, src_lat)
    des_x, des_y = net.convertLonLat2XY(des_lon, des_lat)
    edges_set_start = net.getNeighboringEdges(src_x, src_y, r=20)
    edges_set_end = net.getNeighboringEdges(des_x, des_y, r=20)

    if edges_set_start is not None and edges_set_end is not None and len(edges_set_start) > 0 and len(
            edges_set_end) > 0:
        src_edge = (edges_set_start[randint(0, len(edges_set_start) - 1)][0] if len(edges_set_start) > 1 else
                    edges_set_start[0][0])
        dst_edge = (
            edges_set_end[randint(0, len(edges_set_end) - 1)][0] if len(edges_set_end) > 1 else edges_set_end[0][0])

        src_edge = src_edge.getID()
        dst_edge = dst_edge.getID()

        if net.getEdge(src_edge).getType() != "highway.primary" and src_edge not in not_suitable_edges and dst_edge not in not_suitable_edges:

            path, _ = net.getOptimalPath(net.getEdge(src_edge), net.getEdge(dst_edge), fastest=random.choice([True, False]))
            # path, _ = net.getOptimalPath(net.getEdge(src_edge), net.getEdge(dst_edge),
            #                              fastest=True)

            if path is not None:
                final_edges = [edge.getID() for edge in path]
                # if not sumolib.route.getLength(net, final_edges) < 1250:
                final_nodes = [node.getID() for edge in path for node in (edge.getFromNode(), edge.getToNode())]
                final_coords = [net.convertXY2LonLat(*net.getNode(node).getCoord()) for node in final_nodes]
                # Round the coordinates to 5 decimal places
                final_coords = [(round(coord[0], 5), round(coord[1], 5)) for coord in final_coords]
                # Change the order of the coordinates to (lat, lon)
                final_coords = [(coord[1], coord[0]) for coord in final_coords]
                return final_coords, final_nodes, final_edges
    else:
        return None, None, None

    return None, None, None


def process_route(data):
    return data


def select_point(options, traffic_db_conn, db_path_conn, df=None):
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

    ata, point = get_coord_for_ata(df, traffic_db_conn, db_path_conn)
    return ata, point


def get_coord_for_ata(df, traffic_db_conn, db_path_conn):
    """
    The function takes in a dataframe, a list of ATA codes, and a boolean value indicating whether the dataframe is filtered
    or not. It also takes in a dictionary of options. The function returns the selected ATA code and the point (latitude and
    longitude) of the selected ATA

    Args:
      df: The dataframe containing the data
      options: The options object that contains the path to the database.

    Returns:
      the selected ATA and the point (lat, lon)
    """
    global selected_ata
    ata_list = df['ATA'].to_list()
    n_vehicles = df['n_vehicles'].to_list()
    abs_n_vehicles = np.abs(n_vehicles)
    total_vehicles = np.sum(abs_n_vehicles)
    n_vehicles_prob = np.true_divide(abs_n_vehicles, total_vehicles)
    point = None
    # Max iterations to avoid infinite loop
    for i in range(0, 1000):
        selected_ata = np.random.choice(ata_list, 1, p=n_vehicles_prob)
        nodes = get_nodes_from_db(traffic_db_conn, selected_ata[0])
        nodes = nodes[0].split(" ")
        node = random.choice(nodes)
        point = rtdm.get_coord_from_node(db_path_conn, node)
        if point:
            break

    # Generate a new destination point within 500m radius of the selected point
    # new_point = generate_random_point(point, max_distance=500)
    # new_ata = get_ATA_from_db(traffic_db_conn,
    #                           rtdm.coord_2_node(db_path_conn, new_point[0], new_point[1]))
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


def get_ATA_from_db(db, node, edge=None, coord_node=None):
    """
    It takes a database connection and a node name as input, and returns the ATA of the node

    Args:
      db: the database connection
      node: the node name

    Returns:
      A list of tuples.
    """
    cursor = db.cursor()
    sql_sentence = f'select traffic.ATA from traffic where traffic.node like "%{str(node)}%"'
    cursor.execute(sql_sentence)
    ata_row = cursor.fetchall()
    if len(ata_row) > 1:
        for ata in ata_row:
            if edge is not None and coord_node is not None:
                # Remove evertything after the first # if there is a # in the edge name
                sql_sentence = f'select traffic.edge,traffic.coord_node from traffic where traffic.ATA like "%{ata[0]}%"'
                cursor.execute(sql_sentence)
                result_row = cursor.fetchall()
                edges_row = eval(result_row[0][0])
                coord_node_db = result_row[0][1]
                for edge_id in edges_row:
                    edge_or_coord_node = edge_id == edge or coord_node_db == str(coord_node)
                    if edges_row and edge_or_coord_node:
                        return ata_row[0][0]
    elif len(ata_row) == 1:
        return ata_row[0][0]
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
