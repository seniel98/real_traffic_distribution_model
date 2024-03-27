import sys
import pandas as pd
import sqlite3
import random
import numpy as np
from geopy.distance import geodesic
from numpy.random import randint
from tqdm import tqdm
import sumolib as sumo

sys.path.append("/home/josedaniel/real_traffic_distribution_model")

import real_traffic_distribution_model as rtdm

is_reiterating = False
selection_counts = {}
percentage = 75
tolerated_error = 1.1
MAX_N_VEHICLES = 30000


def is_n_vehicles_ok(conn, ata, df):
    """
    It checks if the number of vehicles for a given ATA is within the tolerated error

    Args:
      conn: Db conn
      ata: The ATA code
      df: the dataframe containing the data

    Returns:
      The function is_n_vehicles_ok() is returning a boolean value.
    """
    n_vehicles = df[df['ATA'] == ata]['n_vehicles'].to_list()
    n_vehicles_copy = get_n_vehicles_from_db(conn, ata)
    if n_vehicles_copy is not None and n_vehicles is not None:
        if abs((n_vehicles_copy - n_vehicles[0]) / float(n_vehicles_copy)) >= tolerated_error:
            return False

    return True


def filter_not_suitable_edges(net, roundabouts):
    not_suitable_edges = []
    not_suitable_edges_set = set()
    for edge in net.getEdges():
        if edge.getID() in roundabouts or edge.getLength() < 75 or edge.getType() == "highway.primary" or edge.getType() == "highway.primary_link" or edge.getType() == "highway.track" or edge.getType() == "highway.motorway_link":
            not_suitable_edges.append(str(edge.getID()))

    not_suitable_edges_set.update(not_suitable_edges)
    return not_suitable_edges_set


def filter_suited_edges(net, roundabouts):
    suited_rows = []  # List to hold all rows of edge_id and coord_node pairs
    for edge in net.getEdges():
        if edge.getID() not in roundabouts and edge.getLength() > 75 and edge.getType() != "highway.primary" and edge.getType() != "highway.primary_link" and edge.getType() != "highway.track" and edge.getType() != "highway.motorway_link":
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


def process_kriging_ata_df(districts_gdf, traffic_df, max_point, min_point, suitable_edges_df):
    kriging_ata_df = apply_kriging(traffic_df=traffic_df, max_point=max_point, min_point=min_point,
                                   districts_gdf=districts_gdf)
    kriging_ata_df = prepare_kriging_df(kriging_ata_df)

    # Join the kriging_ata_df with the suitable_edges_df
    kriging_ata_df = kriging_ata_df.merge(suitable_edges_df, on='coord_node', how='inner')

    # Drop the duplicates on coord_node
    kriging_ata_df.drop_duplicates(subset='coord_node', inplace=True)
    kriging_ata_df.reset_index(drop=True, inplace=True)

    return kriging_ata_df


def apply_kriging(traffic_df, max_point, min_point, districts_gdf):
    min_lat, min_lon = min_point
    max_lat, max_lon = max_point

    traffic_np = rtdm.create_traffic_np_array(traffic_df)

    kriging_df = rtdm.create_kriging_df(traffic_np, min_lat, max_lat, min_lon, max_lon)

    kriging_processed_df = rtdm.process_kriging_df(kriging_df)

    kriging_gdf = rtdm.create_kriging_gdf(kriging_processed_df)

    kriging_ata_df = rtdm.create_kriging_district_df(kriging_gdf, districts_gdf)

    return kriging_ata_df


def process_net_boundaries(net):
    min_lat, max_lat, min_lon, max_lon = rtdm.get_net_boundaries(net)
    max_lon = rtdm.convert_from_180_to360(max_lon)
    min_lon = rtdm.convert_from_180_to360(min_lon)
    return max_lat, max_lon, min_lat, min_lon


def edge_to_coordinates(edge, net):
    # Using the starting node of the edge for simplicity
    x, y = edge.getFromNode().getCoord()
    lon, lat = net.convertXY2LonLat(x, y)
    return round(lat, 5), round(lon, 5)


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
        selection_prob = (selection_prob + population_prob) / 2
        selection_prob /= np.sum(selection_prob)  # Normalize probabilities
    # Penalize selection probabilities based on previous selections
    for i, district in enumerate(district_list):
        if district in selection_counts:
            # Decrease probability for districts that have been selected more often
            # The penalization factor can be adjusted as needed
            penalization_factor = 1 / (1 + selection_counts[district])
            selection_prob[i] *= penalization_factor

    # Ensure probabilities sum to 1 after penalization
    selection_prob /= np.sum(selection_prob)
    selected_district = np.random.choice(district_list, 1, p=selection_prob)
    return selected_district[0]


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

    return selected_point, district


def prepare_kriging_df(df):
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


def calculate_route(src_point, des_point, net, not_suitable_edges):
    src_lat, src_lon = src_point
    des_lat, des_lon = des_point
    src_x, src_y = net.convertLonLat2XY(src_lon, src_lat)
    des_x, des_y = net.convertLonLat2XY(des_lon, des_lat)
    edges_set_start = net.getNeighboringEdges(src_x, src_y, r=30)
    edges_set_end = net.getNeighboringEdges(des_x, des_y, r=30)

    if edges_set_start is not None and edges_set_end is not None and len(edges_set_start) > 0 and len(
            edges_set_end) > 0:
        src_edge = (edges_set_start[randint(0, len(edges_set_start) - 1)][0] if len(edges_set_start) > 1 else
                    edges_set_start[0][0])
        dst_edge = (
            edges_set_end[randint(0, len(edges_set_end) - 1)][0] if len(edges_set_end) > 1 else edges_set_end[0][0])

        src_edge = src_edge.getID()
        dst_edge = dst_edge.getID()

        if src_edge not in not_suitable_edges and dst_edge not in not_suitable_edges:
            fastest = random.random() < 0.8
            path, _ = net.getOptimalPath(net.getEdge(src_edge), net.getEdge(dst_edge),
                                         fastest=fastest)
            # path, _ = net.getOptimalPath(net.getEdge(src_edge), net.getEdge(dst_edge),
            #                              fastest=True)

            if path is not None:
                final_edges = [edge.getID() for edge in path]
                if not sumo.route.getLength(net, final_edges) < 1250:
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


def create_od_routes(options, net):
    """
    It takes a traffic file and a traffic database, and generates a
    routes file and a modified traffic file

    Args:
      net: The sumo network
      options: the options object that contains the path to the database and the path to the traffic file
    """
    global is_reiterating, selection_counts

    route_id_list = []
    route_list = []
    coord_route_list = []
    global_ata_list = []
    routes_count = 0

    traffic_df = pd.read_csv(options.traffic_file)
    districts_df = pd.read_csv(options.districts_file)

    roundabouts = set()

    passenger_cars = 0.72
    vehicle_not_parking = 0.85

    # Apply vectorized operations for efficiency
    traffic_df['n_vehicles'] = (traffic_df['n_vehicles'] * passenger_cars * vehicle_not_parking).astype(int)

    # To csv
    # traffic_df.to_csv("/home/josedaniel/Algoritmo_rutas_eco/TrafficData/way_nodes_relation_adapted.csv", index=False)

    # Crate dbPath and traffic_db connection
    db_path_conn = sqlite3.connect(options.dbPath)
    traffic_db_conn = sqlite3.connect(options.traffic_db)

    # Get the edges for the roundabouts
    for roundabout in net.getRoundabouts():
        roundabouts.update(roundabout.getEdges())

    # Connect to the database once, outside the loop
    total_vehicles_copy = get_n_vehicles_from_db(traffic_db_conn, all_vehicles=True)

    total_vehicles = traffic_df['n_vehicles'].sum()

    not_suitable_edges = filter_not_suitable_edges(net, roundabouts)

    # Use the modified function to get the DataFrame
    suitable_edges_df = filter_suited_edges(net, roundabouts)

    # Make sure coord_node is a string
    suitable_edges_df['coord_node'] = suitable_edges_df['coord_node'].astype(str)

    # Create districts GeoDataFrame
    districts_gdf = rtdm.create_districts_gdf(districts_df)

    # Get the boundaries of the network
    max_lat, max_lon, min_lat, min_lon = process_net_boundaries(net)
    max_point = (max_lat, max_lon)
    min_point = (min_lat, min_lon)

    kriging_ata_df = process_kriging_ata_df(districts_gdf, traffic_df, max_point, min_point, suitable_edges_df)

    kriging_ata_df.to_csv("/home/josedaniel/Algoritmo_rutas_eco/kriging_ata_df_start.csv", index=False)

    # Aggregate the number of vehicles per district
    veh_per_district_df = create_veh_per_district_df(kriging_ata_df)

    # The above code is generating routes for the given percentage of vehicles.
    max_n_vehicles_percentage = int((MAX_N_VEHICLES * (percentage / 100)))

    print(f"Max vehicles percentage: {max_n_vehicles_percentage}")

    pbar = tqdm(total=int((total_vehicles_copy * (percentage / 100))))

    while total_vehicles > int(
            (total_vehicles_copy * (1 - (percentage / 100)))) and routes_count < max_n_vehicles_percentage:
        src_point, src_district = select_origin_destination_from_kriging(options, kriging_ata_df,
                                                                         veh_per_district_df)

        # Make sure column coord_node is a string
        traffic_df['coord_node'] = traffic_df['coord_node'].astype(str)

        des_point, des_district = select_origin_destination_from_kriging(options, kriging_ata_df,
                                                                         veh_per_district_df, is_dest=True,
                                                                         src_point=src_point,
                                                                         src_district=src_district)
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

                # Update selection_counts for the involved districts
                if src_district in selection_counts:
                    selection_counts[src_district] += 1
                else:
                    selection_counts[src_district] = 1

                if des_district in selection_counts:
                    selection_counts[des_district] += 1
                else:
                    selection_counts[des_district] = 1
                route_id_list.append(f'{edges_route[0]}_to_{edges_route[-1]}')
                route_list.append(edges_route)
                coord_route_list.append(coords_route)
                global_ata_list.append(route_ata_list)

                # Update vehicles count and execution time
                total_vehicles -= len(route_ata_list)

                # Generate a new veh_per_district_df every 1000 routes
                if routes_count % 1000 == 0 and routes_count != 0:
                    kriging_ata_df = process_kriging_ata_df(districts_gdf, traffic_df, max_point, min_point,
                                                            suitable_edges_df)
                    veh_per_district_df = create_veh_per_district_df(kriging_ata_df)

                if routes_count == max_n_vehicles_percentage - 1:
                    kriging_ata_df.to_csv("/home/josedaniel/Algoritmo_rutas_eco/kriging_ata_df_end.csv", index=False)

                pbar.update(len(route_ata_list))

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


def create_veh_per_district_df(kriging_ata_df):
    veh_per_district_df = kriging_ata_df.groupby('district_code').agg(
        {'n_vehicles': 'mean', 'population': 'first'}).reset_index()
    return veh_per_district_df
