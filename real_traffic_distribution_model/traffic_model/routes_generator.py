import sys
import pandas as pd
import sqlite3
import random
import numpy as np
from geopy.distance import geodesic
from datetime import datetime

sys.path.append("/home/josedaniel/real_traffic_distribution_model")

import real_traffic_distribution_model as rtdm

EDGE_ATA = ['A72', 'A59', 'A58', 'A413', 'A409', 'A392', 'A373', 'A30', 'A298', 'A297', 'A296', 'A287', 'A257', 'A1']

is_reiterating = False


# def check_distance_reroute(options, mid_point, des_ata, des_lat, des_lon, des_node_type,
#                            distance_src_mid, distance_mid_des, df):
#     if distance_src_mid > 1000.0:
#         while (distance_src_mid + distance_mid_des) < 1200.0:
#             des_ata, des_point = select_point(options, des_node_type, df)
#
#             des_lat, des_lon = des_point
#
#             distance_mid_des = geodesic(mid_point, des_point).m
#
#             # print(f'Distance between points: {distance_src_mid + distance_mid_des}')
#     else:
#         while (distance_src_mid + distance_mid_des) < 2000.0:
#             des_ata, des_point = select_point(options, des_node_type, df)
#
#             des_lat, des_lon = des_point
#
#             distance_mid_des = geodesic(mid_point, des_point).m
#
#             # print(f'Distance between points: {distance_src_mid + distance_mid_des}')
#
#     return des_ata, des_lat, des_lon


def check_distance(options, src_ata, src_lat, src_lon, src_node_type, des_ata, des_lat, des_lon, des_node_type,
                   distance, df):
    if src_node_type == "city" and des_node_type == "city":
        while distance < 1000.0:
            src_ata, src_point = select_point(options, src_node_type, df)
            des_ata, des_point = select_point(options, des_node_type, df)

            src_lat, src_lon = src_point
            des_lat, des_lon = des_point

            distance = geodesic(src_point, des_point).m

    else:
        while distance < 2000.0:
            src_ata, src_point = select_point(options, src_node_type, df)
            des_ata, des_point = select_point(options, des_node_type, df)

            src_lat, src_lon = src_point
            des_lat, des_lon = des_point

            distance = geodesic(src_point, des_point).m

    return src_ata, src_lat, src_lon, des_ata, des_lat, des_lon


def is_n_vehicles_ok(options, ata, df, is_src_or_des=False):
    n_vehicles = df[df['ATA'] == ata]['n_vehicles'].to_list()
    n_vehicles_copy = get_n_vehicles_from_db(sqlite3.connect(options.traffic_db), ata)
    if n_vehicles and n_vehicles_copy:
        if float(n_vehicles_copy) != 0.0:
            if is_src_or_des and abs((n_vehicles_copy - n_vehicles[0]) / float(n_vehicles_copy)) >= 1.0:
                print("Number of vehicles for ATA source or destination exceeds limit")
                return False
            elif not is_src_or_des and abs((n_vehicles_copy - n_vehicles[0]) / float(n_vehicles_copy)) >= 2.0:
                print("Number of vehicles for ATA intermediate exceeds limit")
                return False
            else:
                return True
        else:
            return False
    else:
        return True


def create_od_routes(options):
    global exec_time_start, is_reiterating

    node_type_list = ['edge', 'city']
    traffic_df = pd.read_csv(options.traffic_file)
    traffic_df_copy = traffic_df.copy()

    total_vehicles = np.sum(traffic_df['n_vehicles'].to_numpy())
    total_vehicles_copy = get_n_vehicles_from_db(sqlite3.connect(options.traffic_db), all_vehicles=True)

    route_id_list = []
    route_list = []
    coord_route_list = []
    exec_time_list = []

    while total_vehicles > int(total_vehicles_copy * 0.5):
        if not is_reiterating:
            exec_time_start = datetime.now()
        # route_is_possible = False
        src_node = np.random.choice(node_type_list, 1, p=[0.6, 0.4])
        des_node = np.random.choice(node_type_list, 1, p=[0.4, 0.6])

        src_ata, src_point = select_point(options, src_node[0], traffic_df)
        des_ata, des_point = select_point(options, des_node[0], traffic_df)

        src_lat, src_lon = src_point
        des_lat, des_lon = des_point

        distance = geodesic(src_point, des_point).m

        src_ata, src_lat, src_lon, des_ata, des_lat, des_lon = check_distance(options, src_ata, src_lat, src_lon,
                                                                              src_node[0],
                                                                              des_ata,
                                                                              des_lat, des_lon, des_node[0],
                                                                              distance,
                                                                              traffic_df)

        # print(src_lat, src_lon)
        coord_route = generate_route(options, src_lat, src_lon, des_lat, des_lon)

        nodes_route, edges_route = rtdm.coordinates_to_edge(options, sqlite3.connect(options.dbPath),
                                                            coord_route)

        route_ata_list = []
        if is_n_vehicles_ok(options, src_ata, traffic_df, is_src_or_des=True) and is_n_vehicles_ok(options,
                                                                                                   des_ata,
                                                                                                   traffic_df,
                                                                                                   is_src_or_des=True):
            route_ata_list.append(src_ata)
            for i in range(0, len(nodes_route) - 1):
                node_int = nodes_route[i][0]
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
                        coord_route_list.append(coord_route)
                        total_vehicles = np.sum(traffic_df['n_vehicles'].to_numpy())

                        vehicles_remaining = total_vehicles - int(total_vehicles_copy * 0.5)
                        print(f'Vehicles remaining: {vehicles_remaining}')
                        print(f'Total routes generated: {len(route_list)}')
                        # route_is_possible = True
                        exec_time_end = datetime.now()
                        exec_time = exec_time_end.timestamp() * 1000 - exec_time_start.timestamp() * 1000
                        exec_time_list.append(exec_time)
                        print(f'Execution time: {exec_time}')

                        is_reiterating = False
                else:
                    is_reiterating = True
                    break

            # if not route_is_possible:
            #     print("Generating re-route...")
            #     des_node = np.random.choice(node_type_list, 1, p=[0.3, 0.7])
            #     des_ata, des_point = select_point(options, des_node[0], traffic_df)
            #     lon_mid, lat_mid = coord_route[(len(route_ata_list) - 1)]
            #     distance_src_mid_point = geodesic(src_point, (lat_mid, lon_mid)).m
            #     distance_mid_point_des = geodesic((lat_mid, lon_mid), des_point).m
            #     des_ata, des_lat, des_lon = check_distance_reroute(options, (lat_mid, lon_mid), des_ata, des_lat,
            #                                                        des_lon, des_node[0],
            #                                                        distance_src_mid_point, distance_mid_point_des,
            #                                                        traffic_df)
            #     coord_mid_route = generate_route(options, lat_mid, lon_mid, des_lat, des_lon)
            #     size_coord_route = len(coord_route)
            #     del coord_route[len(route_ata_list) - 1:size_coord_route]
            #     coord_re_route = coord_route + coord_mid_route
            #     nodes_re_route, edges_re_route = rtdm.coordinates_to_edge(options, sqlite3.connect(options.dbPath),
            #                                                               coord_re_route)
            #
            #     iterate_mid_points(True, exec_time_start, coord_re_route, coord_route_list, des_ata,
            #                        edges_re_route,
            #                        nodes_re_route, route_ata_list, route_id_list,
            #                        route_is_possible, route_list, src_ata, total_vehicles_copy, traffic_df,
            #                        traffic_df_copy)

        else:
            is_reiterating = True
            continue

    # print(f'Route id: {edges_route[0]}_to_{edges_route[len(edges_route) - 1]}')
    # print(f'Route: {edges_route}')
    gen_routes_data = {'route_id': route_id_list, 'route': route_list, 'coord': coord_route_list,
                       'exec_time': exec_time_list}
    pd.DataFrame.from_dict(gen_routes_data).to_csv("/home/josedaniel/gen_routes_data_50p_2.csv", index=False)
    traffic_df.to_csv("/home/josedaniel/traffic_df_modified_50p_2.csv", index=False)


# def get_ATA_from_node(options, node_id, df):
#     df_nodes = df[df.node.str.contains(str(node_id), case=False)]
#     if not df_nodes.empty:
#         # ata_list = df_nodes['ATA'].to_list()
#         # ata = ata_list[0]
#         ata = get_ATA_from_db(sqlite3.connect(options.traffic_db), )
#         print(ata)
#         return ata
#     else:
#         return None


# def get_traffic_df_from_csv(options):
#     # traffic_df = pd.read_csv(options.traffic_file)
#     traffic_df = pd.read_csv(options.traffic_file)
#     traffic_df.drop('time', inplace=True, axis=1)
#     aggregation_functions = {'ATA': 'first', 'desc': 'first', 'n_vehicles': 'sum', 'way_id': 'first', 'node': 'first'}
#     df = traffic_df.groupby(traffic_df['ATA']).aggregate(aggregation_functions)
#     # df.to_csv("/home/josedaniel/way_nodes_relation_merged.csv")
#     return df


def select_point(options, node_type, df):
    if node_type == 'edge':
        ata_df = df[df['ATA'].isin(EDGE_ATA)]
        ata, point = get_coord_for_ata(ata_df, options)
        return ata, point

    elif node_type == 'city':
        ata_df = df[~df['ATA'].isin(EDGE_ATA)]
        ata, point = get_coord_for_ata(ata_df, options)
        return ata, point

    # # ata_list = df['ATA'].to_list()
    # ata, point = get_coord_for_ata(df, options)
    # while point is None:
    #     ata, point = get_coord_for_ata(df, options)
    # return ata, point


def get_coord_for_ata(df, options):
    global selected_ata
    ata_list = df['ATA'].to_list()
    n_vehicles = df['n_vehicles'].to_list()
    abs_n_vehicles = np.abs(n_vehicles)
    total_vehicles = np.sum(abs_n_vehicles)
    n_vehicles_prob = np.true_divide(abs_n_vehicles, total_vehicles)
    # Select one ATA based on its probability
    # print(f'Sum of probabilities: {round(sum(n_vehicles_prob), 2)}')
    point = None
    while point is None:
        selected_ata = np.random.choice(ata_list, 1, p=n_vehicles_prob)
        nodes = get_nodes_from_db(sqlite3.connect(options.traffic_db), selected_ata[0])
        nodes = nodes[0].split(" ")
        node = random.choice(nodes)
        point = rtdm.get_coord_from_node(sqlite3.connect(options.dbPath), node)
    return selected_ata[0], point


def generate_route(options, src_lat, src_lon, dest_lat, dest_lon):
    return rtdm.get_route_from_ABATIS(options, src_lat, src_lon, dest_lat, dest_lon)


def get_n_vehicles_from_db(db, ata="", all_vehicles=False):
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


def get_ATA_from_db(db, node):
    cursor = db.cursor()
    sql_sentence = f'select traffic.ATA from traffic where traffic.node like "%{node}%"'
    cursor.execute(sql_sentence)
    result_row = cursor.fetchall()
    if result_row:
        return result_row[0][0]
    else:
        return None


def get_nodes_from_db(db, ata):
    cursor = db.cursor()
    sql_sentence = f'select traffic.node from traffic where traffic.ATA="{ata}"'
    cursor.execute(sql_sentence)
    result_row = cursor.fetchall()
    if result_row:
        return result_row[0]
    else:
        return None
