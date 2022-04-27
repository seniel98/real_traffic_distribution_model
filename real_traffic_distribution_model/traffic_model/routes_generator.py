import sys
import pandas as pd
import sqlite3
import random
from xml.etree import ElementTree

sys.path.append("/home/josedaniel/real_traffic_distribution_model")

import real_traffic_distribution_model as rtdm

edge_valencia_ata = ['A72', 'A59', 'A58', 'A413', 'A409', 'A392', 'A373', 'A30', 'A298', 'A297', 'A296', 'A287',
                     'A257', 'A1']


def check_distance(options, src_ata, src_lat, src_lon, des_ata, des_lat, des_lon, distance, df):
    while distance < 2000.0:
        src_ata, src_point = select_point(options, 'edge', df)
        des_ata, des_point = select_point(options, 'city', df)

        src_lat, src_lon = src_point
        des_lat, des_lon = des_point

        distance = rtdm.distance_2_points(float(src_lat), float(src_lon), float(des_lat), float(des_lon))
        # print(distance)

    return src_ata, src_lat, src_lon, des_ata, des_lat, des_lon


def create_od_routes(options):
    traffic_df_8_15 = get_traffic_df_from_csv(options, '08:15:00')
    # traffic_df_8_30 = get_traffic_df_from_csv(options, '08:30:00')
    # traffic_df_8_45 = get_traffic_df_from_csv(options, '08:45:00')
    # traffic_df_9_00 = get_traffic_df_from_csv(options, '09:00:00')
    traffic_df_8_15.to_csv("/home/josedaniel/traffic_8_15_default.csv", index=False)
    total_vehicles = sum(traffic_df_8_15['n_vehicles'].to_list())

    # 50% vehicles from edge to city
    # 30% from city to edge
    # 25% from city to city
    # 5% from edge to edge

    route_id_list = []
    route_list = []

    for i in range(0, int(total_vehicles * 0.01)):
        j = i + 1
        src_ata, src_point = select_point(options, 'edge', traffic_df_8_15)
        des_ata, des_point = select_point(options, 'city', traffic_df_8_15)

        src_lat, src_lon = src_point
        des_lat, des_lon = des_point

        distance = rtdm.distance_2_points(float(src_lat), float(src_lon), float(des_lat), float(des_lon))
        src_ata, src_lat, src_lon, des_ata, des_lat, des_lon = check_distance(options, src_ata, src_lat, src_lat,
                                                                              des_ata,
                                                                              des_lat, des_lon,
                                                                              distance,
                                                                              traffic_df_8_15)

        row_index = traffic_df_8_15[traffic_df_8_15['ATA'] == src_ata].index
        traffic_df_8_15.at[row_index[0], 'n_vehicles'] -= 1

        coord_route = generate_route(options, src_lat, src_lon, des_lat, des_lon)

        edges_route = rtdm.coordinates_to_edge(options, sqlite3.connect(options.dbPath), coord_route)

        route_id_list.append(f'{edges_route[0]}_to_{edges_route[len(edges_route) - 1]}')
        route_list.append(edges_route)
        print(f'Generating routes: {j}/{int(total_vehicles * 0.01)}')

        # print(f'Route id: {edges_route[0]}_to_{edges_route[len(edges_route) - 1]}')
        # print(f'Route: {edges_route}')
    data = {'route_id': route_id_list, 'route': route_list}
    pd.DataFrame.from_dict(data).to_csv("/home/josedaniel/traffic_test.csv", index=False)
    traffic_df_8_15.to_csv("/home/josedaniel/traffic_8_15_modified.csv", index=False)


def get_traffic_df_from_csv(options, time):
    traffic_df = pd.read_csv(options.traffic_file)
    df = traffic_df[traffic_df['time'] == time]
    return df


def select_point(options, node_type, df):
    if node_type == 'edge':
        ata_df = df[df['ATA'].isin(edge_valencia_ata)]
        ata, point = get_coord_for_ata(ata_df, options)
        while point is None:
            ata, point = get_coord_for_ata(ata_df, options)
        return ata, point

        # Subtract one vehicle for the ata selected
        # row_index = ata_df_copy[ata_df_copy['ATA'] == selected_ata[0]].index
        #
        # ata_df_copy.at[row_index[0], 'n_vehicles'] -= 1
    elif node_type == 'city':
        ata_df = df[~df['ATA'].isin(edge_valencia_ata)]
        ata, point = get_coord_for_ata(ata_df, options)
        while point is None:
            ata, point = get_coord_for_ata(ata_df, options)
        return ata, point


def get_coord_for_ata(df, options):
    # Make a copy to modify it
    ata_df_copy = df.copy()
    # Get the total numbers of vehicles to iterate i times
    total_vehicles = sum(df['n_vehicles'].to_list())
    # for i in range(0, total_vehicles):
    ata_list = ata_df_copy['ATA'].to_list()
    n_vehicles = ata_df_copy['n_vehicles'].to_list()
    # Select one ATA based on its weight probability
    selected_ata = random.choices(ata_list, cum_weights=n_vehicles, k=1)
    nodes = ata_df_copy[ata_df_copy['ATA'] == selected_ata[0]]['node'].to_list()
    nodes = nodes[0].split(" ")
    node = random.choice(nodes)
    return selected_ata[0], rtdm.get_coord_from_node(sqlite3.connect(options.dbPath), node)


def generate_route(options, src_lat, src_lon, dest_lat, dest_lon):
    return rtdm.get_route_from_ABATIS(options, src_lat, src_lon, dest_lat, dest_lon)
