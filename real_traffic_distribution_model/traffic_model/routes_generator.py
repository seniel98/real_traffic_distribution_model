import sys
import pandas as pd
import sqlite3
import random
import numpy as np
from xml.etree import ElementTree

sys.path.append("/home/josedaniel/real_traffic_distribution_model")

import real_traffic_distribution_model as rtdm

EDGE_ATA = ['A72', 'A59', 'A58', 'A413', 'A409', 'A392', 'A373', 'A30', 'A298', 'A297', 'A296', 'A287',
            'A257', 'A1']
update_specific_traffic_csv = 'update_specific_traffic.csv'


def check_distance(options, src_ata, src_lat, src_lon, des_ata, des_lat, des_lon, distance, df):
    while distance < 1500.0:
        src_ata, src_point = select_point(options, 'edge', df)
        des_ata, des_point = select_point(options, 'city', df)

        src_lat, src_lon = src_point
        des_lat, des_lon = des_point

        distance = rtdm.distance_2_points(float(src_lat), float(src_lon), float(des_lat), float(des_lon))
        # print(distance)
        # print("Check distance")
        # print(src_lat, src_lon)

    return src_ata, src_lat, src_lon, des_ata, des_lat, des_lon


def is_n_vehicles_ok(ata, df, df_copy):
    pass


def create_od_routes(options):
    traffic_df_8_15 = get_traffic_df_from_csv(options, '08:15:00')
    traffic_df_8_15_copy = traffic_df_8_15.copy()
    # traffic_df_8_30 = get_traffic_df_from_csv(options, '08:30:00')
    # traffic_df_8_45 = get_traffic_df_from_csv(options, '08:45:00')
    # traffic_df_9_00 = get_traffic_df_from_csv(options, '09:00:00')
    # traffic_df_8_15.to_csv("/home/josedaniel/traffic_8_15_default.csv", index=False)
    total_vehicles = sum(traffic_df_8_15['n_vehicles'].to_list())

    # 50% vehicles from edge to city
    # 30% from city to edge
    # 25% from city to city
    # 5% from edge to edge

    route_id_list = []
    route_list = []
    j = 0

    while int(total_vehicles) > 534000:
        j = j + 1
        src_ata, src_point = select_point(options, 'edge', traffic_df_8_15)
        des_ata, des_point = select_point(options, 'city', traffic_df_8_15)

        src_lat, src_lon = src_point
        des_lat, des_lon = des_point

        distance = rtdm.distance_2_points(float(src_lat), float(src_lon), float(des_lat), float(des_lon))

        src_ata, src_lat, src_lon, des_ata, des_lat, des_lon = check_distance(options, src_ata, src_lat, src_lon,
                                                                              des_ata,
                                                                              des_lat, des_lon,
                                                                              distance,
                                                                              traffic_df_8_15)

        # print(src_lat, src_lon)
        coord_route = generate_route(options, src_lat, src_lon, des_lat, des_lon)

        nodes_route, edges_route = rtdm.coordinates_to_edge(options, sqlite3.connect(options.dbPath),
                                                            coord_route)

        src_row_index = traffic_df_8_15.loc[traffic_df_8_15['ATA'] == src_ata].index
        des_row_index = traffic_df_8_15.loc[traffic_df_8_15['ATA'] == des_ata].index
        traffic_df_8_15.at[src_row_index[0], 'n_vehicles'] = np.int64(
            (traffic_df_8_15.at[src_row_index[0], 'n_vehicles'].item() - 1))
        traffic_df_8_15.at[des_row_index[0], 'n_vehicles'] = np.int64(
            (traffic_df_8_15.at[des_row_index[0], 'n_vehicles'].item() - 1))
        route_ata_list = [src_ata]
        for i in range(1, len(nodes_route) - 2):
            node_int = nodes_route[i][0]
            ata = get_ATA_from_node(str(node_int), traffic_df_8_15)
            if is_n_vehicles_ok(ata, traffic_df_8_15, traffic_df_8_15_copy):
                if ata is not None and ata not in route_ata_list and ata != des_ata:
                    route_ata_list.append(ata)
                    # print(route_ata_list)
                    row_index = traffic_df_8_15.loc[traffic_df_8_15['ATA'] == ata].index
                    # Convert numpy.array to Python list
                    # if row_index._data.tolist():
                    traffic_df_8_15.at[row_index[0], 'n_vehicles'] = np.int64(
                        (traffic_df_8_15.at[row_index[0], 'n_vehicles'].item() - 1))
                    # else:
                    #     continue
            else:
                pass
            #     with open(update_specific_traffic_csv, 'w') as updateTrafficFile:
            #
            #         line_to_write = '%s,%s,%s' % (
            #             result_row_current[i][0], result_row_current[i][1],
            #             (int(round(link_ABATIS.mps_to_kmph(float(result_row_current[i][2]))))))
            #         updateTrafficFile.write(line_to_write)
            #         updateTrafficFile.write('\n')
            #     rtdm.ABATIS_update_traffic(False, options, )
        route_ata_list.append(des_ata)
        # print(route_ata_list)
        route_id_list.append(f'{edges_route[0]}_to_{edges_route[len(edges_route) - 1]}')
        route_list.append(edges_route)

        total_vehicles = sum(traffic_df_8_15['n_vehicles'].to_list())
        print(total_vehicles)
        print(f'Generating routes: {j}/55756')
    # print(f'Route id: {edges_route[0]}_to_{edges_route[len(edges_route) - 1]}')
    # print(f'Route: {edges_route}')
    data = {'route_id': route_id_list, 'route': route_list}
    pd.DataFrame.from_dict(data).to_csv("/home/josedaniel/traffic_test.csv", index=False)
    traffic_df_8_15.to_csv("/home/josedaniel/traffic_8_15_modified.csv", index=False)


def get_ATA_from_node(node_id, df):
    df_nodes = df[df.node.str.contains(str(node_id), case=False)]
    if not df_nodes.empty:
        ata_list = df_nodes['ATA'].to_list()
        ata = ata_list[0]
        return ata
    else:
        return None


def get_traffic_df_from_csv(options, time):
    # traffic_df = pd.read_csv(options.traffic_file)
    traffic_df = pd.read_csv(options.traffic_file)
    df = traffic_df[traffic_df['time'] == time]
    return df


def select_point(options, node_type, df):
    if node_type == 'edge':
        ata_df = df[df['ATA'].isin(EDGE_ATA)]
        ata, point = get_coord_for_ata(ata_df, options)
        while point is None:
            ata, point = get_coord_for_ata(ata_df, options)
        return ata, point

        # Subtract one vehicle for the ata selected
        # row_index = ata_df_copy[ata_df_copy['ATA'] == selected_ata[0]].index
        #
        # ata_df_copy.at[row_index[0], 'n_vehicles'] -= 1
    elif node_type == 'city':
        ata_df = df[~df['ATA'].isin(EDGE_ATA)]
        ata, point = get_coord_for_ata(ata_df, options)
        while point is None:
            ata, point = get_coord_for_ata(ata_df, options)
        return ata, point


def get_coord_for_ata(df, options):
    ata_list = df['ATA'].to_list()
    n_vehicles = df['n_vehicles'].to_list()
    # Select one ATA based on its weight probability
    selected_ata = random.choices(ata_list, cum_weights=n_vehicles, k=1)
    nodes = df[df['ATA'] == selected_ata[0]]['node'].to_list()
    nodes = nodes[0].split(" ")
    node = random.choice(nodes)
    return selected_ata[0], rtdm.get_coord_from_node(sqlite3.connect(options.dbPath), node)


def generate_route(options, src_lat, src_lon, dest_lat, dest_lon):
    return rtdm.get_route_from_ABATIS(options, src_lat, src_lon, dest_lat, dest_lon)
