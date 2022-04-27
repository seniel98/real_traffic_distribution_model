import sys
import pandas as pd
import sqlite3
import random
from xml.etree import ElementTree

sys.path.append("/home/josedaniel/real_traffic_distribution_model")

import real_traffic_distribution_model as rtdm

edge_valencia_ata = ['A72', 'A59', 'A58', 'A413', 'A409', 'A392', 'A373', 'A30', 'A298', 'A297', 'A296', 'A287',
                     'A257', 'A1']


def clean_point(options, point, point_type, node_type, df):
    if point_type == "src":
        while point is None:
            point = select_source_point(options, node_type, df)
            if point is not None:
                break
    elif point_type == "des":
        while point is None:
            point = select_destination_point(options, node_type, df)
            if point is not None:
                break

    return point


def create_od_routes(options):
    traffic_df_8_15 = get_traffic_df_from_csv(options, '08:15:00')
    # traffic_df_8_30 = get_traffic_df_from_csv(options, '08:30:00')
    # traffic_df_8_45 = get_traffic_df_from_csv(options, '08:45:00')
    # traffic_df_9_00 = get_traffic_df_from_csv(options, '09:00:00')

    src_point = select_source_point(options, 'edge', traffic_df_8_15)
    des_point = select_destination_point(options, 'city', traffic_df_8_15)

    src_lat, src_lon = clean_point(options, src_point, "src", "edge", traffic_df_8_15)
    des_lat, des_lon = clean_point(options, des_point, "des", "city", traffic_df_8_15)

    distance = rtdm.distance_2_points(float(src_lat), float(src_lon), float(des_lat), float(des_lon))
    print(distance)
    if distance < 2000.0:
        while True:
            src_point = select_source_point(options, 'edge', traffic_df_8_15)
            des_point = select_destination_point(options, 'city', traffic_df_8_15)

            src_lat, src_lon = clean_point(options, src_point, "src", "edge", traffic_df_8_15)
            des_lat, des_lon = clean_point(options, des_point, "des", "city", traffic_df_8_15)

            distance = rtdm.distance_2_points(float(src_lat), float(src_lon), float(des_lat), float(des_lon))
            print(distance)
            if distance > 2000.0:
                break

    coord_route = generate_route(options, src_lat, src_lon, des_lat, des_lon)
    edges_route = rtdm.coordinates_to_edge(options, sqlite3.connect(options.dbPath), coord_route)

    print(edges_route)


def get_traffic_df_from_csv(options, time):
    traffic_df = pd.read_csv(options.traffic_file)
    df = traffic_df[traffic_df['time'] == time]

    return df

    # # Set index on a Dataframe
    # traffic_df.set_index("ATA", inplace=True)
    #
    # # Using the operator .loc[]
    # # to select multiple rows
    # edges_traffic_df = traffic_df.loc[entry_valencia_ata]
    # print(edges_traffic_df_8_15)

    # How to get the coord for a given node_id  --> rtdm.get_coord_from_node(sqlite3.connect(options.dbPath), node_id)


def select_source_point(options, node_type, df):
    if node_type == 'edge':
        # Get a dataframe for the ata of the edge of the city
        ata_df = df[df['ATA'].isin(edge_valencia_ata)]
        # Make a copy to modify it
        ata_df_copy = ata_df.copy()
        # Get the total numbers of vehicles to iterate i times
        total_vehicles = sum(ata_df['n_vehicles'].to_list())
        # for i in range(0, total_vehicles):
        ata_list = ata_df_copy['ATA'].to_list()
        n_vehicles = ata_df_copy['n_vehicles'].to_list()

        # Select one ATA based on its weight probability
        selected_ata = random.choices(ata_list, cum_weights=n_vehicles, k=1)

        nodes = ata_df_copy[ata_df_copy['ATA'] == selected_ata[0]]['node'].to_list()
        nodes = nodes[0].split(" ")

        node = random.choice(nodes)

        print(node)

        return rtdm.get_coord_from_node(sqlite3.connect(options.dbPath), node)

        # Subtract one vehicle for the ata selected
        # row_index = ata_df_copy[ata_df_copy['ATA'] == selected_ata[0]].index
        #
        # ata_df_copy.at[row_index[0], 'n_vehicles'] -= 1


    elif node_type == 'city':
        print('city')


def select_destination_point(options, node_type, df):
    if node_type == 'edge':
        print('edge')
    elif node_type == 'city':
        # Get a dataframe for the ata of the edge of the city
        ata_df = df[~df['ATA'].isin(edge_valencia_ata)]
        # Make a copy to modify it
        ata_df_copy = ata_df.copy()
        # Get the total numbers of vehicles to iterate i times
        total_vehicles = sum(ata_df['n_vehicles'].to_list())
        # for i in range(0, total_vehicles):
        ata_list = ata_df_copy['ATA'].to_list()
        n_vehicles = ata_df_copy['n_vehicles'].to_list()

        # Select one ATA based on its weight probability
        selected_ata = random.choices(ata_list, cum_weights=n_vehicles, k=1)

        nodes = ata_df_copy[ata_df_copy['ATA'] == selected_ata[0]]['node'].to_list()
        nodes = nodes[0].split(" ")

        node = random.choice(nodes)

        lat, lon = rtdm.get_coord_from_node(sqlite3.connect(options.dbPath), node)

        return lat, lon


def generate_route(options, src_lat, src_lon, dest_lat, dest_lon):
    print("generate_route")
    return rtdm.get_route_from_ABATIS(options, src_lat, src_lon, dest_lat, dest_lon)
