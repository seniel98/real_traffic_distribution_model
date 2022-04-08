import sys
import pandas as pd
import sqlite3
from xml.etree import ElementTree

sys.path.append("/home/josedaniel/real_traffic_distribution_model")

import real_traffic_distribution_model as rtdm

entry_valencia_ata = ['A72', 'A59', 'A58', 'A413', 'A409', 'A392', 'A373', 'A30', 'A298', 'A297', 'A296', 'A287',
                      'A257', 'A1']


def create_od_routes(options):
    print("create_od_routes")
    traffic_df_8_15 = get_traffic_df_from_csv(options, '08:15:00')
    traffic_df_8_30 = get_traffic_df_from_csv(options, '08:30:00')
    traffic_df_8_45 = get_traffic_df_from_csv(options, '08:45:00')
    traffic_df_9_00 = get_traffic_df_from_csv(options, '09:00:00')

    # print(rtdm.distance_2_points(39.468762, -0.372507, 39.473339, -0.350881))


def get_traffic_df_from_csv(options, time):
    traffic_df = pd.read_csv(options.traffic_file)

    if time == '08:15:00':
        df = traffic_df[traffic_df['time'] == time]
    elif time == '08:30:00':
        df = traffic_df[traffic_df['time'] == time]
    elif time == '08:45:00':
        df = traffic_df[traffic_df['time'] == time]
    elif time == '09:00:00':
        df = traffic_df[traffic_df['time'] == time]
    else:
        return

    return df

    # # Set index on a Dataframe
    # traffic_df.set_index("ATA", inplace=True)
    #
    # # Using the operator .loc[]
    # # to select multiple rows
    # edges_traffic_df = traffic_df.loc[entry_valencia_ata]
    # print(edges_traffic_df_8_15)

    # How to get the coord for a given node_id  --> rtdm.get_coord_from_node(sqlite3.connect(options.dbPath), node_id)


def select_source_point(node_type):
    print("select_source_point")
    if node_type == 'edge':
        print('edge')
    elif node_type == 'city':
        print('city')


def select_destination_point(node_type):
    print("select_destination_point")
    if node_type == 'edge':
        print('edge')
    elif node_type == 'city':
        print('city')


def generate_route(options, src_lat, src_lon, dest_lat, dest_lon):
    print("generate_route")
    rtdm.get_route_from_ABATIS(options, src_lat, src_lon, dest_lat, dest_lon)
