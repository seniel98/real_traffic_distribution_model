import sys

sys.path.append("/home/josedaniel/real_traffic_distribution_model")

import real_traffic_distribution_model as rtdm


def create_od_routes(options):
    print("create_od_routes")
    # TODO Create the od routes matrix


def read_traffic_csv(options):
    print("read_traffic_csv")

    # TODO Read the csv


def select_source_point():
    print("select_source_point")
    # TODO Select the source point of the route


def select_destination_point():
    print("select_destination_point")
    # TODO Select the source point of the route


def generate_route(options, src_lat, src_lon, dest_lat, dest_lon):
    print("generate_route")
    rtdm.get_route_from_ABATIS(options, src_lat, src_lon, dest_lat, dest_lon)
