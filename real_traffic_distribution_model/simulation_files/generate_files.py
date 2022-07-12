import datetime
import socket
import sqlite3
import sys
from optparse import OptionParser
import os

# Important to execute it from terminal. This add the module to the PYTHONPATH
import numpy as np

sys.path.append("/home/josedaniel/real_traffic_distribution_model")

import real_traffic_distribution_model as rtdm

optParser = OptionParser
username = socket.gethostname()
update_total_traffic_csv = 'update_total_traffic.csv'
update_specific_traffic_csv = 'update_specific_traffic.csv'


def write_simulation_files(db, way_id_list=None, name="default", sim_type="Normal"):
    """
    This function writes the rou.xml and the add.xml file for the simulation

    Args:
      db: the database connection
      way_id_list: a list of way ids that you want to simulate. If you want to simulate the entire map, leave this as None.
      name: the name of the simulation. Defaults to default
      sim_type: The type of simulation you want to run. Defaults to Normal
    """
    cursor = db.cursor()

    if way_id_list is not None:
        route_dist_list = []
        all_routes = []
        for way_id in way_id_list:
            # Select all the routes with the id of the desire way
            args_route = "%" + way_id + "%"
            sql_routes_sentence = 'select routes.id from routes where routes.route like "%s"' % args_route
            cursor.execute(sql_routes_sentence)
            routes = cursor.fetchall()
            all_routes += routes

        for routeID in all_routes:
            route_dist_id = routeID[0].split("_", 1)
            if route_dist_id[0] not in route_dist_list:
                route_dist_list.append(route_dist_id[0])

        print(len(route_dist_list))
        print('\nWriting files...')

        write_rou_file(cursor, name, sim_type)
    else:
        write_rou_file(cursor, name, sim_type)
        write_additional_file(name, sim_type)


def write_additional_file(name, sim_type):
    """
    It writes an additional file for the simulation

    Args:
      name: The name of the simulation.
      sim_type: The type of simulation you want to run. This can be either "", "a", or "b".
    """
    with open("%s/valencia.%s%s.add.xml" % (os.getcwd(), name.replace(" ", ""), sim_type), 'w+') as additional_file:
        additional_file.write(
            "<!-- \n\tGenerated by write_rou_file by %s at %s $ \n\n-->\n" % (username, datetime.datetime.now()))
        additional_file.write(
            '<additional>\n')
        additional_file.write(
            f'\t<edgeData id="edges_emissions_data" type="emissions" file="edges_emissions_data_{sim_type}.xml" excludeEmpty="true"/>\n')
        additional_file.write("</additional>\n")


def write_rou_file(cursor, name, sim_type):
    """
    This function writes the routes file for the simulation

    Args:
      cursor: the cursor object that we created earlier
      name: The name of the simulation.
      sim_type: The type of simulation you want to run.
    """

    char_to_replace = {'[': '',
                       ',': '',
                       ']': '',
                       '\'': ''}
    with open("%s/valencia.%s%s.rou.xml" % (os.getcwd(), name.replace(" ", ""), sim_type), 'w+') as route_file:
        route_file.write(
            "<!-- \n\tGenerated by write_rou_file by %s at %s $ \n\n-->\n" % (username, datetime.datetime.now()))
        route_file.write(
            '<routes xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://sumo.dlr.de/xsd/routes_file.xsd">\n')
        route_file.write('\t<vType id="gasoline_b" emissionClass="HBEFA3/PC_G_EU3"/>\n')
        route_file.write('\t<vType id="gasoline_c" emissionClass="HBEFA3/PC_G_EU5"/>\n')
        route_file.write('\t<vType id="gas_oil_b" emissionClass="HBEFA3/PC_D_EU4"/>\n')
        route_file.write('\t<vType id="gas_oil_c" emissionClass="HBEFA3/PC_D_EU6"/>\n')

        k = 0

        sql_sentence = 'select DISTINCT routes.route_id,routes.route from vehicles,routes where vehicles.route=routes.route_id'
        cursor.execute(sql_sentence)
        result_row_current = cursor.fetchall()
        routes_list = np.array(result_row_current)
        for element in routes_list:
            edges = element[1]
            for key, value in char_to_replace.items():
                # Replace key character with value character in string
                edges = edges.replace(key, value)
            print(f'\t<route id="{element[0]}" edges="{edges}"/>\n', file=route_file)

            rtdm.update_progress(k + 1, int(len(routes_list)),
                                 'Writing Routes...')
            k = k + 1
        write_vehicles(cursor, route_file)
        route_file.write("</routes>\n")


def write_vehicles(cursor, route_file):
    """
    It writes the vehicles in the route file

    Args:
      cursor: the cursor object that we created earlier
      route_file: the file to write the routes to
    """
    sql_sentence = f'select DISTINCT vehicles.vehicle_id,vehicles.depart,vehicles.departLane,vehicles.departPos,vehicles.departSpeed,vehicles.route from vehicles order by vehicles.depart'
    cursor.execute(sql_sentence)
    result_row_current = cursor.fetchall()
    vehicles_list = np.array(result_row_current)
    cars = 0
    for element in vehicles_list:
        cars += 1
        v_type = ""
        gasoline_cars_b = round(len(result_row_current) * 0.23)
        gasoline_cars_c = round(len(result_row_current) * 0.22)
        diesel_cars_b = round(len(result_row_current) * 0.28)
        diesel_cars_c = round(len(result_row_current) * 0.27)

        if cars <= gasoline_cars_b:
            v_type = "gasoline_b"
        elif cars <= gasoline_cars_b + gasoline_cars_c:
            v_type = "gasoline_c"
        elif cars <= gasoline_cars_b + gasoline_cars_c + diesel_cars_b:
            v_type = "gas_oil_b"
        else:
            v_type = "gas_oil_c"

        rtdm.update_progress(cars, int(len(vehicles_list)),
                             'Writing Vehicles...')
        print(
            f'\t<vehicle id="{element[0]}" type="{v_type}" depart="{element[1]}" departLane="{element[2]}" departPos="{element[3]}" departSpeed="{element[4]}" route="{element[5]}"/>\n',
            file=route_file)


def write_route_coord_file(options, opt_parser, name, sim_type):
    """The function creates a file with the coordinates of the routes

    Args:
        options (options):  Options retrieved from command line
        opt_parser (OptionParser): Parser form the LinkABATISMain
        name (str): Name of the file to sim i.e.: zonaCentro
        sim_type (str): Name of the type of the file to sim i.e.: Eco, Normal, etc.
    """
    print(options.dbPath)
    db = sqlite3.connect(options.dbPath)
    array_routes = rtdm.get_routes(db)

    with open(f'valencia.25min.{name}{sim_type}.rouedgenode.valenciaATA.xml', 'w') as route_edge_node_file:
        route_edge_node_file.write(
            f'<!-- \n\tGenerated by route_coord on {opt_parser.get_version()} by {username} at {datetime.datetime.now()} $ \n\t{opt_parser.get_description()}\n-->\n')
        route_edge_node_file.write("<routes>\n")
        count_routes = 0
        for i in range(0, len(array_routes)):
            array_coord = []
            count_routes = count_routes + 1
            edge_split = str(array_routes[i][1]).split(' ')
            for j in range(0, len(edge_split)):
                array_coord.append(
                    str(rtdm.edge_2_coord(options, db, edge_split[j])[0][0][0]))
                array_coord.append(
                    str(rtdm.edge_2_coord(options, db, edge_split[j])[0][0][1]))
                if j == len(edge_split) - 1:
                    array_coord.append(
                        str(rtdm.edge_2_coord(options, db, edge_split[j])[1][0][0]))
                    array_coord.append(
                        str(rtdm.edge_2_coord(options, db, edge_split[j])[1][0][1]))
            route_edge_node_file.write(
                "\t<route id=\"%s\" coordinates=\"%s\"/>\n" % ((array_routes[i][0]), (" ".join(array_coord))))
            rtdm.update_progress(i + 1, len(array_routes),
                                 'Creating Edge with coordinates')
        route_edge_node_file.write("</routes>")

    db.close()


def update_total_traffic(db):
    """The function updates the speed value of all the edges

    Args:
        db (Database): The database
    """
    print('Writing Traffic Update...')
    with open(update_total_traffic_csv, 'w') as updateTrafficFile:
        cursor = db.cursor()
        sql_sentence = 'select edges."from",edges."to",edges.speedUpdated from edges'
        cursor.execute(sql_sentence)
        result_row_current = cursor.fetchall()
        for i in range(0, len(result_row_current)):
            line_to_write = '%s,%s,%s' % (
                result_row_current[i][0], result_row_current[i][1],
                (int(round(rtdm.mps_to_kmph(float(result_row_current[i][2]))))))
            updateTrafficFile.write(line_to_write)
            updateTrafficFile.write('\n')


def update_specific_traffic(db, nodes_vector_total):
    """The function updates the value of the edges speed for a given list of nodes

    Args:
        db (Database): The database
        nodes_vector_total (list): List of nodes to update the speed.
    """
    print('Writing Traffic Update...')
    with open(update_specific_traffic_csv, 'w') as updateTrafficFile:
        cursor = db.cursor()
        for i in range(0, len(nodes_vector_total)):
            for j in range(0, len(nodes_vector_total[i])):
                print(nodes_vector_total[i][j][0], nodes_vector_total[i][j][1])
                sql_sentence = 'select edges."from",edges."to",edges.speedUpdated from edges where edges."from"="%s" ' \
                               'and edges."to"="%s"' % (
                                   nodes_vector_total[i][j][0], nodes_vector_total[i][j][1])
                cursor.execute(sql_sentence)
                result_row_current = cursor.fetchall()
                line_to_write = '%s,%s,%s' % (result_row_current[0][0], result_row_current[0][1],
                                              (int(round(rtdm.mps_to_kmph(float(result_row_current[0][2]))))))
                updateTrafficFile.write(line_to_write)
                updateTrafficFile.write('\n')
