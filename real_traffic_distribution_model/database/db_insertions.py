import os
import random
import sqlite3
import sys
from xml.etree import ElementTree
import sumolib as sumo

# Important to execute it from terminal. This add the module to the PYTHONPATH
sys.path.append("/home/josedaniel/real_traffic_distribution_model")

import real_traffic_distribution_model as rtdm


def insert_nodes(options, db):
    """The function to insert osm nodes into the database

    Args:
        options (options): Options retrieved from command line
        db (Database): The database
    """
    net = sumo.net.readNet(str(options.netfile))

    nodes_list = []
    j = 0
    for node in net.getNodes():
        id_node = node.getID()
        x, y = node.getCoords()
        lon, lat = net.convertXY2LonLat(x, y)
        lon = round(lon, 5)
        lat = round(lat, 5)
        node_inner_list = (id_node, lat, lon)
        nodes_list.append(node_inner_list)
        rtdm.update_progress(j + 1, len(net.getNodes()), 'Inserting Nodes into DB')

    db.executemany("INSERT INTO nodes(id,lat,lon) VALUES(?,?,?);", nodes_list)
    db.commit()
    print('\n' + 'Nodes inserted into database!')


def insert_edge_type(options, db):
    """The function to insert osm egdes type into the databse

    Args:
        options (options): Options retrieved from command line
        db (Database): The database
    """
    edges_list = []
    edge_types = [['raceway', 90], ['motorway', 90], ['motorway_link', 45], ['trunk', 85], ['trunk_link', 40],
                  ['primary', 65], ['primary_link', 30], ['secondary', 55], [
                      'secondary_link', 25], ['tertiary', 40],
                  ['tertiary_link', 20], ['unclassified', 25], [
                      'residential', 25], ['living_street', 10],
                  ['service', 15]]
    for i in range(0, len(edge_types)):
        edges_inner_list = (edge_types[i][0], edge_types[i][1])
        edges_list.append(edges_inner_list)
    db.executemany("INSERT INTO edgeType(id,speedKMH) VALUES(?,?)", edges_list)
    db.commit()
    print('\n' + 'Edges type inserted into database!')


def insert_edges(options, db):
    """The function to insert the edges of net files combined with osm specifications

    Args:
        options (options): Options retrieved from command line
        db (Database): The database
    """
    net = sumo.net.readNet(str(options.netfile))

    edge_list = []
    j = 0

    for edge in net.getEdges():

        edge_id = edge.getID()
        edge_from = edge.getFromNode()
        edge_to = edge.getToNode()
        edge_speed = edge.getSpeed()
        edge_length = edge.getLength()
        edge_type = edge.getType()
        edge_type_clean = edge_type[edge_type.find('.') + 1:]

        edge_inner_list = (edge_id, edge_from, edge_to, edge_speed, edge_speed, edge_length, edge_type_clean)
        edge_list.append(edge_inner_list)

        rtdm.update_progress(j + 1, len(net.getEdges()), 'Inserting Edge into DB')

    db.executemany(
        "INSERT INTO edges(id,[from],[to],speedOriginal, speedUpdated, length,edgeType) VALUES(?,?,?,?,?,?,?);",
        edge_list)
    db.commit()
    print('\n' + 'Edges inserted into database!')


def insert_routes(options, db):
    """The function inserts the routes from the .rou file into the database

    Args:
        options (options): Options retrieved from command line
        db (Database): The database
    """
    document_route_file = ElementTree.parse(str(options.routefile))
    my_list = []
    routes_list = []
    routes_inner_list = []
    i = 0
    for routes in document_route_file.findall('route'):
        i = i + 1
        routes_inner_list = (routes.attrib['id'], routes.attrib['edges'])
        routes_list.append(routes_inner_list)
        rtdm.update_progress(i, len(document_route_file.findall(
            'route')), 'Inserting routes into DB')
    db.executemany("INSERT INTO routes(id,route) VALUES(?,?);", routes_list)
    db.commit()
    print('\n' + 'Routes inserted into database!')


def insert_vehicles(options, db):
    """The function inserts the vehicles from the .add file into the database

    Args:
        options (options): Options retrieved from command line
        db (Database): The database
    """
    document_additional_file = ElementTree.parse(str(options.additionalfile))
    vehicles_list = []
    i = 0
    for vehicle in document_additional_file.findall('vehicle'):
        i = i + 1
        vehicle_inner_list = (
            vehicle.attrib['id'], vehicle.attrib['depart'], vehicle.attrib['departLane'], vehicle.attrib['departPos'],
            vehicle.attrib['departSpeed'], vehicle.attrib['route'])
        vehicles_list.append(vehicle_inner_list)
        rtdm.update_progress(i, len(document_additional_file.findall(
            'vehicle')), 'Inserting vehicles into DB')
    db.executemany("INSERT INTO vehicles(id,depart,departLane,departPos,departSpeed,route) VALUES(?,?,?,?,?,?);",
                   vehicles_list)
    db.commit()
    print('\n' + 'Vehicles inserted into database!')


def insert_congestion(options):
    """The function adds extra vehicles into the database

    Args:
        options (options): Options retrieved from command line
    """
    if os.path.isfile(options.dbPath):
        if os.path.exists('%s/DataBase_Congestions' % os.getcwd()):
            os.system('cp %s %s/DataBase_Congestions/%s_%s.db' % (
                options.dbPath, os.getcwd(), options.dbPath, options.numextravehicles))
        else:
            os.system('mkdir DataBase_Congestions')
            os.system('cp %s %s/DataBase_Congestions/%s_%s.db' % (
                options.dbPath, os.getcwd(), options.dbPath, options.numextravehicles))
    else:
        print(
            'Warning: I don\'t find the DataBase. Please, Create the DataBase for continue')
    new_path_data_base = ('%s/DataBase_Congestions/%s_%s.db' %
                          (os.getcwd(), options.dbPath, options.numextravehicles))
    db = sqlite3.connect(new_path_data_base)
    cursor = db.cursor()
    array_routes = rtdm.get_routes(db)

    sql_sentence = 'SELECT name FROM sqlite_master WHERE type="table" AND name="vehiclesAdditional";'
    cursor.execute(sql_sentence)
    result_row_current = cursor.fetchall()
    if len(result_row_current) == 0:
        cursor.execute(
            'CREATE TABLE vehiclesAdditional(id STRING PRIMARY KEY UNIQUE NOT NULL, depart DOUBLE (6, 2) NOT NULL, departLane  STRING NOT NULL, departPos   STRING NOT NULL, departSpeed STRING NOT NULL, route STRING NOT NULL REFERENCES routes (id));')
        db.commit()

    for num in range(0, int(options.numrepetition) + 1):
        print("Repetition=%s" % num)
        vehicle_learning_list = []
        # Insert vehicles additional into new tables
        for i in range(0, len(array_routes)):
            time_random = random.sample(range(0, 25000), 25000)
            for j in range(0, int(options.numextravehicles)):
                # Insercion de los n vehiculos
                name_vehicle = "emitter_%s_*_%s_additional" % (
                    (array_routes[i][0], str(time_random[j])))
                vehicle_learning_inner_list = (
                    name_vehicle, str(
                        (round((float(time_random[j]) / float(1000)), 2))), "best", "random_free", "max",
                    (array_routes[i][0]))
                vehicle_learning_list.append(vehicle_learning_inner_list)
                rtdm.update_progress(i + 1, len(array_routes),
                                     'Inserting congestion...')
        db.executemany(
            'INSERT INTO vehiclesAdditional(id,depart,departLane,departPos,departSpeed,route) VALUES(?,?,?,?,?,?);',
            vehicle_learning_list)
        db.commit()
    db.close()


def insert_new_route(options, db, route_name, route_edges):
    """The function insert a new route into the routes table

    Args:
        options (options): Options retrieved from command line
        db (Database): The database
        route_name (str): The id of the route
        route_edges (list): The list of edges that compound the route
    """
    cursor = db.cursor()
    sql_sentence = f'insert into routes (id,route) values ("{route_name}","{route_edges}");'
    cursor.execute(sql_sentence)
    db.commit()
