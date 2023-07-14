import json
import math
import os
import sqlite3
import subprocess
import sys
import time
from urllib.error import HTTPError
from urllib.request import urlopen
from xml.etree import ElementTree

# Important to execute it from terminal. This add the module to the PYTHONPATH
sys.path.append("/home/josedaniel/real_traffic_distribution_model")

import real_traffic_distribution_model as rtdm


def get_time_traveled_by_equation(options, segment_id, x, db):
    """The function gets the traveled time through an equation

    Args:
        options (options): Options retrieved from command line
        segment_id (str): The id of a certain segment
        x (int): ???
        db (Database): The database

    Returns:
        [float,float]: The speed and function of the vehicles
    """
    cursor = db.cursor()
    sql_sentence = 'select * from segmentsUnified where segmentsUnified.id="%s"' % (
        segment_id)
    cursor.execute(sql_sentence)
    result_row_current = cursor.fetchall()
    length = float(result_row_current[0][1])
    a = float(result_row_current[0][4])
    b = float(result_row_current[0][5])
    c = float(result_row_current[0][6])
    ff = float(result_row_current[0][8] * 1000)
    segment_type = result_row_current[0][10]
    # print('\na=%s\nb=%s\nc=%s\nff=%s\nlength:%s\ntype:%s'%(a,b,c,ff,length,type))
    if a == 0 or b == 0 or c == 0:
        if segment_type == 'dot' or segment_type == 'equal':
            sql_sentence2 = 'select edges.speedOriginal from edges,segmentsUnified where edges.segmentUnified=segmentsUnified.id and segmentsUnified.id="%s"' % segment_id
            cursor.execute(sql_sentence2)
            result_row_current2 = cursor.fetchall()
            fx = length / float(result_row_current2[0][0])
        else:
            fx = (a * (math.pow(x, 2)) + (b))
    else:
        fx = ((a / (1 + math.exp(b - (x / c)))) -
              (a / (1 + math.exp(b))) + ff) / 1000
    speed = length / fx
    # print('fx=%s\nspeed:%s'%(fx,speed))
    return [speed, fx]


def get_segments_id(options, db):
    """The function gets the total segments id in an array

    Args:
        options (options): Options retrieved from command line
        db (Database): The database

    Returns:
        [type]: [description]
    """
    cursor = db.cursor()
    sql_sentence = 'select segmentsUnified.id from segmentsUnified'
    cursor.execute(sql_sentence)
    result_row_current = cursor.fetchall()
    segments_id_array = []
    for i in range(0, len(result_row_current)):
        segments_id_array.append(str(result_row_current[i][0]))
    return segments_id_array


def get_edges_id_initials(options, db):
    """The function selects all the edges where vehicles depart at first time = 1000.
    They are selected from the vehiclesLearning table.

    Args:
        options (options): Options retrieved from command line
        db (Database): The database

    Returns:
        list: A list with all the edges id that fulfill the condition
    """
    cursor = db.cursor()
    sql_sentence = 'select vehiclesLearning.edge from vehiclesLearning where vehiclesLearning.firstTime=1000'
    cursor.execute(sql_sentence)
    result_row_current = cursor.fetchall()
    edges_id_array = []
    for i in range(0, len(result_row_current)):
        edges_id_array.append(str(result_row_current[i][0]))
        rtdm.update_progress(
            i + 2, len(result_row_current), 'EdgesIdInitials...')
    return edges_id_array


def get_vehicles_real_depart_info(options, edges_id_initials_array, db):
    """The function gets all specific parameters from vehicleLearning table in order to have a better approach of
    the results of the vehicles at depart time

    Args:
        options (options): Options retrieved from command line
        edges_id_initials_array (list): The list of edges
        db (Database): The database

    Returns:
        list: A list with all the information of departing vehicles
    """
    cursor = db.cursor()
    edges_id_array = []
    for i in range(0, len(edges_id_initials_array)):
        edges_id = edges_id_initials_array[i]
        sql_sentence = 'select vehiclesLearning.vehicle,vehiclesLearning.edge,vehiclesLearning.firstTime, vehicles.route from vehiclesLearning,vehicles where vehiclesLearning.vehicle=vehicles.id and vehiclesLearning.edge="%s" order by vehiclesLearning.firstTime' % edges_id
        cursor.execute(sql_sentence)
        result_row_current = cursor.fetchall()
        edges_id_array.append(result_row_current)
        rtdm.update_progress(i + 2, len(edges_id_initials_array),
                             'VehiclesRealDepartInfo...')
        # for j in range(0, len(result_rowCurrent)):edgesIdArray.append(str(result_rowCurrent[j]))
    return edges_id_array


def get_edges_id_by_time_depart(options, db, depart_time):
    """The function gets all edges ids have been passed by a vehicle in an instant of the time depart

    Args:
        options (options): Options retrieved from command line
        db (Database): The database
        depart_time (str): A certain instant of time

    Returns:
        list: A list with all edges ids have been passed by a vehicle in an instant of the time depart
    """
    cursor = db.cursor()
    sql_sentence = 'select vehiclesLearning.edge, vehiclesLearning.vehicleBeforeVehicle from vehiclesLearning where firstTime=%s' % depart_time
    cursor.execute(sql_sentence)
    result_row_current = cursor.fetchall()
    return result_row_current


def get_edges_id_by_vehicle_id(options, db, vehicle_id):
    """The function gets all edges ids have been passed by a vehicle

    Args:
        options (options): Options retrieved from command line
        db (Database): The database
        vehicle_id (str): The identifier of the vehicle

    Returns:
        list: A list with all edges ids have been passed by a vehicle
    """
    cursor = db.cursor()
    sql_sentence = 'select vehiclesLearning.edge,vehiclesLearning.firstTime,vehiclesLearning.vehicleBeforeVehicle from vehiclesLearning,edges where vehiclesLearning.edge=edges.id and vehiclesLearning.vehicle="%s" order by firstTime' % vehicle_id
    cursor.execute(sql_sentence)
    result_row_current = cursor.fetchall()
    return result_row_current


def get_edges_id_by_vehicle_id_congestion(options, db, vehicle_id, num_repetition):
    """The function gets all edges ids have been passed by a vehicle for the congestion experiment

    Args:
        options (options): Options retrieved from command line
        db (Database): The database
        vehicle_id (str): The identifier of the vehicle
        num_repetition (int): The number of repetition the function of congestion has been executed

    Returns:
        list: A list with all edges ids have been passed by a vehicle for the congestion experiment
    """
    cursor = db.cursor()
    sql_sentence = 'select vehiclesLearning_%s_%s.edge,vehiclesLearning_%s_%s.firstTime,vehiclesLearning_%s_%s.vehicleBeforeVehicle from vehiclesLearning_%s_%s,edges where vehiclesLearning_%s_%s.edge=edges.id and vehiclesLearning_%s_%s.vehicle="%s" order by firstTime' % (
        str(options.numextravehicles), num_repetition, str(
            options.numextravehicles), num_repetition,
        str(options.numextravehicles), num_repetition, str(
            options.numextravehicles), num_repetition,
        str(options.numextravehicles), num_repetition, str(options.numextravehicles), num_repetition, vehicle_id)
    cursor.execute(sql_sentence)
    result_row_current = cursor.fetchall()
    return result_row_current


def get_edges_id_initials_congestion(options, db, num_repetition):
    """The function selects all the edges where vehicles depart at first time = 1000.
    They are selected from the vehiclesLearning table. This is for congestion experiment

    Args:
        options (options): Options retrieved from command line
        db (Database): The database
        num_repetition (int): The number of repetition the function of congestion has been executed

    Returns:
        list: A list with all edges id that fulfill the condition
    """
    cursor = db.cursor()
    sql_sentence = 'select vehiclesLearning_%s_%s.edge from vehiclesLearning_%s_%s where vehiclesLearning_%s_%s.firstTime=1000' % (
        str(options.numextravehicles), num_repetition, str(
            options.numextravehicles), num_repetition,
        str(options.numextravehicles), num_repetition)
    cursor.execute(sql_sentence)
    result_row_current = cursor.fetchall()
    edges_id_array = []
    for i in range(0, len(result_row_current)):
        edges_id_array.append(str(result_row_current[i][0]))
        rtdm.update_progress(
            i + 2, len(result_row_current), 'EdgesIdInitials...')
    return edges_id_array


def get_vehicles_real_depart_info_congestion(options, edges_id_initials_array, db):
    """The function gets all specific parameters from vehicleLearning table in order to have a better approach of
    the results of the vehicles at depart time. This is for the congestion experiment

    Args:
        options (options): Options retrieved from command line
        edges_id_initials_array (list): The list of edges
        db (Database): The database

    Returns:
        list: A list with all the information of departing vehicles
    """
    cursor = db.cursor()
    edges_id_array = []
    for i in range(0, len(edges_id_initials_array)):
        edges_id = edges_id_initials_array[i]
        sql_sentence = 'select vehiclesLearning.vehicle,vehiclesLearning.edge,vehiclesLearning.firstTime, vehicles.route from vehiclesLearning,vehicles where vehiclesLearning.vehicle=vehicles.id and vehiclesLearning.edge="%s" union select vehiclesLearning.vehicle,vehiclesLearning.edge,vehiclesLearning.firstTime, vehiclesAdditional.route from vehiclesLearning,vehiclesAdditional where vehiclesLearning.vehicle=vehiclesAdditional.id and vehiclesLearning.edge="%s" order by vehiclesLearning.firstTime' % (
            edges_id, edges_id)
        cursor.execute(sql_sentence)
        result_row_current = cursor.fetchall()
        edges_id_array.append(result_row_current)
        rtdm.update_progress(i + 2, len(edges_id_initials_array),
                             'VehiclesRealDepartInfo...')
    # for j in range(0, len(result_rowCurrent)):edgesIdArray.append(str(result_rowCurrent[j]))
    return edges_id_array


def get_num_vehicles_way(options, way_name=None, additional=False, eco=True, sim_type=""):
    """The function gets the number of vehicles on a certain way

    Args:
        options (options): Options retrieved from command line
        way_name (str): The name of the way. Default None
        additional (bool): A bool to state if additional vehicles are inserted or not. Default False
        eco (bool): A bool to difference cars that changed route with eco parameter. Default True
        sim_type (str): A string to change the type name of the streets car file
    """
    db = sqlite3.connect(options.dbPath)

    vehicles_vector_total = []
    way_id_list = []
    if not eco:
        way_id_list = rtdm.find_id_way(options, way_name)
    else:
        way_id_list = rtdm.find_id_way(options, way_name, eco)
    cursor = db.cursor()
    k = 0

    for way_id in way_id_list:
        args = '%' + way_id + '%'
        sql_sentence = 'select DISTINCT routes.id from routes where routes.route like "%s"' % args
        cursor.execute(sql_sentence)
        result_row_current = cursor.fetchall()
        for i in range(0, len(result_row_current)):
            sql_sentence = 'select DISTINCT vehicles.id from vehicles where vehicles.route= "%s"' % \
                           result_row_current[i][0]
            cursor.execute(sql_sentence)
            result_row_current_a = cursor.fetchall()

            for j in range(0, len(result_row_current_a)):
                print(result_row_current_a[j][0])
                if result_row_current_a[j][0] not in vehicles_vector_total:
                    k = k + 1
                    vehicles_vector_total.append(result_row_current_a[j][0])
        if additional:
            for i in range(0, len(result_row_current)):
                sql_sentence2 = 'select DISTINCT vehiclesAdditional.id from vehiclesAdditional where vehiclesAdditional.route= "%s"' % \
                                result_row_current[i][0]
                cursor.execute(sql_sentence2)
                result_row_current_b = cursor.fetchall()

                for j in range(0, len(result_row_current_b)):
                    print(result_row_current_b[j][0])
                    if result_row_current_b[j][0] not in vehicles_vector_total:
                        k = k + 1
                        vehicles_vector_total.append(result_row_current_b[j][0])

    # Write file
    f_output = open(f'streets_cars{sim_type}.txt', 'w')
    for i in range(len(vehicles_vector_total)):
        f_output.write(vehicles_vector_total[i] + '\n')
    f_output.close()
    # Close the database
    db.close()


def get_vehicles_real_depart_info_congestion_eco(options, edges_id_initials_array, db):
    """The function gets all specific parameters from vehicleLearning table in order to have a better approach of
    the results of the vehicles at depart time. This is for the eco experiment
    Args:
        options (options): Options retrieved from command line
        edges_id_initials_array (list): The list of edges
        db (Database): The database

    Returns:
        list: A list with all the information of departing vehicles
    """
    cursor = db.cursor()
    edges_id_array = []
    for i in range(0, len(edges_id_initials_array)):
        edges_id = edges_id_initials_array[i]
        sql_sentence = 'select vehiclesLearning.vehicle,vehiclesLearning.edge,vehiclesLearning.firstTime, vehicles.route from vehiclesLearning,vehicles where vehiclesLearning.vehicle=vehicles.id and vehiclesLearning.edge="%s" union select vehiclesLearning.vehicle,vehiclesLearning.edge,vehiclesLearning.firstTime, vehiclesAdditional_1_0.route from vehiclesLearning,vehiclesAdditional_1_0 where vehiclesLearning.vehicle=vehiclesAdditional_1_0.id and vehiclesLearning.edge="%s" order by vehiclesLearning.firstTime' % (
            edges_id, edges_id)
        cursor.execute(sql_sentence)
        result_row_current = cursor.fetchall()
        edges_id_array.append(result_row_current)
        rtdm.update_progress(i + 2, len(edges_id_initials_array),
                             'VehiclesRealDepartInfo...')
    # for j in range(0, len(result_rowCurrent)):edgesIdArray.append(str(result_rowCurrent[j]))
    return edges_id_array


def get_nodes(options, db):
    """The function get all nodes from database

    Args:
        options (options): Options retrieved from command line
        db (Database): The database

    Returns:
        list: A list with all nodes
    """
    cursor = db.cursor()
    sql_sentence = 'select * from nodes'
    cursor.execute(sql_sentence)
    result_row = cursor.fetchall()
    nodes = []
    for i in range(0, len(result_row)):
        nodes.append(result_row[i][0])
    return nodes


def get_edges(options, db):
    """The function get all edges from database

    Args:
        options (options): Options retrieved from command line
        db (Database): The database

    Returns:
        list: A list with all edges
    """
    cursor = db.cursor()
    sql_sentence = 'select * from edges'
    cursor.execute(sql_sentence)
    result_row = cursor.fetchall()
    edges = []
    for i in range(0, len(result_row)):
        edges.append(result_row[i][0])
    return edges


def get_to_from_edge(options, db, node_from):
    """The function gets all the "to" value of edges with an specific "from"

    Args:
        options (options): Options retrieved from command line
        db (Database): The database
        node_from (str): The from node of an edge

    Returns:
        list: A list with all edges that fulfill the condition
    """
    cursor = db.cursor()
    sql_sentence = 'select edges."to" from edges where edges."from"=%s' % (
        node_from)
    cursor.execute(sql_sentence)
    result_row = cursor.fetchall()
    edges = []
    for i in range(0, len(result_row)):
        edges.append(result_row[i][0])
    return edges


def get_from_from_edge(options, db, node_to):
    """The function gets all the "from" value of edges with an specific "to"

    Args:
        options (options): Options retrieved from command line
        db (Database): The database
        node_to (str): The to node of an edge

    Returns:
        list: A list with all edges that fulfill the condition
    """
    cursor = db.cursor()
    sql_sentence = 'select edges."from" from edges where edges."to"=%s' % (
        node_to)
    cursor.execute(sql_sentence)
    result_row = cursor.fetchall()
    edges = []
    for i in range(0, len(result_row)):
        edges.append(result_row[i][0])
    return edges


def get_routes(db, route_id=""):
    """The function get all the routes from database

    Args:
        db (Database): The database
        route_id (str): The route id

    Returns:
        list: List of routes
    """
    if route_id == "":
        cursor = db.cursor()
        sql_sentence = 'select DISTINCT routes.id,routes.route from vehicles,routes where vehicles.route=routes.id'
        cursor.execute(sql_sentence)
        result_row = cursor.fetchall()
        routes = []
        for i in range(0, len(result_row)):
            routes.append(result_row[i])
        return routes
    else:
        cursor = db.cursor()
        sql_sentence = 'select DISTINCT routes.id from routes where routes.route like "%s"' % route_id
        cursor.execute(sql_sentence)
        routes_vector = cursor.fetchall()
        return routes_vector


def get_vehicles(options, db):
    """The function get all the vehicles from database

    Args:
        options (options): Options retrieved from command line
        db (Database): The database

    Returns:
        list: List of vehicles
    """
    cursor = db.cursor()
    sql_sentence = 'select * from vehicles'
    cursor.execute(sql_sentence)
    result_row = cursor.fetchall()
    vehicles = []
    for i in range(0, len(result_row)):
        vehicles.append(result_row[i][0])
    return vehicles


def get_route_from_external_source(options, edge_a, edge_b):
    """The function gets the specific route given two edges

    Args:
        options (options): Options retrieved from command line
        edge_a (str): Initial edge
        edge_b (str): Final edge

    Returns:
        str: The route between the edges
    """
    if os.path.exists('/home/josedaniel/MapaValencia2022/valencia2022.net.xml'):
        route = ''
        with open('trips.xml', 'w+') as tripFile:
            tripFile.write("<trips>\n")
            print('\t<trip id="1625993_25" depart="25" from="%s" to="%s"/>' % (edge_a, edge_b), file=tripFile)
            tripFile.write("</trips>\n")

        # os.system('duarouter -n valencia.net.xml -r trips.xml -o rou.xml > /dev/null')
        command_run = subprocess.call(['duarouter', '-n', '/home/josedaniel/MapaValencia2022/valencia2022.net.xml', '-r', 'trips.xml', '-o', 'rou.xml'],
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(edge_a)
        print(edge_b)
        # command_run = subprocess.call(['python3', '/home/josedaniel/sumo-1.13.0/tools/findAllRoutes.py', '-n',
        #                                '/home/josedaniel/MapaValencia2022/valencia2022.net.xml',
        #                                '-o', 'rou.xml', '-s', f'{edge_a}', '-t', f'{edge_b}'],
        #                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # time.sleep(5)
        # sts=subprocess.call('duarouter -n valencia.net.xml -r trips.xml -o rou.xml > /dev/null',shell=False)
        if command_run == 0:
            for vehicle in ElementTree.parse('rou.xml').findall('vehicle'):
                route = (
                    vehicle.find('route').attrib['edges'])
            os.system('rm trips.xml')
            os.system('rm rou.xml')
            os.system('rm rou.alt.xml')
        else:
            os.system('rm trips.xml')
            route = 'totalFailed'
    else:
        route = 'Warning: The file .net No Exist'
    return route


def get_coord_from_node(db, node_id):
    """The function get the coordinates of a given node

  Args:
      db (db): The database
      node_id (str): The node itself

  Returns:
      tuple: The coordinates of a given node
  """

    cursor = db.cursor()
    sql_sentence = f'select nodes.lat,nodes.lon from nodes where nodes.id={node_id}'
    cursor.execute(sql_sentence)
    result_row = cursor.fetchall()
    if result_row:
        return result_row[0]
    else:
        return None


def get_route_from_ABATIS(options, lat1, lon1, lat2, lon2, callback):
    """The function connect with ABATIS and get the route between two given coordinates

    Args:
        options (options): Options retrieved from command line

        lat1 (str): Latitude of point 1
        lon1 (str): Longitude of point 1
        lat2 (str): Latitude of point 2
        lon2 (str): Longitude of point 2

    Returns:
        list: The route between two points in a list format
        :param callback:
    """

    url = f"http://0.0.0.0:5000/route/v1/driving/{lon1},{lat1};{lon2},{lat2}.json?alternatives=true&steps=true&overview=full&geometries=geojson"
    try:
        data = (json.load(urlopen(url)))['routes'][0]['geometry']['coordinates']
        return callback(data)
    except HTTPError as e:
        content = e.read()  # Get the response content
        try:
            error_message = json.loads(content)
            if error_message.get('message') == 'No route found between points':
                print("No route found between points, skipping this pair...")
                return None
        except json.JSONDecodeError:
            print(f"HTTP error occurred: {e.code} {e.reason}")
            print(f"URL causing error: {url}")
            return None
