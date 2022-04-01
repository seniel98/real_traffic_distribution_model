import math
import os
import re
import socket
import sqlite3
import sys
from xml.etree import ElementTree

import pandas as pd

# Important to execute it from terminal. This add the module to the PYTHONPATH
sys.path.append("/home/josedaniel/real_traffic_distribution_model")

import real_traffic_distribution_model as rtdm

# Global variables
edge_id_s = ""
edge_id_d = ""


def update_progress(progress, maxi, messages):
    """ The function to update the progress of a given process

    Args:
        progress (int): Number of processed elements
        maxi (int): Maximum number of elements
        messages (str): Message of the action
    """
    progress2 = '%i/%i' % (progress, maxi)
    x = (progress * 100) / maxi
    if progress > maxi:
        sys.stdout.write("\n")
    else:
        sys.stdout.write('\r{0}... {1} {2}%'.format(
            messages, progress2, round(x)))
    sys.stdout.flush()


def natural_key(string_):
    """ The function

    Args:
        string_ (str): String

    Returns:
        list:
    """
    return [int(s) if s.isdigit() else s for s in re.split(r'(\d+)', string_)]


def normalize(s):
    """The function delete the accents from the string

    Args:
        s (str): Way name

    Returns:
        str: Way name normalized
    """
    replacements = (
        ("á", "a"),
        ("é", "e"),
        ("í", "i"),
        ("ó", "o"),
        ("ú", "u"),
    )
    for a, b in replacements:
        s = s.replace(a, b).replace(a.upper(), b.upper())
    return s


def kmph_to_mps(kmph): return 0.277778 * kmph


def mps_to_kmph(mps): return 3.6 * mps


def average(data):
    """The function calculates the average

    Args:
        data: The data to calculate the average

    Returns:
        int: The result of average calculation

    """
    return round(sum(data) * 1.0 / len(data), 4)


def median(data):
    """The function calculates the median of certain data

    Args:
        data: he data to calculate the median

    Returns:
        int: The result of median calculation
    """
    data.sort()
    if len(data) % 2 == 0:
        n = len(data)
        median_obj = (data[n / 2 - 1] + data[n / 2]) / 2
    else:
        median_obj = data[len(data) / 2]
    return median_obj


def distance_2_points(lat_a, lon_a, lat_b, lon_b):
    """The function gets the distance between two points expressed in km, with an error of 0.3%

    Args:
        lat_a (float): The latitude of point a
        lon_a (float): The longitude of point a
        lat_b (float): The latitude of point b
        lon_b (float): The longitude of point b

    Returns:
        int: The distance between two given coordinates
    """
    return round(((6372.137 * math.acos((math.sin(math.radians(lat_a)) * math.sin(math.radians(lat_b))) + (
            math.cos(math.radians(lat_a)) * math.cos(math.radians(lat_b)) * math.cos(
        math.radians(lon_a) - math.radians(lon_b))))) * 1000), 2)


def mail_notification(now, finish, message_mail):
    """The function sends a mail notifying when an operation has finished

    Args:
        now (datetime): The actual date
        finish (datetime): The finish time
        message_mail (str): The message to send
    """
    # os.system('\n echo "GenerateDataBase.py has\n started:\n Time: %s\n finished:\n Time: %s \n %s \n Greeting from your computer" | /usr/bin/mutt -s "Script finished in %s" jorgenluis004@hotmail.com'%(str(now), str(finish),messageMail,socket.gethostname()))
    os.system(
        '\n echo "linkABATIS_rtdm.py has\n started:\n Time: %s\n finished:\n Time: %s \n\n %s \n\n\n Greeting from your computer" | mutt -s "Script finished in %s" jdpadron98@gmail.com' % (
            str(now), str(finish), message_mail, socket.gethostname()))


def fix_edges_broken(options, db, edges, edges_broken_list):
    """The function fix all the possible edges that are broken

    Args:
        options (options): Options retrieved from the command line 
        db (Database): The database
        edges (dict): The dictionary of all edges
        edges_broken_list (list): The list of edges broken

    Returns:
        list: A list with all edges
    """
    add_edges = {}
    new_edges = []
    total_fail = ''

    for i in range(0, len(edges_broken_list)):
        if 0 == edges_broken_list[i]:
            edge_a = edge_id_s
        else:
            edge_a = edges[edges_broken_list[i] - 1]

        if len(edges) - 1 == edges_broken_list[i]:
            edge_b = edge_id_d
        else:
            edge_b = edges[edges_broken_list[i] + 1]

        result_row = (rtdm.get_route_from_external_source(options, edge_a, edge_b))
        if result_row != 'totalFailed':
            edges_founded = (
                result_row[(result_row.index(edge_a) + len(edge_a) + 1):(result_row.index(edge_b) - 1)]).split(
                ' ')
            add_edges[edges_broken_list[i]] = edges_founded
        else:
            total_fail = 'totalFailed'
    if total_fail == '':
        # Fusion both dictionaries
        for i in range(0, len(edges)):
            for j in range(0, len(add_edges)):
                if edges.keys()[i] == add_edges.keys()[j]:
                    edges[edges.keys()[i]] = add_edges[edges.keys()[i]]
        # Dictionary to vector completely
        for i in range(0, len(edges)):
            if not isinstance(edges[i], str):
                for j in range(0, len(edges.values()[i])):
                    new_edges.append(str(edges.values()[i][j]))
            else:
                new_edges.append(edges.values()[i])
    else:
        new_edges = (rtdm.get_route_from_external_source(
            options, edge_id_s, edge_id_d)).split(' ')
    return new_edges


def original_speed_from_ABATIS_default(options, db, edge_type):
    """The function gets the original speed of the edge type

    Args:
        options (options): Options retrieved from command line
        db (Database): The database
        edge_type (str): The type of the edge

    Returns:
        int: The speed for a given edge type
    """
    cursor = db.cursor()
    sql_sentence = 'select edgeType.speedKMH from edgeType where edgeType.id="%s"' % (
        edge_type)
    cursor.execute(sql_sentence)
    result_row_current = cursor.fetchall()
    return kmph_to_mps(result_row_current[0][0])


def edges_from_ABATIS(options, route_id):
    """The function request ABATIS a route id

    Args:
        options (options): Options retrieved from command line
        route_id (str): The id of the selected route

    Returns:
        list: List of edges
    """
    global edge_id_s, edge_id_d

    edge_id_s = route_id[:route_id.index('_')]
    # it makes a substring depending if exist @ or not, because sometimes a routeID can be repeated
    if '@' not in route_id:
        edge_id_d = route_id[route_id.index('to_') + 3:]
    else:
        edge_id_d = route_id[route_id.index('to_') + 3:route_id.index('@')]

    if edge_id_s == edge_id_d:
        edges = edge_id_s
    else:

        coor = rtdm.edge_to_coordinates(options, sqlite3.connect(
            options.dbPath), edge_id_s, edge_id_d)

        try:
            # request to ABATIS a route
            # rutas obtenidas en array

            coor_array = rtdm.get_route_from_ABATIS(options, coor[coor.index(',') + 1:coor.index('|')],
                                                    coor[:coor.index(',')], coor[coor.rindex(',') + 1:],
                                                    coor[coor.index('|') + 1:coor.rindex(',')])
            # time.sleep(1)
            edges_array = rtdm.coordinates_to_edge(
                options, sqlite3.connect(options.dbPath), coor_array)
            if (check_order_route(options, sqlite3.connect(options.dbPath), edges_array)) and len(edges_array) > 0:
                edges = ' '.join(str(x) for x in edges_array)
            else:
                # get from external source because in sometimes matching the same coordinate, it's the last resource
                print("aqui")
                edges = rtdm.get_route_from_external_source(
                    options, edge_id_s, edge_id_d)
        except Exception as e:
            edges = edge_exception(options, route_id)
    return edges


def find_id_way(options, way_name=None, eco=False):
    """The function returns a list of ids from the .osm file depending on the given name

    Args:
        options (options):  Options retrieved from command line
        way_name (str): Name of the way to find. Defaults to None.
        eco (bool): Boolean to state if the eco mode is called or not. Default False.
    Returns:
        list: Id list of ways for the given way name or list
    """
    if way_name is not None:
        document_osm_file = ElementTree.parse(str(options.osmfile))
        id_list = []
        for way in document_osm_file.findall('way'):
            id_way = way.attrib['id']
            if way.find('tag') is not None:
                for t in way.findall('tag'):
                    if t.attrib['k'] == 'name':
                        source = t.attrib['v']
                        if source == way_name:
                            id_list.append(id_way)
        id_list = list(set(id_list))
        return id_list
    else:
        if not eco:
            df = pd.read_csv(options.streetsfile)
        else:
            df = pd.read_csv(options.ecostreetsfile)
        street_list = df.street_name.to_list()
        document_osm_file = ElementTree.parse(str(options.osmfile))
        id_list = []
        for street in street_list:
            for way in document_osm_file.findall('way'):
                id_way = way.attrib['id']
                if way.find('tag') is not None:
                    for t in way.findall('tag'):
                        if t.attrib['k'] == 'name':
                            source = t.attrib['v']
                            if source == street:
                                id_list.append(id_way)
        id_list = list(set(id_list))
        return id_list


def edge_exception(options, route_id):
    """The function gets the edges for a given route

    Args:
        options (options): Options retrieved from the command line
        route_id (str): The route id

    Returns:
        str: The edges that contains a route
    """
    db = sqlite3.connect(options.dbPath)
    cursor = db.cursor()
    sql_sentence = 'select routes.route from routes where routes.id="%s"' % route_id
    cursor.execute(sql_sentence)
    result_row_current = cursor.fetchall()
    db.close()
    return result_row_current[0][0]


def check_order_route(options, db, edges_array):
    """The function checks the route order

    Args:
        options (options): Options retrieved from the command line
        db (Database): The database
        edges_array (list): The list of edges

    Returns:
        bool: A boolean that says if the order is correct or not
    """
    cursor = db.cursor()
    state = True
    for i in range(0, len(edges_array) - 1):
        sql_sentence = 'select edges."to" from edges where edges.id="%s"' % (
            edges_array[i])
        sql_sentence2 = 'select edges."from" from edges where edges.id="%s"' % (
            edges_array[i + 1])
        cursor.execute(sql_sentence)
        result1 = cursor.fetchall()
        cursor.execute(sql_sentence2)
        result2 = cursor.fetchall()
        if result1[0][0] != result2[0][0]:
            state = False
            break
    db.close()
    return state


def is_edge(options, db, node_from, node_to):
    """The function gets the edge id for two given nodes

    Args:
        options (options): Options retrieved from the command line
        db (Database): The database
        node_from (str): The initial node
        node_to (str): The final node

    Returns:
        str: The edge id
    """
    cursor = db.cursor()
    sql_sentence = 'select edges.id from edges where edges."from"=%s and edges."to"=%s' % (
        node_from, node_to)
    cursor.execute(sql_sentence)
    result_row = cursor.fetchall()
    if result_row:
        edge = str(result_row[0][0])
    else:
        edge = str(0)
    return edge
