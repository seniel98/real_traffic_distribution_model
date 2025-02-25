import sys
import re
from random import seed
from random import randint

# Important to execute it from terminal. This add the module to the PYTHONPATH
sys.path.append("/home/josedaniel/real_traffic_distribution_model")

import real_traffic_distribution_model as rtdm


def atoi(text):
    return int(text) if text.isdigit() else text


def natural_keys(text):
    '''
    alist.sort(key=natural_keys) sorts in human order
    http://nedbatchelder.com/blog/200712/human_sorting.html
    (See Toothy's implementation in the comments)
    '''
    return [atoi(c) for c in re.split(r'(\d+)', text)]


def segment_id_into_edges(db, edge_id):
    """
    The function converts a given edge id into a segment id

    Args:
        db (Database): The database
        edge_id (str): Edge id

    Returns:
        str: The segment id associated with the edge id
    """
    cursor = db.cursor()
    segment_id = ''
    if "#" in edge_id:
        name = edge_id[:edge_id.index('#')]
        num_part = edge_id[edge_id.index('#') + 1:]
        sql_sentence = "select segmentsUnified.id from segmentsUnified where segmentsUnified.id like '%s%%'" % name
        cursor.execute(sql_sentence)
        result_row_current = cursor.fetchall()
        for i in range(0, len(result_row_current)):
            parts_segment_id = str(result_row_current[i][0]).split('#')
            for j in range(0, len(parts_segment_id)):
                if num_part == parts_segment_id[j]:
                    segment_id = (str(result_row_current[i][0]))
    else:
        sql_sentence = "select segmentsUnified.id from segmentsUnified where segmentsUnified.id like '%s%%'" % edge_id
        cursor.execute(sql_sentence)
        result_row_current = cursor.fetchall()
        for i in range(0, len(result_row_current)):
            segment_id = (str(result_row_current[i][0]))

    if segment_id == '':
        edge_id = '-%s' % edge_id
        if "#" in edge_id:
            name = edge_id[:edge_id.index('#')]
            num_part = edge_id[edge_id.index('#') + 1:]
            sql_sentence = "select segmentsUnified.id from segmentsUnified where segmentsUnified.id like '%s%%'" % name
            cursor.execute(sql_sentence)
            result_row_current = cursor.fetchall()
            for i in range(0, len(result_row_current)):
                parts_segment_id = str(result_row_current[i][0]).split('#')
                for j in range(0, len(parts_segment_id)):
                    if num_part == parts_segment_id[j]:
                        segment_id = (str(result_row_current[i][0]))
        else:
            sql_sentence = "select segmentsUnified.id from segmentsUnified where segmentsUnified.id like '%s%%'" % edge_id
            cursor.execute(sql_sentence)
            result_row_current = cursor.fetchall()
            for i in range(0, len(result_row_current)):
                segment_id = (str(result_row_current[i][0]))
    return segment_id


def edge_2_coord(db, edge_id):
    """
    The function converts edge into coordinates

    Args:
        db (Database): The database
        edge_id (str): The id of the edge

    Returns:
        list: Pair of coordinate. I.E.: xº,xº
    """
    cursor = db.cursor()
    sql_sentence = 'select edges."from",edges."to" from edges where edges.id="%s"' % (
        edge_id)
    cursor.execute(sql_sentence)
    result_row = cursor.fetchall()
    sql_sentence = 'select nodes.lat,nodes.lon from nodes where nodes.id="%s"' % (
        result_row[0][0])
    sql_sentence2 = 'select nodes.lat,nodes.lon from nodes where nodes.id="%s"' % (
        result_row[0][1])
    cursor.execute(sql_sentence)
    result_row = cursor.fetchall()
    node_from = [result_row[0]]
    cursor.execute(sql_sentence2)
    result_row = cursor.fetchall()
    node_to = [result_row[0]]
    coor = [node_from, node_to]
    return coor


def edge_to_coordinates(db, edge_id_s, edge_id_d):
    """
    The function convert edge to coordinates

    Args:
        db (Database): The database
        edge_id_s (str): The source edge
        edge_id_d (str): The destination edge

    Returns:
        list: The coordinates of the two given edges
    """
    cursor = db.cursor()
    sql_sentence = 'select edges.id,nodes.id,edges.speedUpdated,edges.length,nodes.lat,nodes.lon from edges, nodes where (edges."from"=nodes.id) and edges.id="%s"' % (
        edge_id_s)
    sql_sentence2 = 'select edges.id,nodes.id,edges.speedUpdated,edges.length,nodes.lat,nodes.lon from edges, nodes where (edges."to"=nodes.id) and edges.id="%s"' % (
        edge_id_d)
    lat_long_source_destination = []
    cursor.execute(sql_sentence)
    for row in cursor:
        lat_long_source_destination.append('%s,%s' % (row[4], row[5]))
    cursor.execute(sql_sentence2)
    for row in cursor:
        lat_long_source_destination.append('%s,%s' % (row[4], row[5]))
    db.close()
    return "%s|%s" % (lat_long_source_destination[0], lat_long_source_destination[1])


def edge_2_coord(db, lat, lon):
    """
    It takes in a database, a latitude and a longitude, and returns the edge id of the edge that contains the node with the
    given latitude and longitude

    Args:
      db: the database connection
      lat: latitude of the point
      lon: longitude of the point

    Returns:
      The edge id of the edge that starts at the node with the given coordinates.
    """
    cursor = db.cursor()
    sql_sentence = f'select nodes.id from nodes where nodes.lat like"%{lat}%" and nodes.lon like "%{lon}%"'
    cursor.execute(sql_sentence)
    node = cursor.fetchall()
    if node:
        sql_sentence_2 = f'select edges.id from edges where edges."from"="{node[0][0]}" '
        cursor.execute(sql_sentence_2)
        edge = cursor.fetchall()[0][0]
        return edge
    else:
        return None


def node_2_coord(db, node_id):
    """
    It takes a node id as input and returns the latitude and longitude of that node

    Args:
      db: the database connection
      node_id: the id of the node you want to find the coordinates of

    Returns:
      A list of tuples with the latitude and longitude of the node.
    """
    cursor = db.cursor()
    sql_sentence = f'select nodes.lat, nodes.lon from nodes where nodes.id like "{node_id}"'
    cursor.execute(sql_sentence)
    coord = cursor.fetchall()
    return coord


def coord_2_node(db, lat, lon):
    cursor = db.cursor()
    sql_sentence = f'select nodes.id from nodes where nodes.lat like "%{lat}%" and nodes.lon like "%{lon}%"'
    cursor.execute(sql_sentence)
    node = cursor.fetchall()
    if node:
        return node[0][0]
    else:
        return None


def edge_to_nodes(db, edge_id):
    """
    The function converts a given edge into nodes

    Args:
        db (Database): The database
        edge_id ([type]): [description]

    Returns:
        [type]: [description]
    """
    cursor = db.cursor()
    sql_sentence = 'select edges.id,edges."from",edges."to",edges.speed,edges.length from edges where edges.id="%s"' % (
        edge_id)
    nodes_from_to = []
    cursor.execute(sql_sentence)
    for row in cursor:
        nodes_from_to.append('%s|%s' % (row[1], row[2]))
    db.close()
    return "%s" % (nodes_from_to[0])


def coord_to_edges(db, lat, lon):
    """
    The function converts a given coordinate into edges

    Args:
        db (Database): The database
        lat (float): Latitude
        lon (float): Longitude

    Returns:
        [type]: [description]
    """
    cursor = db.cursor()
    sql_sentence = 'select nodes.id from nodes where nodes.lat like "%s%%" and nodes.lon like "%s%%"' % (
        lat, lon)
    cursor.execute(sql_sentence)
    node = cursor.fetchall()
    if node:
        sql_sentence_2 = 'select edges.id from edges where edges."from"="%s" or edges."to"="%s"' % (
            node[0][0], node[0][0])
        cursor.execute(sql_sentence_2)
        edge = cursor.fetchall()
        return edge
    else:
        return None


def coordinates_to_edge(coor_array, net, primary_count, roundabouts):
    # Use if ABATIS is used
    way_id_name_start = coor_array[0]['name']
    way_id_name_end = coor_array[-1]['name']
    edges_set_start = list(net.getEdgesByOrigID(way_id_name_start))
    edges_set_end = list(net.getEdgesByOrigID(way_id_name_end))

    if not edges_set_start or not edges_set_end:
        return None, None, None, primary_count

    src_edge = edges_set_start[randint(0, len(edges_set_start) - 1)].getID() if len(edges_set_start) > 1 else \
    edges_set_start[0].getID()
    dst_edge = edges_set_end[randint(0, len(edges_set_end) - 1)].getID() if len(edges_set_end) > 1 else edges_set_end[
        0].getID()

    if src_edge in roundabouts or dst_edge in roundabouts:
        return None, None, None, primary_count
    # Reject the selection if the count of primary links reaches 3 or if any edge is shorter than 75 units.
    if net.getEdge(src_edge).getLength() < 75 or net.getEdge(dst_edge).getLength() < 75 or net.getEdge(
            src_edge).getType() == "highway.primary_link":
        return None, None, None, primary_count
    # if net.getEdge(src_edge).getLength() < 25 or net.getEdge(dst_edge).getLength() < 25 or net.getEdge(src_edge).getType() == "highway.primary":
    #    return None, None, None

    path, _ = net.getFastestPath(net.getEdge(src_edge), net.getEdge(dst_edge))
    if path is None:
        return None, None, None, primary_count

    # # Check if the selected source edge is a primary link.
    # if net.getEdge(src_edge).getType() == "highway.primary":
    #     primary_count += 1  # Increment the count if it's a primary link.

    final_edges = [edge.getID() for edge in path]
    final_nodes = [node.getID() for edge in path for node in (edge.getFromNode(), edge.getToNode())]
    final_coords = [net.convertXY2LonLat(*net.getNode(node).getCoord()) for node in final_nodes]

    return final_coords, final_nodes, final_edges, primary_count


