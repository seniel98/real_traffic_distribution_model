import sys

# Important to execute it from terminal. This add the module to the PYTHONPATH
sys.path.append("/")

import real_traffic_distribution_model as main


def segment_id_into_edges(options, db, edge_id):
    """The function converts a given edge id into a segment id

    Args:
        options (options): Options retrieved from command line
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


def edge_2_coord(options, db, edge_id):
    """The function converts edge into coordinates

    Args:
        options (options): Options retrieved from command line
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


def edge_to_coordinates(options, db, edge_id_s, edge_id_d):
    """ The function convert edge to coordinates

    Args:
        options (options): Options retrieved from command line
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


def edge_to_nodes(options, db, edge_id):
    """The function converts a given edge into nodes

    Args:
        options (options): Options retrieved from command line
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


def coordinates_to_edge(options, db, coor_array):
    """The function converts coordinates to edge

    Args:
        options (options): Options retrieved from command line
        db (Database): The database
        coor_array (list): The coordinates of source and destination

    Returns:
        list: The edges that are related with those coordinates
    """
    cursor = db.cursor()
    s_coor_array = []

    for x in coor_array:
        s_coor = [float(((str(x[0]))[:(str(x[0])).index('.') + 6])), float(((str(x[1]))[:(str(x[1])).index('.') + 6]))]
        s_coor_array.append(s_coor)
    nodes_vector = []
    for i in range(0, len(s_coor_array)):
        node = []

        sql_sentence = 'select nodes.id from nodes where (nodes.lat like "%s%%" and nodes.lon like "%s%%")' % (
            "{0:.5f}".format(s_coor_array[i][1]), "{0:.5f}".format(s_coor_array[i][0]))
        cursor.execute(sql_sentence)
        result_row_current = cursor.fetchall()
        if result_row_current:
            node.append(result_row_current[0][0])
            nodes_vector.append(node)
    new_nodes = {}
    for i in range(0, len(nodes_vector) - 1):
        edge = main.is_edge(
            options, db, nodes_vector[i][0], nodes_vector[i + 1][0])
        if edge == '0':
            nodes = main.get_to_from_edge(options, db, nodes_vector[i][0])
            for j in range(0, len(nodes)):
                if nodes_vector[i + 1][0] in main.get_to_from_edge(options, db, nodes[j]):
                    new_nodes[nodes_vector[i][0]] = nodes[j]

    for key, value in new_nodes.items():
        node = [value]
        nodes_vector.insert(nodes_vector.index([key]) + 1, node)

    edges = {}
    edges_broken_index = []

    for i in range(0, len(nodes_vector) - 1):
        edge = main.is_edge(
            options, db, nodes_vector[i][0], nodes_vector[i + 1][0])
        if edge == '0':
            edges[i] = edge
            edges_broken_index.append(i)
        else:
            edges[i] = edge
    final_edges = []

    if '0' in edges.values():
        edges_aux = []
        for i in range(0, len(edges) - 1):
            if edges[i] == '0' and edges[i + 1] == '0':
                a = 0
            else:
                edges_aux.append(edges[i])
            if i + 1 == len(edges) - 1:
                edges_aux.append(edges[i + 1])
        edges = {}
        for i in range(0, len(edges_aux)):
            edges[i] = edges_aux[i]
        edges_broken_index = []
        for i in range(0, len(edges)):
            if edges[i] == '0':
                edges_broken_index.append(i)
        final_edges = main.fix_edges_broken(
            options, db, edges, edges_broken_index)
    else:
        for value in edges.items():
            final_edges.append(value[1])
    db.close()
    return final_edges
