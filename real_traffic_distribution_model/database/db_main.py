import os
import sqlite3
import sys

# Important to execute it from terminal. This add the module to the PYTHONPATH
sys.path.append("/home/josedaniel/real_traffic_distribution_model")

import real_traffic_distribution_model as rtdm


def create(options):
    """This function creates the structure database of the project

    Args:
        options (options): Options retrieved from command line
    """
    if not os.path.exists(options.dbPath):
        db = sqlite3.connect(options.dbPath)

        cursor = db.cursor()

        cursor.execute(
            'CREATE TABLE nodes (id STRING PRIMARY KEY UNIQUE NOT NULL, lat DOUBLE (3, 7) NOT NULL, lon DOUBLE (3, 7) NOT NULL);')
        cursor.execute(
            'CREATE TABLE edgeType (id STRING PRIMARY KEY UNIQUE NOT NULL, speedKMH INTEGER (4) NOT NULL);')
        cursor.execute(
            'CREATE TABLE edges (id STRING PRIMARY KEY UNIQUE NOT NULL, [from] STRING REFERENCES nodes (id) NOT NULL, [to] STRING REFERENCES nodes (id) NOT NULL, speedOriginal DOUBLE (5,5), speedUpdated DOUBLE (5,5), length DOUBLE (8,4),edgeType STRING REFERENCES edgeType (id) NOT NULL);')
        cursor.execute(
            'CREATE TABLE routes (id BIGINT PRIMARY KEY UNIQUE, route STRING NOT NULL);')
        cursor.execute(
            'CREATE TABLE vehicles (id STRING PRIMARY KEY UNIQUE NOT NULL, depart DOUBLE (6, 2) NOT NULL, departLane  STRING NOT NULL, departPos   STRING NOT NULL, departSpeed STRING NOT NULL, route STRING NOT NULL REFERENCES routes (id));')
        db.commit()
        print('Database created!')
    else:
        print('The database was created in %s' % options.dbPath)


def insert_data(options):
    """Inserts the different elements into the database

    Args:
        options (options): Options retrieved from command line
    """

    db = sqlite3.connect(options.dbPath)

    rtdm.insert_nodes(options, db)

    rtdm.insert_edge_type(options, db)

    rtdm.insert_edges(options, db)

    #rtdm.insert_routes(options, db)

    #rtdm.insert_vehicles(options, db)

    db.close()
