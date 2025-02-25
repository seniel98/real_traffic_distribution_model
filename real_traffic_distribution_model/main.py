import os
import sys
import sqlite3
from optparse import OptionParser
import database
import tools
import traffic_model as tm
import simulation_files as sim
from datetime import datetime
import sumolib as sumo

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Global variables
optParser = OptionParser


def get_options():
    """The function establish the options that the user can pass through command line
    Returns:
        OptionParser: The total possible options
    """
    global optParser
    usage = " \n%prog [options] arg1 [options] arg2 [options] arg3...\n\nExample:\n\t %prog --useTrafficUpdate -i jlzrou.grc.upv.es -p 5000 \n\t %prog --useTool -i jlzrou.grc.upv.es -p 5000 \n\t %prog --createDB --db -s osmFile.xml -n netFile.xml -r routeFile.xml -a additionalFile.xml -b segmentsAllCategories.csv -v VehicleLearning.sta.csv \n\t %prog --createRoutesCoord --db \n\t***Traffic Congestion***\n\t %prog --createCongestion --db coor_node_edgesOriginal.db --numExtraVehicle 001 --numRepetition 19 \n\t %prog --useTrafficUpdateCongestion -i jlzrou.grc.upv.es -p 5000 --numExtraVehicle 001 --numRepetition 19"
    optParser = OptionParser(usage=usage, version="%prog v2.0",
                             description="Description:\tThis program links between ABATIS and SUMO-OMNeT++")
    optParser.add_option("--createDB", dest="createDB",
                         action="store_true", help="Mode Create DataBase (mandatory)")
    optParser.add_option("-s", "--osm-file", dest="osmfile",
                         help="define the osm file")
    optParser.add_option("-n", "--net-file", dest="netfile",
                         help="define the net file")
    optParser.add_option("-r", "--route-file", dest="routefile",
                         help="define the input route filename")
    optParser.add_option("-a", "--additional-file", dest="additionalfile",
                         help="define the input additional filename")
    optParser.add_option("-t", "--traffic_file", dest="traffic_file",
                         help="CSV with the information of the traffic")
    optParser.add_option("--rd", "--routes_data", dest="routes_data",
                         default="/home/josedaniel/Modelo_distrib_trafico_real/routes_data/gen_routes_data_1p_1.1_v2_full.csv",
                         help="CSV with the information of the routes")
    optParser.add_option("-d", "--db", dest="dbPath",
                         default="/home/josedaniel/Algoritmo_rutas_eco/TrafficDB/network_data_edited.db",
                         help="Name of a database")
    optParser.add_option("--tdb", "--traffic_db", dest="traffic_db",
                         default="/home/josedaniel/Algoritmo_rutas_eco/TrafficData/way_nodes_relation.db",
                         help="Name of a database")
    optParser.add_option("--useTool", dest="useTool",
                         action="store_true", help="Mode Use Tool(mandatory)")
    optParser.add_option("-i", "--ip", dest="ip",
                         default="0.0.0.0", help="IP Address")
    optParser.add_option("-p", "--port", dest="port",
                         default="5000", help="TCP/IP Port")
    optParser.add_option("--useTrafficUpdate", dest="useTrafficUpdate", action="store_true",
                         help="Mode Traffic Update(mandatory)")
    optParser.add_option("--startABATIS", dest="startABATIS",
                         action="store_true", help="Mode Start ABATIS")
    optParser.add_option("--createCongestion", dest="congestion", action="store_true",
                         help="Mode Create traffic congestion")
    optParser.add_option("--generate_sim_files", dest="generate_sim_files", action="store_true",
                         help="Generate the necessary files for simulating")
    optParser.add_option("--useTrafficUpdateCongestion", dest="useTrafficUpdateCongestion", action="store_true",
                         help="Mode Create traffic update congestion")
    optParser.add_option("--numExtraVehicle", dest="numextravehicles", default="0",
                         help="Number of vehicle for creating traffic congestion")
    optParser.add_option("--numRepetition", dest="numrepetition", default="1",
                         help="Number of repetition for creating traffic congestion")
    optParser.add_option("--infoCongestion", dest="infocongestion", action="store_true",
                         help="Mode unify all repetition in an experiment")
    optParser.add_option("--eco", dest="eco_mode",
                         action="store_true", help="Active tool about eco system")
    optParser.add_option("--streets-file", dest="streetsfile",
                         help="Csv with the names of the streets of an area")
    optParser.add_option("--eco-streets-file", dest="ecostreetsfile",
                         help="Csv with the names of the streets to modify weight")
    optParser.add_option("--alpha", dest="alpha",
                         help="Parameter that influence on the weight of the road and its speed value")
    optParser.add_option("--generate_routes", dest="generate_routes",
                         help="Generate the o-d matrix for the traffic data", action="store_true")
    optParser.add_option("--generate_vehicles", dest="generate_vehicles",
                         help="Generate the vehicles distribution in time for the traffic data", action="store_true")
    optParser.add_option("--districts-file", dest="districts_file",
                         help="Districts info file")

    (options, args) = optParser.parse_args()

    main_actions(options)

    return options


def main_actions(options):
    """The function retrieves data from command line and executes the actions

    Args:
        options (options): Options retrieved from the command line
    """
    if options.startABATIS and options.ip and options.port:
        tools.start_ABATIS(options)
    else:
        if tools.server_is_alive(options) and tools.port_is_alive(options):

            if options.createDB and options.osmfile and options.netfile:
                database.create(options)
                database.insert_data(options)

            elif options.generate_routes:
                start = datetime.now()
                net = sumo.net.readNet(str(options.netfile))
                tm.create_od_routes(options, net)
                end = datetime.now()
                print(f'Execution time: {end - start}')

            elif options.generate_vehicles:
                start = datetime.now()
                print(options)
                tm.generate_vehicles_distribution(options)
                end = datetime.now()
                print(f'Execution time: {end - start}')

            elif options.generate_sim_files and options.dbPath and options.osmfile:
                vehicles_distribution_dict = {"electric": 15.2, "hybrid": 5, "gasoline_c": 25, "gas_oil_c": 25, "gasoline_b": 15, "gas_oil_b": 15}
                sim.write_simulation_files(sqlite3.connect(options.dbPath), name="scenario2_", sim_type="no_rr",
                                           vehicle_type_dict=vehicles_distribution_dict)

            else:
                optParser.error('Command incomplete, please check again or use -h for help')
        else:
            #    optParser.error('Server is not alive!!')
            tools.start_ABATIS(options)


############################
def main(options):
    print('\nDone!')


if __name__ == "__main__":
    main(get_options())
