import os
import platform  # For getting the operating system name
import socket
import subprocess  # For executing a shell command
import sys

# Important to execute it from terminal. This add the module to the PYTHONPATH
sys.path.append("/home/josedaniel/real_traffic_distribution_model")

import real_traffic_distribution_model as rtdm

# Global variables
username = socket.gethostname()


def ABATIS_update_traffic(is_remote, options, csv_file):
    """The function checks if the call of the server is remote or not and updates the traffic file

    Args:
        is_remote (bool): A boolean to say if the connections is remote or not
        options (options): Options retrieved from command line
        csv_file (str): The path to the csv update file
    """
    if not is_remote:
        # Copy a file generate that contains a traffic update_specific_traffic_csv
        os.system("cp %s /home/josedaniel/osrm-backend/build" %
                  csv_file)
        # Update ABATIS Server
        os.system("'startABATIS_MLD'")
    else:
        # Copy a file generate that contains a traffic update_specific_traffic_csv
        os.system("scp %s josedaniel@%s:/home/josedaniel/osrm-backend/build" % (
            csv_file, options.ip))
        # It's for update in ABATIS Server
        os.system("ssh %s 'startABATIS_MLD'" % options.ip)


def start_ABATIS(options):
    """The function starts ABATIS server

    Args:
        options (options): Options retrieved from command line
    """
    if (server_is_alive(options)) and (not port_is_alive(options)):
        print('Starting ABATIS...')
        # os.system("ssh -q jorzamma@%s 'sh startABATIS_MLD_MTL > /dev/null'"%(options.ip)) mtl--> no funcionaba con sh, seguramente al iniciarlo a nivel local
        subprocess.run('startABATIS_MLD', stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, shell=True)
        print('ABATIS is started...')
    elif (server_is_alive(options)) and (port_is_alive(options)):
        print('ABATIS is alive...')
    elif not server_is_alive(options):
        print('ABATIS is dead, look server')
    elif not port_is_alive(options):
        print('ABATIS is dead, look port')


def port_is_alive(options):
    """The function checks if the port is already in use 

    Args:
        options (options): Options retrieved from command line

    Returns:
        bool: True if its free, False if its ocuppied
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)  # 1 Second Timeout
        result = sock.connect_ex((options.ip, int(options.port)))
        if result == 0:
            return True
        else:
            return False
    except Exception as e:
        return False


def server_is_alive(options):
    """The function checks if the server is ailve.
    Remember that a host may not respond to a ping (ICMP) request even if the hostname is valid.
    Args:
        options (options): Options retrieved from command line

    Returns:
        bool: True if host (str) responds to a ping request.
    """
    try:
        # Option for the number of packets as a function of
        param = '-n' if platform.system().lower() == 'windows' else '-c'

        # Building the command. Ex: "ping -c 1 google.com"
        command = ['ping', param, '1', options.ip]
        return subprocess.call(command) == 0
    except Exception as e:
        return False
