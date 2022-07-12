import sys
import pandas as pd
import numpy as np

sys.path.append("/home/josedaniel/real_traffic_distribution_model")

# P(X=k) = λk * e– λ / k!
# λ: mean number of successes that occur during a specific interval
# k: number of successes
# e: a constant equal to approximately 2.71828

# generate random values from Poisson distribution with mean=3 and sample size=10

PERIOD = 3600
percentage = 25
tolerated_error = 1.1


def generate_vehicles_distribution(options):
    """
    It takes the routes dataframe and creates a new dataframe with the vehicles distribution

    Args:
      options: the options object that contains the routes data file
    """
    vehicle_list = []
    time_list = []
    route_list = []
    routes_data_df = pd.read_csv(options.routes_data)
    df_veh_per_route = routes_data_df.pivot_table(index=['route_id'], aggfunc='size').to_frame().reset_index()
    df_veh_per_route.rename(columns={0: "n_vehicles", "index": "route_id"}, inplace=True)
    for index, row in df_veh_per_route.iterrows():
        n_vehicles = row['n_vehicles'] * 1
        dist = np.random.poisson(n_vehicles / PERIOD, PERIOD)
        for i in range(0, len(dist)):
            if dist[i] < 1:
                continue
            elif dist[i] == 1:
                time = round(i / 60, 2)
                time_str = str(time).replace(".", "")
                src_edge = row["route_id"].split("_to_")[0]
                vehicle_name = f'emitter_dd_{src_edge}_{time_str}'
                time_list.append(time)
                vehicle_list.append(vehicle_name)
                route_list.append(row["route_id"])
            elif dist[i] > 1:
                for j in range(0, dist[i]):
                    time = round(i / 60, 2)
                    if j != 0:
                        time += round((1 / j), 2)
                    time_str = str(time).replace(".", "")
                    src_edge = row["route_id"].split("_to_")[0]
                    vehicle_name = f'emitter_dd_{src_edge}_{time_str}'
                    time_list.append(time)
                    vehicle_list.append(vehicle_name)
                    route_list.append(row["route_id"])

    vehicle_data = {'vehicle_id': vehicle_list, 'depart': time_list, 'departLane': ["best"] * len(vehicle_list),
                    'departPos': ["last"] * len(vehicle_list),
                    'departSpeed': ["max"] * len(vehicle_list),
                    'route': route_list}
    vehicle_df = pd.DataFrame.from_dict(vehicle_data)
    vehicle_df_clean = vehicle_df.drop_duplicates(subset=['vehicle_id'])
    vehicle_df_clean.to_csv(f'/home/josedaniel/vehicle_data_{percentage}p_{tolerated_error}.csv', index=False)
