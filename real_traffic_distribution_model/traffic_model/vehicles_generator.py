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
percentage = 50
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
    # Group by 'src_edge' and calculate the sum of 'n_vehicles'
    # Split 'route_id' to extract 'src_edge'
    df_veh_per_route['src_edge'] = df_veh_per_route['route_id'].str.split('_to_').str[0]
    # Get the way id by getting the number in front of the hashtag
    df_veh_per_route['src_street'] = df_veh_per_route['src_edge'].str.split('#').str[0]
    result = df_veh_per_route.groupby('src_street').agg({'n_vehicles': 'sum', 'route_id': 'unique', 'src_edge': 'unique'}).reset_index()

    # Drop the src_edge column
    result.drop(columns=['src_edge'], inplace=True)

    # Rename the columns for clarity
    result.rename(columns={'n_vehicles': 'total_vehicles', 'route_id': 'associated_routes'}, inplace=True)

    for index, row in result.iterrows():
        is_vehicle_in_route = False
        n_vehicles = row['total_vehicles']
        num_routes_associated = len(row['associated_routes'])
        dist = np.random.poisson(n_vehicles / PERIOD, PERIOD)
        vehicles_calculated = sum(dist)
        print(f"Vehicles calculated: {vehicles_calculated}")
        print(f"Vehicles real: {n_vehicles}")
        temp_routes = 0
        for i in range(0, len(dist)):
            if temp_routes == num_routes_associated:
                temp_routes = 0
            if dist[i] < 1:
                continue
            elif dist[i] == 1:
                # time = round(i / 60, 2)
                # time_str = str(time).replace(".", "")
                time_str = str(i)
                # src_edge = row["src_edge"]
                src_edge = row["associated_routes"][temp_routes].split("_to_")[0]
                vehicle_name = f'emitter_dd_{src_edge}_{time_str}'
                time_list.append(i)
                vehicle_list.append(vehicle_name)
                route_list.append(row["associated_routes"][temp_routes])
                is_vehicle_in_route = True
                temp_routes += 1
            elif dist[i] > 1:
                is_vehicle_in_route = True
                for j in range(0, dist[i]):
                    if temp_routes == num_routes_associated:
                        temp_routes = 0
                    # time = round(i / 60, 2)
                    # if j != 0:
                    #     time += round((1 / j), 2)
                    # time_str = str(time).replace(".", "")
                    time_str = str(i)
                    # src_edge = row["src_edge"]
                    src_edge = row["associated_routes"][temp_routes].split("_to_")[0]
                    vehicle_name = f'emitter_dd_{src_edge}_{time_str}_{j}'
                    time_list.append(i)
                    vehicle_list.append(vehicle_name)
                    route_list.append(row["associated_routes"][temp_routes])
                    temp_routes += 1
        if not is_vehicle_in_route:
            i = np.random.randint(0, PERIOD)
            # src_edge = row["src_edge"]
            src_edge = row["associated_routes"][0].split("_to_")[0]
            vehicle_name = f'emitter_dd_{src_edge}_{str(i)}'
            time_list.append(i)
            vehicle_list.append(vehicle_name)
            route_list.append(row["associated_routes"][0])

    vehicle_data = {'vehicle_id': vehicle_list, 'depart': time_list, 'departLane': ["best"] * len(vehicle_list),
                    'departPos': ["last"] * len(vehicle_list),
                    'departSpeed': ["avg"] * len(vehicle_list),
                    'route': route_list}
    vehicle_df = pd.DataFrame.from_dict(vehicle_data)
    vehicle_df_clean = vehicle_df.drop_duplicates(subset=['vehicle_id'])
    vehicle_df_clean.to_csv(f'/home/josedaniel/Modelo_distrib_trafico_real/vehicle_files/vehicle_data_{percentage}p_{tolerated_error}_net_edited_full.csv', index=False)


def get_route_src_edge(route):
    return route.split("_to_")[0]
