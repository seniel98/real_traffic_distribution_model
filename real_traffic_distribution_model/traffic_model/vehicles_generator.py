import sys
import pandas as pd
import sqlite3
import random
import numpy as np
from geopy.distance import geodesic
from datetime import datetime
from scipy.stats import poisson

sys.path.append("/home/josedaniel/real_traffic_distribution_model")

import real_traffic_distribution_model as rtdm


# P(X=k) = λk * e– λ / k!
# λ: mean number of successes that occur during a specific interval
# k: number of successes
# e: a constant equal to approximately 2.71828

# generate random values from Poisson distribution with mean=3 and sample size=10


def generate_vehicles_distribution(options):
    routes_data_df = pd.read_csv(options.routes_data)
    dups = routes_data_df.pivot_table(index=['route_id'], aggfunc='size')
    dups.columns = ['veh_per_route']
    dups.to_csv("/home/josedaniel/prueba.csv", index=False)
    dist = poisson.rvs(mu=0.048889, size=900)
    print(dups)
    # print(sum(dist))
