from utils import get_kaggle_data
import pandas as pd
import time
from cache import Cache


def calculate_average_delay(df: pd, column_to_group: str, column_to_analyze: str):
    start_time = time.time()
    average_delay_per_parameter = df.groupby(column_to_group)[column_to_analyze].mean()
    print(f"Time taken to execute {time.time()-start_time}")
    return average_delay_per_parameter


def calculation_cached(
    cache: Cache, TTL: int, func_id: str, function_to_calc, *args, **kwargs
):

    start_time = time.time()
    key = f"measure_cached_{func_id}"
    cached_result = cache.get_data_from_cache(key)

    if cached_result is not None:
        print("Cache hit: Returning result from cache")
        print(f"Time taken to execute (cached) {time.time()-start_time}")

        return cached_result

    else:
        print("Cache miss: Data not found in cache. \nCalculating..")
        measure_per_parameter = function_to_calc(*args, **kwargs).to_dict()

        print("..Saving to cache..")
        cache.save_data_to_cache(key, measure_per_parameter, TTL)

        return measure_per_parameter


if __name__ == "__main__":
    # get_kaggle_data("usdot/flight-delays")

    airlines_df = pd.read_csv("data/airlines.csv")
    airports_df = pd.read_csv("data/airports.csv")
    flights_df = pd.read_csv(
        "data/flights.csv",
        dtype={
            "YEAR": "int16",
            "MONTH": "int8",
            "DAY": "int8",
            "DAY_OF_WEEK": "int8",
            "ORIGIN_AIRPORT": "str",
            "DESTINATION_AIRPORT": "str",
            "DEPARTURE_TIME": "float32",
            "DEPARTURE_DELAY": "float32",
            "ARRIVAL_TIME": "float32",
            "ARRIVAL_DELAY": "float32",
        },
    )

    # Functions without caching
    print("\n\n ****** Results from normal functions ******")
    print(
        f"Average Arrival Delay by airline:\n{calculate_average_delay(flights_df, 'AIRLINE', 'ARRIVAL_DELAY')}\n"
    )

    print(
        f"Average Departure Delay by airline:\n{calculate_average_delay(flights_df, 'AIRLINE', 'DEPARTURE_DELAY')}\n"
    )

    # Functions with caching
    cache = Cache()
    TTL_min = 15
    print("\n\n ****** Results from cached functions ****** ")

    print(
        f"Average Arrival delay by airline:  \n{calculation_cached(cache, 15, 'ARRIVAL_delay_by_airline', calculate_average_delay, flights_df, 'AIRLINE', 'ARRIVAL_DELAY' )}\n"
    )

    print(
        f"Average Departure Delay by airline:  \n{calculation_cached(cache, 15, 'DEPARTURE_delay_by_airline', calculate_average_delay, flights_df, 'AIRLINE', 'DEPARTURE_DELAY')}\n"
    )
