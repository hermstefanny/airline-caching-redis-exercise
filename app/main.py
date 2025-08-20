from utils import get_kaggle_data
import pandas as pd
import time
from cache import Cache


def calculate_average_delay(df, column_to_group, column_to_analyze):
    start_time = time.time()
    average_delay_per_parameter = df.groupby(column_to_group)[column_to_analyze].mean()
    print(f"Time taken to execute {time.time()-start_time}")
    return average_delay_per_parameter


def avg_arrival_p_airline_cached(cache: Cache, df, column_to_group, column_to_analyze):
    start_time = time.time()
    arrv_key = f"avg_arrival_p_airline_cached:{column_to_group}:{column_to_analyze}"
    cached_result = cache.get_data_from_cache(arrv_key)

    if cached_result is not None:
        print(f"Time taken to execute (cached) {time.time()-start_time}")
        print("Result from cache")
        return cached_result

    average_delay_per_parameter = (
        df.groupby(column_to_group)[column_to_analyze].mean().to_dict()
    )
    print(f"Time taken to execute (non-cached) {time.time()-start_time}")
    cache.save_data_to_cache(arrv_key, average_delay_per_parameter)

    return average_delay_per_parameter


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
    print(calculate_average_delay(flights_df, "AIRLINE", "ARRIVAL_DELAY"))

    # avg_departure_p_airline = calculate_average_delay(
    #     flights_df, "AIRLINE", "DEPARTURE_DELAY"
    # )

    # Functions with caching
    cache = Cache()

    print(avg_arrival_p_airline_cached(cache, flights_df, "AIRLINE", "ARRIVAL_DELAY"))
