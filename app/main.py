from utils import get_kaggle_data
import pandas as pd
import logging
from cache import Cache
from dotenv import load_dotenv
import os
from calculations import calculate_average_delay, calculation_cached


if __name__ == "__main__":

    # get_kaggle_data("usdot/flight-delays")

    logging.basicConfig(
        filename="cached_logs.log", format="%(asctime)s %(message)s", filemode="w"
    )

    logger = logging.getLogger()

    logger.setLevel(logging.INFO)

    print("---- Loading data ----")

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
    print("----  Data loaded  ----")
    logger.info(" Data loaded")
    # Functions without caching
    print("\n****** Results from normal functions ******")
    print(
        f"\nAverage Arrival Delay by airline:\n{calculate_average_delay(flights_df, 'AIRLINE', 'ARRIVAL_DELAY')}\n"
    )

    print(
        f"\nAverage Departure Delay by airline:\n{calculate_average_delay(flights_df, 'AIRLINE', 'DEPARTURE_DELAY')}\n"
    )

    # Functions with caching

    load_dotenv()

    cache = Cache()
    TTL = int(os.getenv("TTL_MIN", 60))

    print("\n\n****** Results from cached functions ****** ")

    print(
        f"\nAverage Arrival delay by airline:  \n{calculation_cached(cache, 15, 'ARRIVAL_delay_by_airline',  calculate_average_delay, flights_df, 'AIRLINE', 'ARRIVAL_DELAY', logger )}\n"
    )

    print(
        f"\nAverage Departure Delay by airline:  \n{calculation_cached(cache, 15, 'DEPARTURE_delay_by_airline',  calculate_average_delay, flights_df, 'AIRLINE', 'DEPARTURE_DELAY', logger)}\n"
    )
