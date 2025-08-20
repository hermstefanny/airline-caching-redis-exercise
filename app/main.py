from utils import get_kaggle_data
import pandas as pd
import time


def calculate_average_delay(df, column_to_group, column_to_analyze):
    start_time = time.time()
    average_delay_per_parameter = df.groupby(column_to_group)[column_to_analyze].mean()
    print(f"Time taken to execute {time.time()-start_time}")
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

    avg_arrival_p_airline = calculate_average_delay(
        flights_df, "AIRLINE", "ARRIVAL_DELAY"
    )

    avg_departure_p_airline = calculate_average_delay(
        flights_df, "AIRLINE", "DEPARTURE_DELAY"
    )
