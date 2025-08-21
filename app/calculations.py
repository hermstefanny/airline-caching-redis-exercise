import pandas as pd
import logging
import time
from cache import Cache


def calculate_average_delay(df: pd, column_to_group: str, column_to_analyze: str):
    start_time = time.time()
    average_delay_per_parameter = df.groupby(column_to_group)[column_to_analyze].mean()
    logging.info(f"Time taken to execute (non-cached) {time.time()-start_time}")
    return average_delay_per_parameter


def calculation_cached(
    cache: Cache,
    TTL: int,
    func_id: str,
    function_to_calc,
    *args,
    **kwargs,
):

    start_time = time.time()
    key = f"measure_cached_{func_id}"
    cached_result = cache.get_data_from_cache(key)

    if cached_result is not None:
        print("Returning result from cache")
        logging.info("Cache Hit")
        logging.info(f"Time taken to execute (cached) {time.time()-start_time}")

        return cached_result

    else:
        print("Data not found in cache. \nCalculating..")
        logging.info("Cache miss")
        measure_per_parameter = function_to_calc(*args, **kwargs).to_dict()

        print("..Saving to cache..")
        cache.save_data_to_cache(key, measure_per_parameter, TTL)

        return measure_per_parameter
