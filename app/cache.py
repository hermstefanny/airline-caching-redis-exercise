import os
import sys
import redis
import json
from typing import Dict, Any
from datetime import timedelta


class Cache:
    def __init__(
        self, redis_host: str = "localhost", redis_port: int = 6379, redis_db: int = 0
    ):
        """Establish connection with Redis."""
        try:
            client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
            )
            # Verify that Redis is responding
            ping = client.ping()
            if ping is True:
                self.client = client
            else:
                print("Connection established but Redis not responding to ping!")
        # Log error and stop the process if not responding
        except redis.ConnectionError as ex:
            print("Redis Connection Error: ", ex)
            # Stop code execution
            sys.exit(1)

    def save_data_to_cache(
        self, key: str, value: Dict[Any, Any], exp_in_minutes: int
    ) -> bool:
        """
        Save data to redis.

        :param key: the key used to store the data in Redis.
        :param value: the value associated to the key to save in Redis.
        :param exp_in_hours: The number of hours before expiration of the cached data in Redis.
        :return state: True if the data is well saved in Redis, False if not.
        """
        # Convert the dict to string to be able to store it on Redis
        str_value = json.dumps(value)

        # Store data in cache with the defined key
        state = self.client.setex(
            key,
            timedelta(minutes=exp_in_minutes),
            value=str_value,
        )

        return state

    def get_data_from_cache(self, key: str) -> Dict[Any, Any] | None:
        """
        Get data from redis.

        :param key: the key used to store the data in Redis.
        :return data: A dictionary with the data cached if found, None if nothing found with the key.
        """
        # Try to get the data from cache
        data = self.client.get(key)

        # Return None if the key doesn't exist (so the data is not in cache yet)
        if data is None:
            return None

        # Decode data to utf8
        data = data.decode("UTF-8")
        # Load data from string to dict
        data_dict = json.loads(data)

        # Return the cached data
        return data_dict
