import kaggle
import pandas as pd
from time import sleep
import random


def get_kaggle_data(data_path) -> None:
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files(data_path, path="./data", unzip=True)
    print("Data imported")
