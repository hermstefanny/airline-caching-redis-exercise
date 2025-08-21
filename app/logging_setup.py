import logging


def setup_config(name: str):
    logging.basicConfig(
        filename=name,
        format="%(asctime)s %(message)s",
        filemode="w",
        level=logging.INFO,
    )
