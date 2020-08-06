import csv
import logging
import os
import requests
import time
from datetime import datetime

from keboola import docker
import logging_gelf.formatters
import logging_gelf.handlers


def generate_pages(base_url: str, client_id: str, sleep_time: float = 1):
    """Yield pages with product info until the last page is reached."""
    total_pages = 2  # initialize to an arbitrary value > 1
    page_num = 1
    while page_num <= total_pages:
        time.sleep(sleep_time)
        parameters = {"client_id": client_id, "filter": "basic", "page": page_num}
        response = requests.get(base_url, params=parameters).json()
        logging.info(f"Page {page_num} of {total_pages} downloaded.")
        total_pages = response["paging"]["pages"]
        page_num += 1
        yield response["data"]


def main():
    logging.basicConfig(
        level=logging.DEBUG, handlers=[]
    )  # do not create default stdout handler
    logger = logging.getLogger()
    try:
        logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(
            host=os.getenv("KBC_LOGGER_ADDR"), port=int(os.getenv("KBC_LOGGER_PORT"))
        )
    except TypeError:
        logging_gelf_handler = logging.StreamHandler()

    logging_gelf_handler.setFormatter(
        logging_gelf.formatters.GELFFormatter(null_character=True)
    )
    logger.addHandler(logging_gelf_handler)
    logger.setLevel(logging.INFO)

    datadir = os.getenv("KBC_DATADIR", "/data/")
    path = f'{os.getenv("KBC_DATADIR")}out/tables/results.csv'
    conf = docker.Config(datadir)
    params = conf.get_parameters()
    column_names = params["column_names"]
    api_url = params["api_url"]
    shops = params["shops"]
    interbatch_sleep_seconds = params["interbatch_sleep_seconds"]

    utc_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    with open(path, "w") as outfile:
        dict_writer = csv.DictWriter(
            outfile, fieldnames=column_names, extrasaction="ignore"
        )
        dict_writer.writeheader()
        for shop in shops:
            logger.info(f"Processing vendor_id {shop['vendor_id']}")
            for batch in generate_pages(
                api_url, shop["#client_id"], sleep_time=interbatch_sleep_seconds
            ):
                # write batch by lines to be able to add columns
                for row in batch:
                    row_amended = {
                        **row,
                        "utc_timestamp": utc_timestamp,
                        "vendor_id": shop["vendor_id"],
                        "country": shop["country"],
                    }
                    dict_writer.writerow(row_amended)


if __name__ == "__main__":
    main()
