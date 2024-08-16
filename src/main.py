"""Driver for the Rush rotating script."""

import os
import certifi
import logging

from dotenv import dotenv_values
from pymongo import MongoClient
from util.rotator import Rotator

logger = logging.getLogger(__name__)

config = dotenv_values(os.path.join("src", ".env"))

def main():
    logging.basicConfig(
        filename="main.log",
        level=logging.INFO,
        filemode="w",
        format='[%(asctime)s] (%(levelname)s) %(name)s:%(lineno)d - %(message)s'
    )
    logger.info("Getting MongoDB Client.")
    with MongoClient(config["ATLAS_URI"], tlsCAFile=certifi.where()) as client:
        rotator = Rotator(
            client=client,
            db_name=config["DB_NAME"],
            spreadsheet_id=config["SHEET_ID"]
        )
        rotator.execute()

if __name__ == '__main__':
    main()
