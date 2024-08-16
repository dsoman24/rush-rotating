"""Driver for the Rush rotating script."""

import certifi
import logging

from dotenv import dotenv_values
from pymongo import MongoClient
from util.rotator import Rotator

logger = logging.getLogger(__name__)

config = dotenv_values(".env")

def main():
    logging.basicConfig(
        filename="main.log",
        level=logging.INFO,
        filemode="w",
    )
    logger.info("Getting MongoDB Client.")
    client = MongoClient(config["ATLAS_URI"], tlsCAFile=certifi.where())
    rotator = Rotator(
        client=client,
        db_name=config["DB_NAME"],
        spreadsheet_id=config["SHEET_ID"]
    )
    rotator.execute()
    client.close()

if __name__ == '__main__':
    main()
