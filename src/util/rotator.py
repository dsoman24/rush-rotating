"""This module contains Rotator, the driver for the rush sheet script."""

import logging

from util.sheets import SheetEditor
from pymongo import MongoClient

logger = logging.getLogger(__name__)

class Rotator:
    """Driver class for the Rush rotating script."""

    def __init__(
            self,
            client: MongoClient,
            db_name: str,
            spreadsheet_id: str
        ):
        """
        Args:
            client (MongoClient): The MongoDB client.
            db_name (str): The name of the database to use.
        """
        self.client = client
        self.db = client[db_name]
        self.sheet_editor = SheetEditor(spreadsheet_id)

    def execute(self):
        logger.info("Updating the rotator data sheet.")
        # Create the data sheet if it doesn't exist
        self.sheet_editor.verify_or_create_data_sheet()
        # Ensure the data sheet is empty
        self.sheet_editor.clear_data_sheet()
        logger.info("Rotator data sheet successfully updated.")
