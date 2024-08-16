"""Module to interact with the Google Sheets API."""

import os
import logging

from google.oauth2 import service_account
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

_KEYS_DIR = "src/keys"
# Name of the sheet that contains PNM data for rotating.
_DATA_SHEET_NAME = 'data'

class SheetEditor:
    """Class to interact with one Google Sheets spreadsheet via Sheets API.

    https://developers.google.com/sheets/api/guides/concepts?ref=hackernoon.com
    """

    def __init__(
            self, spreadsheet_id: str,
            data_sheet_name: str = _DATA_SHEET_NAME
        ):
        """Initialize the SheetEditor class.

        Args:
            spreadsheet_id (str): The ID of the Google Sheet.
            data_sheet_name (str): The name of the sheet to use for data.
        """
        self.spreadsheet_id = spreadsheet_id
        self.data_sheet_name = data_sheet_name
        credentials = self._get_credentials()
        self.service = build("sheets", "v4", credentials=credentials)

    def _get_credentials(self) -> service_account.Credentials:
        """Gets the service account credentials.

        Finds the first key file in the 'src/keys' directory if it exists.

        Returns:
            service_account.Credentials: The service account credentials.

        Raises:
            FileNotFoundError: If the 'keys' directory is not found or if no key
                files are found.
        """
        if not os.path.exists(_KEYS_DIR):
            error_msg = f"{_KEYS_DIR} directory not found."
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        key_files = os.listdir(_KEYS_DIR)
        if not key_files:
            error_msg = f"No key files found in {_KEYS_DIR} directory."
            raise FileNotFoundError(error_msg)
        # Choose first credential file in 'keys' directory
        logger.info(f"Credentials created using key file: {key_files[0]}")
        return service_account.Credentials.from_service_account_file(
            os.path.join(_KEYS_DIR, key_files[0]),
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )

    def _sheet_exists(self, sheet_name: str) -> bool:
        """Checks if a sheet exists in the spreadsheet.

        Args:
            sheet_name (str): The name of the sheet to check.

        Returns:
            bool: True if the sheet exists, False otherwise.
        """
        logger.info(f"Checking if sheet '{sheet_name}' exists.")
        sheet_metadata = self.service.spreadsheets().get(
            spreadsheetId=self.spreadsheet_id
        ).execute()
        sheets = sheet_metadata.get("sheets", "")
        for sheet in sheets:
            if sheet["properties"]["title"] == sheet_name:
                return True
        return False

    def _create_sheet(self, sheet_name: str) -> None:
        """Creates a new sheet in the spreadsheet.

        Args:
            sheet_name (str): The name of the sheet to create.
        """
        logger.info(f"Creating sheet '{sheet_name}'.")
        request = {
            "requests": [
                {
                    "addSheet": {
                        "properties": {
                            "title": sheet_name
                        }
                    }
                }
            ]
        }
        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body=request
        ).execute()

    def _get_sheet_id(self, sheet_name: str) -> int:
        """Gets the sheet ID using the sheet name.

        Args:
            sheet_name (str): The name of the sheet.

        Returns:
            int: The ID of the sheet.
        """
        sheets = self.service.spreadsheets().get(
            spreadsheetId=self.spreadsheet_id
        ).execute().get('sheets', [])
        for sheet in sheets:
            if sheet["properties"]["title"] == sheet_name:
                return sheet["properties"]["sheetId"]
        raise ValueError(f"Sheet with name {sheet_name} not found.")

    def _clear_sheet(self, sheet_name: str) -> None:
        """Clears the sheet of all data.

        Args:
            sheet_name (str): The name of the sheet to clear.
        """
        sheet_id = self._get_sheet_id(sheet_name)
        request = {
            "requests": [
                {
                    "updateCells": {
                        "range": {
                            "sheetId": sheet_id
                        },
                        "fields": "*"
                    }
                }
            ]
        }
        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body=request
        ).execute()

    def verify_or_create_data_sheet(self):
        """Verifies that the data sheet exists, creating it if necessary."""
        if not self._sheet_exists(self.data_sheet_name):
            logger.info(f"Creating {self.data_sheet_name} sheet.")
            self._create_sheet(self.data_sheet_name)
        else:
            logger.info(f"{self.data_sheet_name} sheet already exists.")

    def clear_data_sheet(self):
        """Clears the data sheet of all data."""
        self._clear_sheet(self.data_sheet_name)
