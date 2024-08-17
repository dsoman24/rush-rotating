"""This module contains Rotator, the driver for the rush sheet script."""

import datetime
import logging

from pprint import pprint

from bson import ObjectId
from util.sheets import SheetEditor
from pymongo import MongoClient

logger = logging.getLogger(__name__)

# Bid status constants
_GREEN = "Green"
_RED = "Red"
_PRO = "Pro/Put Up"
_CON = "Con"

class PNMData:

    def __init__(self, data_dict):
        self.name = data_dict["name"]
        self.check_in_time = data_dict["check_in_time"]
        self.check_out_time = data_dict["check_out_time"]
        self.avg_fit_rating = data_dict["mean_fit_rating"]
        self.num_reds = data_dict["num_reds"]
        self.num_greens = data_dict["num_greens"]
        self.num_pro = data_dict["num_pro"]
        self.num_con = data_dict["num_con"]
        self.brother_recs = data_dict["brother_recs"]
        self.interests = data_dict["interests"]

    def get_row_list(self):
        """Returns the PNM data as a list of strings for the data sheet."""
        return [
            self.name,
            self.check_in_time.strftime("%I:%M %p"),
            self.check_out_time.strftime("%I:%M %p") if self.check_out_time else "",
            str(self.avg_fit_rating),
            str(self.num_reds),
            str(self.num_greens),
            str(self.num_pro),
            str(self.num_con),
            ", ".join(self.brother_recs),
            ", ".join(self.interests)
        ]

class Rotator:
    """Driver class for the Rush rotating script.

    Implements logic to get checked-in contacts from the database and update
    the PNM data sheet with the relevant information.
    """

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
        self._client = client
        self._db = self._client[db_name]
        self._sheet_editor = SheetEditor(spreadsheet_id)

    def execute(self):
        """Executes the rotator script, updating the PNM data sheet."""
        # Row: PNM name, check in time, check out time, avg fit rating,
        # num reds, num greens, num pro, num con, brother recs (comma separated
        # string of names), interests (comma separated string of interests)
        logger.info("Updating the rotator data sheet.")
        contactIds = self._get_contact_ids_by_todays_attendance()
        pnms = self._aggregate_pnm_data(contactIds)
        rows = self._create_pnm_rows(pnms)
        self._sheet_editor.verify_or_create_data_sheet()
        # TODO: clear sheet only on a new day
        self._sheet_editor.clear_data_sheet()
        self._sheet_editor.write_header()
        self._sheet_editor.write_data_rows(rows)
        logger.info("Rotator data sheet successfully updated.")

    def _get_contact_ids_by_todays_attendance(self):
        attendance_collection = self._db["attendances"]
        today = datetime.datetime.now(tz=datetime.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        query = {
            'checkInDate': {
                '$gte': today,
                '$lt': today.replace(hour=23, minute=59, second=59, microsecond=999999)
            }
        }
        return [ObjectId(id) for id in attendance_collection.find(query).distinct("contactId")]


    def _aggregate_pnm_data(self, contactIds):
        """Creates the PNM data for the data sheet based for all contacts."""
        pnm_data = []
        # Get the collection named "contacts"
        contacts_collection = self._db["contacts"]
        query = {
            '_id': {
                '$in': contactIds
            }
        }
        for contact in contacts_collection.find(query):
            data_dict = {"name": contact["name"]}
            attendance_info = self._get_attendance_info(contact)
            # Exclude the contact if they did not check in today.
            if not attendance_info["include_contact"]:
                continue
            data_dict.update(attendance_info["attendance_info"])
            data_dict.update(self._aggregate_survey_results(contact))
            pnm = PNMData(data_dict)
            pnm_data.append(pnm)
        logger.info(f"Found {len(pnm_data)} contacts checked in today."
                    " Their data has been successfully aggregated.")
        # Sort the rows in ascending order by check-in time
        logger.info("Sorting contacts by check-in time.")
        pnm_data.sort(key=lambda pnm: pnm.check_in_time)
        return pnm_data

    def _create_pnm_rows(self, pnm_data):
        return [pnm.get_row_list() for pnm in pnm_data]

    def _aggregate_survey_results(self, contact):
        """Aggregates the survey results for one contact.

        Args:
            contact (dict): The contact to aggregate survey data for.

        Returns:
            dict: A dictionary mapping each aggregate to its value.
        """
        logger.info(
            f"Aggregating survey results for contact '{contact["_id"]}'."
        )
        aggregates = {
            "mean_fit_rating": float("nan"),
            "num_reds": 0,
            "num_greens": 0,
            "num_pro": 0,
            "num_con": 0
        }
        # Get the contact's surveys
        survey_ids = contact["surveyInfo"]
        survey_collection = self._db["surveys"]
        num_surveys = 0
        fit_rating_sum = 0
        # Brother recommendations are case-insensitive and stored in a set to
        # avoid duplicates as best as possible.
        brother_recs = set()
        interests = set()
        for survey_id in survey_ids:
            survey = survey_collection.find_one({"_id": survey_id})
            fit_rating_sum += float(survey["fitRating"].to_decimal())
            if survey["bidStatus"] == _GREEN:
                aggregates["num_greens"] += 1
            elif survey["bidStatus"] == _RED:
                aggregates["num_reds"] += 1
            elif survey["bidStatus"] == _PRO:
                aggregates["num_pro"] += 1
            elif survey["bidStatus"] == _CON:
                aggregates["num_con"] += 1
            brother_recs.update(
                [name.lower() for name in survey["brotherRecs"]]
            )
            interests.update(
                [interest.lower() for interest in survey["interestTags"]]
            )
            num_surveys += 1

        # Convert the sets to comma-separated strings
        aggregates["brother_recs"] = brother_recs
        aggregates["interests"] = interests

        if num_surveys > 0:
            aggregates["mean_fit_rating"] = fit_rating_sum / num_surveys

        return aggregates

    def _get_attendance_info(self, contact):
        """Gets the check-in and check out date for a contact.

        Args:
            contact (dict): The contact to get attendance data for.

        Returns:
            dict: A pair containing a boolean representing if this checked in today,
            and a dictionary containing the check-in and check-out
            dates for the contact.
        """
        logger.info(f"Getting attendance info for contact {contact["_id"]}.")
        attendance_info = {
            "check_in_time": None,
            "check_out_time": None
        }
        most_recent_attendance = self._get_most_recent_attendance(contact)
        if not most_recent_attendance or not self._is_today(most_recent_attendance):
            logger.info("Contact did not check in today.")
            return {"include_contact": False, "attendance_info": attendance_info}
        attendance_info["check_in_time"] = most_recent_attendance["checkInDate"].time()
        if most_recent_attendance.get("checkOutDate"):
            attendance_info["check_out_time"] = most_recent_attendance["checkOutDate"].time()
        else:
            attendance_info["check_out_time"] = None
        logger.info("Contact is checked in today.")
        return {"include_contact": True, "attendance_info": attendance_info}

    def _get_most_recent_attendance(self, contact):
        """Gets the most recent attendance data for each contact.

        Args:
            contact (dict): The contact to get attendance data for.

        Returns:
            The most recent attendance data for the contact, or None if the
            contact has no attendance data.
        """
        attendance_ids = contact["attendance"]
        if not attendance_ids:
            return None
        attendance_collection = self._db["attendances"]
        latest_check_in = datetime.datetime.min
        latest_attendance = None
        for attendance_id in attendance_ids:
            attendance = attendance_collection.find_one({"_id": attendance_id})
            check_in_datetime = attendance["checkInDate"]
            if check_in_datetime > latest_check_in:
                latest_check_in = check_in_datetime
                latest_attendance = attendance
        return latest_attendance

    def _is_today(self, attendance):
        """Checks if this attendance data is for today.

        Args:
            attendance (dict): The attendance data to check.

        Returns:
            bool: True if the contact is here today, False otherwise.
        """
        return attendance["checkInDate"].date() == datetime.date.today()
