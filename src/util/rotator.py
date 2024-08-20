"""This module contains Rotator, the driver for the rush sheet script."""

import datetime
import logging

from pprint import pprint

from bson import ObjectId
from util.sheets import SheetEditor
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone, date

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
        self.num_of_days_since_bid = data_dict["num_of_days_since_bid"]
        self.days_rushed = data_dict["days_rushed"]
        self.release = self.num_of_days_since_bid >= 2 or (self.num_of_days_since_bid == "No Pros" and self.days_rushed >=2)

    def get_row_list(self):
        """Returns the PNM data as a list of strings for the data sheet."""
        est_offset = timedelta(hours=-5)
        est_check_in_time = datetime.combine(date.today(), self.check_in_time) + est_offset
        est_check_out_time = None
        if self.check_out_time:
            est_check_out_time = datetime.combine(date.today(), self.check_out_time) + est_offset

        return [
            self.name,
            est_check_in_time.strftime("%I:%M %p"),
            est_check_out_time.strftime("%I:%M %p") if est_check_out_time else "",
            str(self.avg_fit_rating),
            str(self.num_reds),
            str(self.num_greens),
            str(self.num_pro),
            str(self.num_con),
            ", ".join(self.brother_recs),
            ", ".join(self.interests),
            str(self.num_of_days_since_bid),
            str(self.days_rushed),
            str(self.release),
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
        logger.info("Aggregating PNM Data")
        contactIds = self._get_contact_ids_by_todays_attendance()
        pnms = self._aggregate_pnm_data(contactIds)
        rows = self._create_pnm_rows(pnms)
        self._sheet_editor.verify_or_create_data_sheet()
        logger.info("Aggregated Info Successfully")
        # TODO: clear sheet only on a new day
        logger.info("Updating the rotator data sheet.")
        self._sheet_editor.clear_data_sheet()
        self._sheet_editor.write_header()
        self._sheet_editor.write_data_rows(rows)
        logger.info("Rotator data sheet successfully updated.")

    def _get_contact_ids_by_todays_attendance(self):
        logger.info("Getting Contact Ids from Database")
        attendance_collection = self._db["attendances"]
        # For EST (Eastern Standard Time)
        #utc_offset = timedelta(hours=-5)

        # For EDT (daylight saving time)
        utc_offset = timedelta(hours=-4)
        
        est = timezone(utc_offset)
        
        today_est = datetime.now(tz=est)
        start_of_day_est = today_est.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day_est = today_est.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        start_of_day_utc = start_of_day_est.astimezone(timezone.utc)
        end_of_day_utc = end_of_day_est.astimezone(timezone.utc)
        
        
        query = {
            'checkInDate': {
                '$gte': start_of_day_utc,
                '$lt': end_of_day_utc
            }
        }
        contactIds = [ObjectId(id) for id in attendance_collection.find(query).distinct("contactId")]
        logger.info("Retrieved Contact Ids from Database")
        return contactIds


    def _aggregate_pnm_data(self, contactIds):
        """Creates the PNM data for the data sheet based for all contacts."""
        logger.info("Getting PNM Data from Database")
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
        logger.info("Retrieved PNM Data from Database")
        return pnm_data

    def _create_pnm_rows(self, pnm_data):
        logger.info("Creating PNM rows")
        rows = [pnm.get_row_list() for pnm in pnm_data]
        logger.info("Successfully created PNM rows")
        return rows

    def _aggregate_survey_results(self, contact):
        """Aggregates the survey results for one contact.

        Args:
            contact (dict): The contact to aggregate survey data for.

        Returns:
            dict: A dictionary mapping each aggregate to its value.
        """
        logger.info(
            f"Aggregating survey results for contact '{contact['_id']}'."
        )
        aggregates = {
            "mean_fit_rating": float("nan"),
            "num_reds": 0,
            "num_greens": 0,
            "num_pro": 0,
            "num_con": 0,
            "num_of_days_since_bid": 0,
            "days_rushed": 0,
        }
        # Get the contact's surveys
        survey_ids = contact["surveyInfo"]
        survey_collection = self._db["surveys"]
        num_surveys = 0
        fit_rating_sum = 0
        most_recent_pro = datetime.min
        # Brother recommendations are case-insensitive and stored in a set to
        # avoid duplicates as best as possible.
        brother_recs = set()
        interests = set()
        query = {
            '_id': {
                '$in': survey_ids
            }
        }
        for survey in survey_collection.find(query):
            fit_rating_sum += float(survey["fitRating"].to_decimal()) if survey["fitRating"] != "NoneType" else 0
            if survey["bidStatus"] == _GREEN:
                aggregates["num_greens"] += 1
            elif survey["bidStatus"] == _RED:
                aggregates["num_reds"] += 1
            elif survey["bidStatus"] == _PRO:
                aggregates["num_pro"] += 1
                if (most_recent_pro < survey["createdAt"]):
                    most_recent_pro = survey["createdAt"]
            elif survey["bidStatus"] == _CON:
                aggregates["num_con"] += 1
            brother_recs.update(
                [name.lower() for name in survey["brotherRecs"]]
            )
            interests.update(
                [interest.lower() for interest in survey["interestTags"]]
            )
            num_surveys += 1
            
        num_of_days_since_bid = self._get_num_of_attendances_from_date(most_recent_pro, str(contact["_id"])) if aggregates["num_pro"] > 0 else "No Pros"
        # Convert the sets to comma-separated strings
        aggregates["brother_recs"] = brother_recs
        aggregates["interests"] = interests
        aggregates["num_of_days_since_bid"] = num_of_days_since_bid

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
        logger.info(f"Getting attendance info for contact {contact['_id']}.")
        attendance_info = {
            "check_in_time": None,
            "check_out_time": None,
            "days_rushed": 0,
        }
        most_recent_attendance, attendance_info["days_rushed"] = self._get_most_recent_attendance(contact)
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
        latest_check_in = datetime.min
        latest_attendance = None
        query = {
            '_id': {
                '$in': attendance_ids
            }
        }
        attendances = attendance_collection.find(query)
        print(attendances)
        days_rushed = len(list(attendances))
        for attendance in attendances:
            check_in_datetime = attendance["checkInDate"]
            if check_in_datetime > latest_check_in:
                latest_check_in = check_in_datetime
                latest_attendance = attendance
        return latest_attendance, days_rushed
        
    def _get_num_of_attendances_from_date(self, start, contact_id):
        attendance_collection = self._db["attendances"]
        attendances = attendance_collection.find({ "contactId":contact_id })
        count = 0
        for attendance in attendances:
            check_in_datetime = attendance["checkInDate"]
            if check_in_datetime > start:
                count += 1
        return count
