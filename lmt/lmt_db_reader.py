import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Tuple

import numpy as np
import pytz
import pandas as pd
from common_log import create_logger



@dataclass
class DBInfo:
    path: Path
    date_start: datetime
    date_end: datetime

    @property
    def duration(self) -> float:
        return (self.date_end - self.date_start).total_seconds() / 60 / 60 / 24

class LMTDBException(Exception):

    class ExceptionType(Enum):
        NO_DETECTION = 0
        TOO_FAR = 1
        # B = auto()

    def __init__(self, message: str, error_type: ExceptionType):
        super().__init__(message)
        self.error_type = error_type


class LMTDBReader:

    def __init__(self, db_path: Path):
        self.logger = create_logger(self)

        self._date_start: datetime = None
        self._date_end: datetime = None
        
        self.db_path: Path = db_path

        self._connexion: sqlite3.Connection = None

    @property
    def connexion(self) -> sqlite3.Connection:
        if self._connexion is None:
            self._connexion = sqlite3.connect(self.db_path)

        return self._connexion

    @property
    def db_info(self) -> 'DBInfo':
        return DBInfo(self.db_path, self.date_start, self.date_end)

    @property
    def date_start(self):
        if self._date_start is None:
            self._fetch_date_begin_end()
        return self._date_start

    @property
    def date_end(self):
        if self._date_end is None:
            self._fetch_date_begin_end()
        return self._date_end

    @property
    def duration(self) -> float:
        # in hours
        return (self.date_end - self.date_start).total_seconds()/60/60

    def close(self):

        if self._connexion is not None:
            self._connexion.close()

        self._connexion = None

    def is_date_inside(self, date: datetime) -> bool:
        return self.date_start <= date <= self.date_end

    def _fetch_date_begin_end(self):
        self.logger.info(f"Load sql file:'{self.db_path}'")

        connection = self.connexion
        # connection = sqlite3.connect(self.db_path)
        c = connection.cursor()

        tz_str = pytz.timezone("Europe/Paris")

        try:
            c.execute('SELECT timestamp FROM frame WHERE framenumber = (SELECT MIN(framenumber) FROM frame)')
            row = c.fetchone()
            self._date_start = datetime.fromtimestamp(row[0]/1000, tz=tz_str)
        except sqlite3.DatabaseError as e:
            err_msg = f"Unable to fetch date start from database: '{self.db_path}' cause: {e}"
            self.logger.error(err_msg)

        try:
            c.execute('SELECT timestamp FROM frame WHERE framenumber = (SELECT MAX(framenumber) FROM frame)')
            row = c.fetchone()
            self._date_end = datetime.fromtimestamp(row[0]/1000, tz=tz_str)
        except sqlite3.DatabaseError as e:
            err_msg = f"Unable to fetch date end from database: '{self.db_path}' cause: {e}"
            self.logger.error(err_msg)

        c.close()
        self.close()

    def get_closest_animal(self, frame_number: int, location: Tuple[int, int], close_connection: bool = True) -> str:

        connection = self.connexion
        c = connection.cursor()

        delta_frame = 5

        # ORDER BY FRAME DISTANCE OF THE REF FRAME
        request = f"""SELECT d.MASS_X, d.MASS_Y, a.RFID, d.FRAMENUMBER FROM
            DETECTION as d, ANIMAL as a 
        WHERE 
            d.FRAMENUMBER BETWEEN {frame_number - delta_frame} AND {frame_number + delta_frame}
            AND a.ID = d.ANIMALID
            ORDER BY ABS({frame_number}-d.FRAMENUMBER)"""

        c.execute(request)

        min_dist: float = None
        min_rfid: str = None

        df = pd.read_sql_query(request, connection)
        gb = df.groupby('FRAMENUMBER')

        # iterate each group ordered by time precision from the reference num frame
        for x, group in gb:
            for idx, row in group.iterrows():
                dist = math.dist((row.iloc[0], row.iloc[1]), location)

                if min_dist is None or dist < min_dist:
                    min_dist = dist
                    min_rfid = row.iloc[2]

            # stop if a min < 60 has been found
            if min_dist <= 60:
                break

        if min_dist is None:
            err_msg = f"No detection at frame {frame_number}"
            raise LMTDBException(err_msg, LMTDBException.ExceptionType.NO_DETECTION)

        if min_dist > 60:
            err_msg = f"Min dist is {min_dist:.2f} for rfid '{min_rfid}' but is too far to be considered as pertinent"
            raise LMTDBException(err_msg, LMTDBException.ExceptionType.TOO_FAR)

            # self.logger.warning(f"Date: {datetime.fromtimestamp(ts)} Min dist is {min_dist} for rfid '{min_rfid}' but is too far to be considered as pertinent")
            # return None

        if close_connection:
            connection.close()

        return min_rfid


    def get_corresponding_frame_number(self, date: datetime, close_connexion: bool = True) -> int:

        if not (self.date_start < date < self.date_end):
            raise ValueError(f"Date {date} is out of range [{self.date_start}, {self.date_end}]")

        connection = self.connexion
        # connection = sqlite3.connect(self.db_path)

        # every 500 frames (1650 ms) LMT recording delay the next frame for 220 ms
        delta_t = (date - self.date_start).total_seconds()

        expected_frame = int(delta_t * 30)

        c = connection.cursor()
        search_offset = 10000

        # pd.Timestamp to timestamp give local tz date as a GMT date, it is a wrong behavior (tested with older pandas version always the same results ...)
        # convert timestamp to python datetime and convert it to timestamp give the expected value
        # from_date_ts = date.to_pydatetime().timestamp() * 1000
        # date need to be convert in utc +0 to be compared to ts of LMT DB
        # and converted to tz unaware
        # date_tz_unaware = date.astimezone(tz=timezone.utc).replace(tzinfo=None)
        # from_date_ts = date.replace(tzinfo=None).timestamp() * 1000
        from_date_ts = date.timestamp() * 1000

        if expected_frame < 0:
            raise Exception(f"Expected frame should be positive ({expected_frame})")
        #
        c.execute(
            f'SELECT framenumber, timestamp FROM frame WHERE framenumber BETWEEN {expected_frame - search_offset} AND {expected_frame} ORDER BY ABS(? - timestamp) ASC LIMIT 1',
            (from_date_ts,))

        row = c.fetchone()
        c.close()

        if close_connexion:
            self.close()
        
        if row is None:
            self.logger.warning(f"Frame number not found for date {date} of timestamp {from_date_ts} expected frame {expected_frame}")
            return None

        # delta_t = abs(date-datetime.fromtimestamp(row[1]/1000, tz=pytz.timezone("Europe/Paris"))).total_seconds()

        return row[0]

