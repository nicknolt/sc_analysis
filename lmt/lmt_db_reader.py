import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pytz

from common_log import create_logger



@dataclass
class DBInfo:
    path: Path
    date_start: datetime
    date_end: datetime

    @property
    def duration(self) -> float:
        return (self.date_end - self.date_start).total_seconds() / 60 / 60 / 24


class LMTDBReader:

    def __init__(self, db_path: Path):
        self.logger = create_logger(self)

        self._date_start: datetime = None
        self._date_end: datetime = None
        
        self.db_path: Path = db_path

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

    def _fetch_date_begin_end(self):
        self.logger.info(f"Load sql file:'{self.db_path}'")

        connection = sqlite3.connect(self.db_path)
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

        # self.connexion = connection
        c.close()
        connection.close()

    def get_corresponding_frame_number(self, date: datetime) -> int:

        if not (self.date_start < date < self.date_end):
            raise ValueError(f"Date {date} is out of range [{self.date_start}, {self.date_end}]")

        connection = sqlite3.connect(self.db_path)

        # every 500 frames (1650 ms) LMT recording delay the next frame for 220 ms
        delta_t = (date - self.date_start).total_seconds()

        expected_frame = int(delta_t * 30)

        c = connection.cursor()
        search_offset = 10000

        # pd.Timestamp to timestamp give local tz date as a GMT date, it is a wrong behavior (tested with older pandas version always the same results ...)
        # convert timestamp to python datetime and convert it to timestamp give the expected value
        # from_date_ts = date.to_pydatetime().timestamp() * 1000
        from_date_ts = date.timestamp() * 1000

        if expected_frame < 0:
            raise Exception(f"Expected frame should be positive ({expected_frame})")
        #
        c.execute(
            f'SELECT framenumber, timestamp FROM frame WHERE framenumber BETWEEN {expected_frame - search_offset} AND {expected_frame} ORDER BY ABS(? - timestamp) ASC LIMIT 1',
            (from_date_ts,))

        row = c.fetchone()

        if row is None:
            self.logger.warning(f"Frame number not found for date {date} of timestamp {from_date_ts} expected frame {expected_frame}")
            return None

        return row[0]

