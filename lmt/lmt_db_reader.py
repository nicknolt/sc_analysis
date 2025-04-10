import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Tuple, List, Dict

import pandas as pd
import pandas.errors
import pytz

from common_log import create_logger


@dataclass
class DBInfo:
    path: Path
    date_start: datetime
    date_end: datetime
    nb_frames: int

    @property
    def duration(self) -> float:
        return (self.date_end - self.date_start).total_seconds() / 60 / 60 / 24

    @property
    def setup_id(self) -> str:
        return self.path.parent.name

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
        self._nb_frames: int = 0

        self.db_path: Path = db_path

        self._connexion: sqlite3.Connection = None

    @property
    def setup_id(self) -> str:
        return self.db_path.parent.name

    @property
    def connexion(self) -> sqlite3.Connection:
        if self._connexion is None:
            self._connexion = sqlite3.connect(self.db_path)

        return self._connexion

    @property
    def db_info(self) -> 'DBInfo':
        return DBInfo(self.db_path, self.date_start, self.date_end, self.nb_frames)

    @property
    def nb_frames(self) -> int:
        if self._nb_frames is None:
            self._fetch_date_begin_end()
        return self._nb_frames

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

        tz_str = pytz.timezone("Europe/Paris")

        query = """
                SELECT timestamp, framenumber 
                FROM frame 
                WHERE framenumber IN (SELECT MIN(framenumber) FROM frame UNION SELECT MAX(framenumber) FROM frame)"""

        try:
            # c.execute('SELECT timestamp FROM frame WHERE framenumber = (SELECT MIN(framenumber) FROM frame)')
            df = pd.read_sql_query(query, connection)
            row_start = df.iloc[0]
            row_end = df.iloc[1]

            self._date_start = datetime.fromtimestamp(row_start.TIMESTAMP/1000, tz=tz_str)
            self._date_end = datetime.fromtimestamp(row_end.TIMESTAMP/1000, tz=tz_str)
            self._nb_frames = row_end.FRAMENUMBER + 1
        except (sqlite3.DatabaseError, pandas.errors.DatabaseError) as e:
            err_msg = f"Unable to fetch informations from database: '{self.db_path}' cause: {e}"
            self.logger.error(err_msg)
            # raise e

        self.close()

    def get_closest_animal(self, frame_numbers: List[int], location: Tuple[int, int]) -> pd.DataFrame:

        # due to first record without ms expected frame could be 30 frames later
        # we are looking into this interval and stop at the first group of frame corresponding to distance criteria
        delta_frame = 20

        query_values = [str((cpt, num_frame)) for cpt, num_frame in enumerate(frame_numbers)]
        query_val_str = ','.join(query_values)

        query = f"""
                    WITH T(id, num_frame) AS (
        			VALUES {query_val_str})
        			SELECT
                        T.*, d.MASS_X, d.MASS_Y,
                        a.RFID as lmt_rfid,
                        f.FRAMENUMBER as lmt_db_frame, f.TIMESTAMP as lmt_date                        
                    FROM
                        FRAME as f, T
                    LEFT JOIN DETECTION as d ON d.FRAMENUMBER = f.FRAMENUMBER
                    LEFT JOIN ANIMAL as a ON a.ID = d.ANIMALID
        			WHERE
                            f.FRAMENUMBER BETWEEN T.num_frame AND (T.num_frame+{delta_frame})
                """

        # self.logger.debug(f"QUERY = {query}")
        self.logger.debug("Query start")

        df = pd.read_sql_query(query, self.connexion, dtype={'lmt_rfid': str})

        self.logger.debug("Query end")

        df['db_error'] = ''

        self.connexion.close()

        columns = ['db_error', 'lmt_rfid', 'lmt_db_frame', 'lmt_date']

        def get_closest_frame(rows: pd.DataFrame) -> pd.Series:

            res_row = pd.Series(dict.fromkeys(columns)).transpose()

            rows = rows[rows["lmt_rfid"] != "None"]

            if rows.empty:
                res_row["db_error"] = "NO DETECTION"
                return res_row

            rows["distance"] = rows.apply(lambda row: math.dist((row.MASS_X, row.MASS_Y), location), axis=1)
            rows = rows[rows.distance <= 60]

            if rows.empty:
                res_row["db_error"] = "TOO_FAR"
                return res_row

            res_row = rows.iloc[0]

            return res_row[columns]

        res = df.groupby('id').apply(get_closest_frame)

        tz_str = pytz.timezone("Europe/Paris")
        res['lmt_date'] = pd.to_datetime(res['lmt_date'], unit="ms", utc=True).dt.tz_convert(tz_str)

        return res

    def get_trajectories(self, date_list: List[datetime], duration_s: int, rfid: str):

        frame_values = dict()
        for id, date in enumerate(date_list):
            start_frame = self.get_corresponding_frame_number(date_list=[date])[0]
            end_frame = self.get_corresponding_frame_number(date_list=[date + timedelta(seconds=duration_s)])[0]
            frame_values[id] = {
                             "start_frame": start_frame,
                             "end_frame": end_frame
            }

            print(f"START => {int(start_frame)}-> {int(end_frame)}")

        query_values = [str((k, int(v["start_frame"]), int(v["end_frame"]))) for k, v in frame_values.items()]
        query_val_str = ','.join(query_values)


        query = f"""
            WITH T(id, frame_start, frame_end) AS (
			VALUES {query_val_str})
			SELECT 
                T.*, d.MASS_X as X, d.MASS_Y as Y
            FROM 
                DETECTION as d, T, ANIMAL as a
			WHERE 
			    d.ANIMALID = a.ID AND a.RFID = '{rfid}'
				AND d.FRAMENUMBER BETWEEN T.frame_start AND T.frame_end 
        """

        self.logger.debug(f"QUERY = {query}")

        df = pd.read_sql_query(query, self.connexion)

        self.connexion.close()

        return df


    def _get_corresponding_frame_number(self, from_ref_frame: Tuple[int, datetime], date: datetime, search_offset: int = 1000) -> Tuple[int, datetime]:

        delta_t = (date - from_ref_frame[1]).total_seconds()
        expected_frame = int(delta_t * 30) + from_ref_frame[0]

        connection = self.connexion

        date_ts = date.timestamp()*1000

        low_index = max(1, expected_frame - search_offset)
        high_index = min(self.nb_frames+1, expected_frame + search_offset)

        query = f"""
                SELECT 
                    framenumber, timestamp, ABS({date_ts} - timestamp) as dt FROM frame 
                WHERE 
                    framenumber BETWEEN {low_index} AND {high_index} ORDER BY dt ASC 
                """

        df = pd.read_sql_query(query, connection)

        # best results
        row = df.iloc[0]

        # check if there are frame before and after
        before = df[df.TIMESTAMP < row.TIMESTAMP]
        after = df[df.TIMESTAMP > row.TIMESTAMP]

        # test if best result is surrounded by the previous and the next frame
        if not(before.empty or after.empty):
            return row["FRAMENUMBER"], datetime.fromtimestamp(row.TIMESTAMP/1000).astimezone(tz=pytz.timezone("Europe/Paris"))

        else:
            err_msg = \
                f"""Research interval around {expected_frame} [{low_index} -> {high_index}] don't contain the best result delta_t = {delta_t} in db: {self.db_path}
                query = {query}
                """
            self.logger.debug(err_msg)

            return None, None


    def get_corresponding_frame_number(self, date_list: List[datetime]) -> pd.DataFrame:

        frame_number = 1
        frame_date = self.date_start

        res: List[Dict] = list()

        nb_frames = len(date_list)

        for cpt, date in enumerate(date_list):

            if cpt % 100 == 0:
                self.logger.debug(f"Frame {cpt}/{nb_frames}")

            search_offset = 100

            while True:
                res_number, res_date = self._get_corresponding_frame_number(from_ref_frame=(frame_number, frame_date), date=date, search_offset=search_offset)
                if res_number is None:
                    search_offset *= 10
                else:
                    frame_number = res_number
                    frame_date = res_date
                    break

            delta_t = (date - frame_date).total_seconds()

            entry = {
                'db_frame': frame_number,
                'db_delta': delta_t
            }

            res.append(entry)

            # res.append((frame_number, delta_t))

        self.close()

        return pd.DataFrame(res)


