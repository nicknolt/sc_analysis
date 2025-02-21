import sqlite3
from datetime import datetime
from pathlib import Path

from common_log import create_logger


class LMTDBReader:

    def __init__(self, db_path: Path):
        self.logger = create_logger(self)

        self._date_start: datetime = None
        self._date_end: datetime = None
        
        self.db_path = db_path
    
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

        c.execute('SELECT timestamp FROM frame WHERE framenumber = (SELECT MIN(framenumber) FROM frame)')
        row = c.fetchone()
        self._date_start = datetime.fromtimestamp(row[0]/1000)

        c.execute('SELECT timestamp FROM frame WHERE framenumber = (SELECT MAX(framenumber) FROM frame)')
        row = c.fetchone()
        self._date_end = datetime.fromtimestamp(row[0]/1000)

        # self.connexion = connection
        c.close()
        connection.close()

    def get_corresponding_frame_number(self, date: datetime) -> int:

        if not (self.date_start < date < self.date_end):
            raise ValueError(f"Date {date} is out of range [{self.date_start}, {self.date_end}]")

        connection = sqlite3.connect(self.db_path)

        # tmp_db_name = tmp_db_name.split('\\')[-2:]
        # every 500 frames (1650 ms) LMT recording delay the next frame for 220 ms
        delta_t = (date - self.date_start).total_seconds()

        expected_frame = int(delta_t * 30)

        # # delay = (delta_t * 0.22) / 1.65
        # #
        # # expected_frame = np.round((delta_t - delay) * 30)
        # # connection = sqlite3.connect(dbi.filepath)
        # # await connection.set_trace_callback(print)

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

        # c.execute(
        #     f'SELECT framenumber, timestamp FROM frame WHERE framenumber BETWEEN {expected_frame - search_offset} AND {expected_frame + search_offset} ORDER BY ABS(? - timestamp) ASC LIMIT 1',
        #     (from_date_ts,))
        row = c.fetchone()

        if row is None:
            self.logger.warning(f"Frame number not found for date {date} of timestamp {from_date_ts} expected frame {expected_frame}")
            return None
        #
        delta_s = (row[1] - from_date_ts) / 1000.0
        if abs(delta_s) > 1:
            err_msg = f"Accurate frame has not been found for db:\n'{tmp_db_name}' date {date} of timestamp {from_date_ts} expected frame {expected_frame}. Closest is {delta_s} s after with frame {row[0]}"
            raise Exception(err_msg)

        return row[0]


    # async def _get_corresponding_frame_number_per_db_group(self, db_info: pd.Series, df_events: pd.DataFrame) -> pd.DataFrame:
    #     self.logger.debug(f"ASYNC deal with {db_info.filepath}")
    #
    #     connection = await aiosqlite.connect(db_info.filepath)
    #     # connection = sqlite3.connect(db_file)
    #     # connection.set_trace_callback(print)
    #     res = list()
    #
    #     for idx, row in df_events.iterrows():
    #
    #         if row.error != '':
    #             num_frame = -1
    #         else:
    #             num_frame = await self._get_corresponding_frame_number_per_event(start_date=db_info.date_start, connection=connection,
    #                                                           date=row.time, tmp_db_name=db_info.filepath)
    #
    #         short_db = db_info.filepath.split('\\')[-2:]
    #
    #         res.append(
    #             {
    #                 'event_idx': idx,
    #                 'frame_number': num_frame,
    #                 'db_file': str.join('\\', short_db),
    #             }
    #         )
    #
    #     return pd.DataFrame(res)