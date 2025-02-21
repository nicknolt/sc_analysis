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

