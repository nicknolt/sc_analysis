import os
from dataclasses import dataclass
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import List

import pandas as pd
import pytz
from pandas import DataFrame
from pytz.exceptions import AmbiguousTimeError

from common import FileMerger
from common_log import create_logger


@dataclass
class BatchInfo:
    setup_id: str
    name: str
    date_start: datetime
    date_end: datetime

    @property
    def duration(self) -> float:
        return (self.date_end - self.date_start).total_seconds() / 60 / 60 / 24


class DataService:

    def __init__(self, data_dir: str):
        self.logger = create_logger(self)
        self.data_dir = Path(data_dir)

    def _get_all_batches_dir(self) -> List[Path]:
        xp_folder = filter(lambda elem: elem.is_dir(), self.data_dir.glob("*/*"))
        return list(xp_folder)

    def _get_batch_dir(self, batch_name: str) -> Path:
        # search for directory
        xp_folder = self._get_all_batches_dir()
        res = list(filter(lambda batch_dir: batch_dir.name == batch_name, xp_folder))

        if len(res) == 0:
            return None
        else:
            return res[0]



    def get_batch_info(self, batch_name: str):

        df = self.get_raw_df(batch_name)

        batch_folder = self._get_batch_dir(batch_name)
        setup_id = batch_folder.parent.name

        try:
            date_start = df['time'].iloc[0]
            date_end = df['time'].iloc[-1]
        except IndexError as e:
            err_msg = f"Unable to extract date start and date end for batch {batch_name}"
            self.logger.error(err_msg)
            raise IndexError(err_msg, e)

        return BatchInfo(setup_id, batch_name, date_start, date_end)

    def get_batches(self) -> List[BatchInfo]:

        data_dir = self.data_dir

        # get first level folder
        xp_folder = self._get_all_batches_dir()
        xp_names = map(lambda elem: elem.name, xp_folder)

        res = map(lambda x: self.get_batch_info(x), xp_names)

        return list(res)

    def get_raw_df(self, batch_name: str) -> DataFrame:

        data_dir = self._get_batch_dir(batch_name)

        if data_dir is None:
            err_msg = f"Batch '{batch_name}' does not exist"
            raise Exception(err_msg)

        csv_file = list(data_dir.glob("*.csv"))
        csv_file.sort(key=os.path.getmtime)
        file_merger = FileMerger(csv_file)
        csv_str = file_merger.merge()

        cols = [
            'action',
            'device',
            'time',
            'rfid',
            'from_loc',
            'to_loc',
            'weight',
            'error',
            'direction',
            'activate',
            'liquids'
        ]


        dtype = {
            'rfid': str,
            'error': str
        }

        # df = pd.read_csv(csv_file, dtype=dtype, sep=";", names=cols, header=None)
        df = pd.read_csv(StringIO(csv_str), dtype=dtype, sep=";", names=cols, header=None)
        df[['day_since_start', 'trans_group']] = ''

        # format have changed btw experiment, we have to check the dateformat
        old_date_format = '%d-%m-%Y %H:%M:%S'

        tz_str = pytz.timezone("Europe/Paris")

        try:
            date = pd.to_datetime(df['time'], format=old_date_format)

            df['time'] = date
            # add time zone to be homogenous
            df["time"] = df["time"].dt.tz_localize(tz_str)
        except AmbiguousTimeError as e:
            err_msg = f"Unable to convert time to tz Paris for batch : {batch_name}"
            self.logger.error(err_msg)
            raise e
        except ValueError as error:
            # mixed instead of "%Y-%m-%dT%H:%M:%S.%f%z" or "ISO8601" because when ms is .000 pandas remove them and when saved in csv the format is not the same
            # and raise an error

            # tz_format = "%Y-%m-%dT%H:%M:%S.%f%z"

            df['time'] = pd.to_datetime(df['time'], format="ISO8601", utc=True).dt.tz_convert(tz_str)
            # df["time"] = df["time"].dt.tz_localize(tz_str)

        df.sort_values(by='time', inplace=True)
        df.reset_index(drop=True, inplace=True)

        return df


