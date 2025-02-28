import os
from io import StringIO
from pathlib import Path

import pandas as pd
from pandas import DataFrame

from common import FileMerger


class DataService:

    def __init__(self, result_dir: str):
        self.result_dir = Path(result_dir)

    def get_csv_data(self, batch_name: str) -> DataFrame:

        data_dir = self.result_dir / "data" / batch_name

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

        try:
            date = pd.to_datetime(df['time'], format=old_date_format)
        except ValueError as error:
            # mixed instead of "%Y-%m-%dT%H:%M:%S.%f%z" or "ISO8601" because when ms is .000 pandas remove them and when saved in csv the format is not the same
            # and raise an error
            date = pd.to_datetime(df['time'], format="mixed")

        df['time'] = date
        df.sort_values(by='time', inplace=True)
        df.reset_index(drop=True, inplace=True)

        return df


