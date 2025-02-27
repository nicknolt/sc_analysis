import os
from pathlib import Path

import pandas as pd

from common import FileMerger


class DataService:

    def __init__(self, result_dir: str):
        self.result_dir = Path(result_dir)

    def get_csv_data(self, batch_name: str) -> str:

        data_dir = self.result_dir / "data" / batch_name

        csv_file = list(data_dir.glob("*.csv"))
        csv_file.sort(key=os.path.getmtime)
        file_merger = FileMerger(csv_file)
        csv_str = file_merger.merge()

        return csv_str

