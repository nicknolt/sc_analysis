from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
from dependency_injector.wiring import Provide, inject

from container import Container
from data_service import DataService, BatchInfo
from lmt.lmt_service import LMTService
from process import GlobalProcess

def has_overlap(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool:

    latest_start = max(a_start, b_start)
    earliest_end = min(a_end, b_end)

    return latest_start <= earliest_end

class LMT2BatchLinkProcess(GlobalProcess):

    @inject
    def __init__(self, data_service: DataService = Provide[Container.data_service], lmt_service: LMTService = Provide[Container.lmt_service]):
        super().__init__()

        self.data_service = data_service
        self.lmt_service = lmt_service

    @property
    def result_id(self) -> str:
        return 'lmt2batch'

    @property
    def dtype(self) -> Dict:
        pass

    def _compute(self) -> pd.DataFrame:

        batch_list: List[BatchInfo] = self.data_service.get_batches()
        db_list = self.lmt_service.get_db_infos()

        res_dict: List[Dict] = []

        for db in db_list:

            # res: List[BatchInfo] = list(filter(lambda batchinfo: (batchinfo.date_start <= db.date_start <= batchinfo.date_end) or (batchinfo.date_start <= db.date_end <= batchinfo.date_end), batch_list))
            res: List[BatchInfo] = list(filter(lambda batchinfo: has_overlap(batchinfo.date_start, batchinfo.date_end, db.date_start, db.date_end), batch_list))


            dict_entry = {'path': db.path, 'date_start': db.date_start, 'date_end': db.date_end, 'duration': db.duration, 'batch': ''}

            if len(res) == 1:
                self.logger.info(f"{db.path.name} is link with batch: {res[0].name}")
                dict_entry['batch'] = res[0].name
            elif len(res) > 1:
                raise Exception(f"link to more than one batch ({len(res)})")

            res_dict.append(dict_entry)

        df = pd.DataFrame(res_dict)
        df.sort_values(by='date_start', inplace=True)

        # add position inside batch group (to keep the same reference values into the events df)
        df["db_idx"] = df.groupby('batch').cumcount()

        return df

    def get_db_path(self, batch_name: str, date: datetime) -> Tuple[Path, int]:

        df = self.df

        res = df[(df['batch'] == batch_name) & (df.date_start <= date) & (date <= df.date_end)]

        if len(res) == 1:
            return Path(res.iloc[0].path), res.iloc[0].db_idx

        return None, None



    def initialize(self):
        self.df['date_start'] = pd.to_datetime(self.df['date_start'], format='mixed', utc=True)
        self.df['date_end'] = pd.to_datetime(self.df['date_end'], format='mixed', utc=True)

