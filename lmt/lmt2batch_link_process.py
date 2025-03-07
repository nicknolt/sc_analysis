from typing import Dict, List

import pandas as pd
from dependency_injector.wiring import Provide, inject

from container import Container
from data_service import DataService, BatchInfo
from lmt.lmt_service import LMTService
from parameters import Parameters

from process import GlobalProcess


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

            res: List[BatchInfo] = list(filter(lambda batchinfo: (batchinfo.date_start <= db.date_start <= batchinfo.date_end) or (batchinfo.date_start <= db.date_end <= batchinfo.date_end), batch_list))


            dict_entry = {'path': db.path, 'date_start': db.date_start, 'date_end': db.date_end, 'duration': db.duration, 'batch': ''}

            if len(res) == 1:
                self.logger.info(f"{db.path.name} is link with batch: {res[0].name}")
                dict_entry['batch'] = res[0].name
            elif len(res) > 1:
                raise Exception(f"link to more than one batch ({len(res)})")

            res_dict.append(dict_entry)

        df = pd.DataFrame(res_dict)
        df.sort_values(by='date_start', inplace=True)

        return df

    def initialize(self):
        self.df['date_start'] = pd.to_datetime(self.df['date_start'], format='mixed')
        self.df['date_end'] = pd.to_datetime(self.df['date_end'], format='mixed')

