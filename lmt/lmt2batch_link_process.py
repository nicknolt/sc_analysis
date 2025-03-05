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
        pass

    @property
    def dtype(self) -> Dict:
        pass

    def _compute(self) -> pd.DataFrame:

        batch_list: List[BatchInfo] = self.data_service.get_batches()
        db_list = self.lmt_service.get_db_infos()

        for db in db_list:
            res = list(filter(lambda batchinfo:  db.date_start <= batchinfo.date_end <= db.date_end, batch_list))
            print(f"{db.path} is link to {len(res)} batches")

        print("ok")

