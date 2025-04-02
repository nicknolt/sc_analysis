from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
from dependency_injector.wiring import Provide, inject

from container import Container
from data_service import DataService, BatchInfo
from lmt.lmt2batch_link_process import has_overlap
from lmt.lmt_service import LMTService
from lmt.lmt_video_service import LMTVideoService, VideoInfo
from process import GlobalProcess


class Video2BatchLinkProcess(GlobalProcess):

    @inject
    def __init__(self, data_service: DataService = Provide[Container.data_service], video_service: LMTVideoService = Provide[Container.video_service]):
        super().__init__()

        self.data_service = data_service
        self.video_service = video_service

    @property
    def result_id(self) -> str:
        return 'video2batch'

    @property
    def dtype(self) -> Dict:
        pass

    def _compute(self) -> pd.DataFrame:

        batch_list: List[BatchInfo] = self.data_service.get_batches()
        video_list: List[VideoInfo] = self.video_service.get_videos_info()

        res_dict: List[Dict] = []

        for video in video_list:

            # # res: List[BatchInfo] = list(filter(lambda batchinfo: (batchinfo.date_start <= db.date_start <= batchinfo.date_end) or (batchinfo.date_start <= db.date_end <= batchinfo.date_end), batch_list))
            res: List[BatchInfo] = list(filter(lambda batchinfo: has_overlap(batchinfo.date_start, batchinfo.date_end, video.date_start, video.date_end), batch_list))
            #
            #
            dict_entry = {'path': video.path, 'date_start': video.date_start, 'date_end': video.date_end, 'duration': video.duration, 'batch': ''}

            if len(res) == 1:
                self.logger.info(f"{video.path.name} is linked with batch: {res[0].name}")
                dict_entry['batch'] = res[0].name
            elif len(res) > 1:
                err_msg = f"video {video.path} is linked to many batches : {','.join(map(lambda batchinfo: batchinfo.name, res))}"
                raise Exception(err_msg)

            res_dict.append(dict_entry)

        df = pd.DataFrame(res_dict)
        df.sort_values(by='date_start', inplace=True)

        # add position inside batch group (to keep the same reference values into the events df)
        df["video_idx"] = df.groupby('batch').cumcount()

        return df

    def get_db_path(self, batch_name: str, date: datetime = None, db_idx: int = None) -> Tuple[Path, int]:

        df = self.df
        res = None

        if date is not None:
            res = df[(df['batch'] == batch_name) & (df.date_start <= date) & (date <= df.date_end)]

        if db_idx is not None:
            res = df[(df['batch'] == batch_name) & (df.db_idx == db_idx)]

        if len(res) == 1:
            return Path(res.iloc[0].path), res.iloc[0].db_idx

        return None, None



    def initialize(self):
        self.df['date_start'] = pd.to_datetime(self.df['date_start'], format='mixed', utc=True)
        self.df['date_end'] = pd.to_datetime(self.df['date_end'], format='mixed', utc=True)

