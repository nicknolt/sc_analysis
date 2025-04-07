import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import vlc
from vlc import MediaPlayer

from batch_process.import_batch import ImportBatch
from common_log import create_logger
from lmt.lmt2batch_link_process import LMT2BatchLinkProcess
from lmt.video2batch_link_process import Video2BatchLinkProcess


class LMTVideoReader:

    def __init__(self, batch_name: str):
        self.logger = create_logger(self)
        self.batch_name = batch_name
        self._df_event: pd.DataFrame = None


    def play_by_date(self, date: datetime, play_before: int = 1):

        p = Video2BatchLinkProcess()
        df_video = p.df

        video_path, video_row = p.get_video_path(batch_name=self.batch_name, date=date)


        delta_t = (date - video_row.date_start).total_seconds()
        #
        # path = (self.lmt_service.lmt_dir / Path(res.iloc[0].path)).resolve()


        cmd = f"""vlc --start-time={delta_t-play_before} {video_path} """

        self.logger.info(f"Play at {date - timedelta(seconds=play_before)} s in file {video_path}")

        returned_value = subprocess.call(cmd, shell=True)




    def play_by_event(self, event_id: int, play_before: int = 1):

        df_event = ImportBatch(self.batch_name).df
        df_event = df_event.iloc[event_id]

        self.play_by_date(date=df_event["time"], play_before=play_before)
