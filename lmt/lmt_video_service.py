from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import pytz

from common_log import create_logger

import cv2
import os

class VideoInfo:

    def __init__(self, video_path: Path):

        self.logger = create_logger(self)
        self.path = video_path
        self.duration: float = None
        self.date_start: datetime = None
        self.date_end: datetime = None

        self._initialize()

    def _initialize(self):

        self.logger.debug(f"Initialize video {self.path}")
        cap = cv2.VideoCapture(str(self.path))

        try:
            self.duration = cap.get(cv2.CAP_PROP_FRAME_COUNT) / cap.get(cv2.CAP_PROP_FPS)
            self.date_end = datetime.fromtimestamp(os.path.getmtime(self.path)).astimezone(pytz.timezone("Europe/Paris"))
            self.date_start = (self.date_end - timedelta(seconds=self.duration)).astimezone(pytz.timezone("Europe/Paris"))
        except Exception as e:
            err_msg = f"Failed to initialize video {self.path}"
            self.logger.error(err_msg)
        finally:
            cap.release()


class LMTVideoService:

    def __init__(self, video_dir: str):
        self.logger = create_logger(self)
        self.video_dir = Path(video_dir)


    def _get_all_video_files(self) -> List[Path]:

        res = list(self.video_dir.glob('**/*.mp4'))

        self.logger.info(f"{len(res)} videos files found")

        return res



    def get_videos_info(self) -> List[VideoInfo]:

        self.logger.info(f"Getting Videos infos in directory '{self.video_dir}'")

        res = map(lambda video_path: VideoInfo(video_path), self._get_all_video_files())

        return list(res)
        # res = map(lambda db_path: LMTDBReader(db_path).db_info, self._get_all_db_files())
        #
        # return list(res)

