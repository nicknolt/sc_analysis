from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

from common_log import create_logger


class VideoInfo:

    def __init__(self, video_path: Path):
        self.video_path = video_path




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

