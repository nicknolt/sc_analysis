import logging
import os
import subprocess
import unittest
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path

import matplotlib
import pytz

from common import ROOT_DIR
from common_log import basic_config_log
from container import Container
from lmt.lmt_db_reader import DBInfo
from lmt.lmt_video_service import LMTVideoService, VideoInfo

container = Container()
container.config.from_ini(ROOT_DIR / "tests/resources/config.ini")

class TestLMTVideoService(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        basic_config_log(level=logging.DEBUG)

    def test_VideoInfos(self):
        path = Path(r"\\192.168.25.175\souriscity\SourisCity2.0\LMT_DATA\Experience6mice3LMT\2023_03_21\video_t1116007.mp4")

        res = VideoInfo(video_path=path)

        print("ok")


    def test_get_video_path(self):
        date_str = "2023-09-04 13:24:30+02:00"
        date = datetime.fromisoformat(date_str)

        video_path = r"\\192.168.25.175\souriscity\SourisCity2.0\LMT_DATA"

        service = LMTVideoService(video_dir=video_path)

        res = service.get_videos_info()

        print("ok")
        # res = service.get_video_file("XP5", date)


if __name__ == '__main__':
    unittest.main()
