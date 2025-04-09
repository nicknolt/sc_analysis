import logging
import unittest
from datetime import datetime
from pathlib import Path
from time import sleep

import pytz

from batch_process.import_batch import ImportBatch
from common import ROOT_DIR
from common_log import basic_config_log
from container import Container
from lmt.lmt_video_reader import LMTVideoReader
from lmt.lmt_video_service import LMTVideoService, VideoInfo
from lmt.video2batch_link_process import Video2BatchLinkProcess

container = Container()
container.config.from_ini(ROOT_DIR / "tests/resources/config.ini")

class TestLMTVideoService(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        basic_config_log(level=logging.DEBUG)

    def test_LMTVideoReader(self):
        reader = LMTVideoReader("XP6")

        date_str = "2023-10-23 09:53:24+02:00"
        date = datetime.fromisoformat(date_str).astimezone(pytz.timezone("Europe/Paris"))
        reader.play_by_date(date=date)


    def test_LMTVideoReader_by_event(self):
        reader = LMTVideoReader("XP6")
        reader.play_by_event(124428)



    def test_compute_all(self):

        batches = container.data_service().get_batches()

        for batch in batches:
            print(f"batch {batch.name}")
            df = ImportBatch(batch_name=batch.name).df

    def test_video_batch2link(self):
        res = Video2BatchLinkProcess().df

        print("ok")

    def test_add_video_frame_info(self):
        df = ImportBatch(batch_name="XP6").df


        print("ok")

    def test_VideoInfos(self):
        path = Path(r"\\192.168.25.175\souriscity\SourisCity2.0\LMT_DATA\SC1\Experience6mice3LMT\2023_03_21\video_t1116007.mp4")

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
