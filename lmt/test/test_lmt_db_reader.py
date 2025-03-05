import unittest
from datetime import datetime
from pathlib import Path

from common import ROOT_DIR
from common_log import basic_config_log
from container import Container
from lmt.lmt2batch_link_process import LMT2BatchLinkProcess
from lmt.lmt_db_reader import LMTDBReader
from lmt.lmt_service import LMTService

container = Container()
container.config.from_ini(ROOT_DIR / "tests/resources/config.ini")

class TestLMTDBReader(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        basic_config_log()

    def test_lmt2batch(self):
        process = LMT2BatchLinkProcess()

        res = process.df

        print("ok")

    def test_something(self):
        db_file = Path(r"\\192.168.25.177\SourisCity\SourisCity_2\Experience6mice3LMT_0_2\2023_04_12\2023_04_12.sqlite")
        reader = LMTDBReader(db_file)

        begin = reader.date_start
        end = reader.date_end

        res = reader.get_corresponding_frame_number(date=datetime(year=2023, month=4, day=12, hour=10))
        print("kikoo")

    def test_lmt_service(self):
        lmt_service = container.lmt_service()

        res = lmt_service.get_db_infos()

        print("ok")

if __name__ == '__main__':
    unittest.main()
