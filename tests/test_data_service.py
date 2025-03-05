import os
import subprocess
import unittest
from pathlib import Path
from typing import Dict

import pandas as pd
import pytz

from batch_process import ImportBatch, MiceSequence, OccupationTime
from common import FileMerger, ROOT_DIR
from common_log import basic_config_log
from container import Container
from data_service import DataService
from pre_analysis.pre_analysis import MiceWeight

container = Container()
container.config.from_ini(ROOT_DIR / "tests/resources/config.ini")

class TestDataService(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        basic_config_log()
        # config = Configuration(base_dir=Path('./resources'), result_dir=Path(r"C:\Users\Nicolas\Desktop\tmp\SC_OUTPUT"))


    def test_get_batch_info(self):
        data_service = container.data_service()

        df = data_service.get_raw_df("XP11")

        print("ok")

    def test_gel_all_batches(self):
        data_service = container.data_service()

        res = data_service.get_batches()

        print("ok")
