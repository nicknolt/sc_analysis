import os
import unittest
from pathlib import Path

from common import FileMerger
from common_log import basic_config_log
from configuration import Configuration
from model import Experiment


class TestModel(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        basic_config_log()

    def test_load_experiment(self):
        config = Configuration(base_dir=Path('./resources'))
        xp = Experiment.load(xp_name="XP9", delete_cache=False)

    def test_load_experiment_XP6(self):
        config = Configuration(base_dir=Path('./resources'))
        xp = Experiment.load(xp_name="XP11", delete_cache=False)

        print("OK")

    def test_FileMerger(self):
        dir = Path("resources/data/XP6")

        files = list(dir.glob(pattern="*.csv"))
        files.sort(key=os.path.getmtime)



        fm = FileMerger(files=files)
        tutu = fm.merge()

        print("ok")
    # def test_MiceOccupation(self):
    #     config = Configuration(base_dir=Path('./resources'))
    #
    #     print("ok")
    #     # self.assertEqual(Path('./resources'), config.get_base_dir())
    #     #
    #     # config2 = Configuration()
    #     #
    #     # self.assertIs(config, config2)

