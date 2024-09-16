import unittest
from pathlib import Path
from typing import List

from common_log import basic_config_log
from configuration import Configuration
from model import Batch, OccupationTime

import pandas as pd
class TestCecile(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        basic_config_log()

    def test_load_experiment(self):
        config = Configuration(base_dir=Path('/Users/macminicv/Documents/Data_SC/'))
        xp = Batch.load(batch_name="XP11T")
    def test_occupation_time_each_mouse(self):
        config = Configuration(base_dir=Path('/Users/macminicv/Documents/Data_SC/'))
        xp = Batch.load(batch_name="XP11T")
        xp.get_mice_occupation("T_MAZE").compute()
        tutu = OccupationTime(xp)


        df = tutu.df


    # def test_MiceOccupation(self):
    #     config = Configuration(base_dir=Path('./resources'))
    #
    #     print("ok")
    #     # self.assertEqual(Path('./resources'), config.get_base_dir())
    #     #
    #     # config2 = Configuration()
    #     #
    #     # self.assertIs(config, config2)

