import unittest
from pathlib import Path
from typing import List

from common_log import basic_config_log
from configuration import Configuration
from model import Batch, OccupationTime

import pandas as pd

from pre_analysis.pre_analysis import Action


class TestCecile(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        basic_config_log()

    def test_load_experiment(self):
        config = Configuration(base_dir=Path('/Users/macminicv/Documents/Data_SC/'))
        xp = Batch.load(batch_name="XP11")
    def test_occupation_time_each_mouse(self):
        config = Configuration(base_dir=Path('/Users/macminicv/Documents/Data_SC/'))
        xp = Batch.load(batch_name="XP11")
        xp.get_mice_occupation("T_MAZE").compute()
        tutu = OccupationTime(xp)


        df = tutu.df

    def test_one_sequence(self):
        import pre_analysis
        config = Configuration(base_dir=Path('/Users/macminicv/Documents/Data_SC/'))

        res=pre_analysis.one_step_sequence("XP11", Action.TRANSITION)
        df = res.df

        print("ok")

        #config = Configuration(base_dir=Path('/Users/macminicv/Documents/Data_SC/'), result_dir='/Users/macminicv/Documents/Data_SC/Results')
    # def test_MiceOccupation(self):
    #     config = Configuration(base_dir=Path('./resources'))
    #
    #     print("ok")
    #     # self.assertEqual(Path('./resources'), config.get_base_dir())
    #     #
    #     # config2 = Configuration()
    #     #
    #     # self.assertIs(config, config2)

