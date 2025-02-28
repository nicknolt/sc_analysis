import unittest
from pathlib import Path
from typing import List

from common import ROOT_DIR
from common_log import basic_config_log
from configuration import Configuration
from container import Container
from batch_process import ImportBatch, OccupationTime

import pandas as pd

from pre_analysis.pre_analysis import Action, MiceWeight

container = Container()
# container.wire(modules=["pseudo_lmt_analysis.process"])
container.config.from_ini(ROOT_DIR / "tests/resources/config.ini")

class TestCecile(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        basic_config_log()
        config = Configuration(base_dir=Path('/Users/macminicv/Documents/Data_SC'), result_dir=Path('/Users/macminicv/Documents/Data_SC/SC_OUTPUT'))

    def test_MiceWeight(self):
        batch = ImportBatch.load(batch_name="XP11")

        mice_weight = MiceWeight(batch)
        mice_weight.compute()

        mice_weight.export_figure()

        print("OK")

    def test_load_experiment(self):
        config = Configuration(base_dir=Path('/Users/macminicv/Documents/Data_SC/'))
        xp = ImportBatch.load(batch_name="XP11F2T")
    def test_occupation_time_each_mouse(self):
        config = Configuration(base_dir=Path('/Users/macminicv/Documents/Data_SC/'))
        xp = ImportBatch.load(batch_name="XP11")
        xp.get_mice_occupation("T_MAZE").compute()
        tutu = OccupationTime(xp)


        df = tutu.df

    def test_one_sequence(self):
        import pre_analysis
        config = Configuration(base_dir=Path('/Users/macminicv/Documents/Data_SC/'))

        res=pre_analysis.one_step_sequence("XP11F2T", Action.LEVER_PRESS)
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

