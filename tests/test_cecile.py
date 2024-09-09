import unittest
from pathlib import Path
from typing import List

from common_log import basic_config_log
from configuration import Configuration
from model import Experiment

import pandas as pd
class TestCecile(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        basic_config_log()

    def test_load_experiment(self):
        config = Configuration(base_dir=Path('/Users/macminicv/Documents/Data_SC/'))
        xp = Experiment.load(xp_name="XP11T")
    def test_occupation_time_each_mouse(self):
        config = Configuration(base_dir=Path('/Users/macminicv/Documents/Data_SC/'))
        xp = Experiment.load(xp_name="XP11T")

        tutu = xp.mice_location.mice_occupation

        df = tutu._df


        to_concat: List[pd.DataFrame] = list()

        for mouse in [*xp.mice, 'EMPTY']:
            df_mouse = df[df['mice_comb'].str.contains(mouse)]
            tmp_df = df_mouse.groupby(['day_since_start', 'nb_mice'])['duration'].sum().reset_index()
            tmp_df['mouse'] = mouse
            to_concat.append(tmp_df)

        merged = pd.concat(to_concat).sort_values(['day_since_start', 'mouse']).reset_index(drop=True)

        merged.to_csv('./tutu.csv')
    # def test_MiceOccupation(self):
    #     config = Configuration(base_dir=Path('./resources'))
    #
    #     print("ok")
    #     # self.assertEqual(Path('./resources'), config.get_base_dir())
    #     #
    #     # config2 = Configuration()
    #     #
    #     # self.assertIs(config, config2)

