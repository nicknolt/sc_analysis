import os
import unittest
from pathlib import Path
from typing import Dict, Set, List

from common import FileMerger
from common_log import basic_config_log
from configuration import Configuration
from model import Experiment, MiceOccupation, MiceSequence

import pandas as pd

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

        tutu = xp.mice_location.mice_occupation
        print("OK")

    def test_compute_sequences(self):
        config = Configuration(base_dir=Path('./resources'))
        xp = Experiment.load(xp_name="XP11")

        ms = MiceSequence(xp)
        res = ms.compute(force_recompute=True)

        print("ok")


    def test_occupation_time_each_mouse(self):
        config = Configuration(base_dir=Path('./resources'))
        xp = Experiment.load(xp_name="XP11")

        tutu = xp.mice_location.mice_occupation

        df = tutu._df
        #
        # df['mice_comb'] = df['mice_comb'].fillna('EMPTY')
        # df['nb_mice'] = df.apply(lambda x: 0 if x['mice_comb'] == 'EMPTY' else len(x['mice_comb'].split('|')), axis=1)

        to_concat: List[pd.DataFrame] = list()

        for mouse in [*xp.mice, 'EMPTY']:
            df_mouse = df[df['mice_comb'].str.contains(mouse)]
            tmp_df = df_mouse.groupby(['day_since_start', 'nb_mice'])['duration'].sum().reset_index()
            tmp_df['mouse'] = mouse
            to_concat.append(tmp_df)

        merged = pd.concat(to_concat).sort_values(['day_since_start', 'mouse'])




        print("ok")


        # def mouse_is_in(combi: str):
        #     return mouse in combi
        #
        # for day, day_data in xp.mice_location.mice_occupation._df.groupby('day_since_start'):
        #     for mouse in xp.mice:
        #         tutu = day_data.mouse_comb.apply()
        #
        #         print("ok")
        #         mouse_res = list()
        #         for index, row in day_data.iterrows():
        #             mice = str(row.mice_comb).split(',')
        #             if mouse in mice:
        #                 tmp_res = {
        #                     'nb_mice': len(mice),
        #                     'duration': row.duration,
        #                 }
        #
        #                 mouse_res.append(tmp_res)
        #
        #         if not mouse_res:
        #             continue
        #
        #     mouse_df = pd.DataFrame(mouse_res)
        #
        #     t_mouse_df = mouse_df.groupby('nb_mice')['duration'].sum().to_dict()
        #
        #     day_res = {
        #         'mouse': mouse,
        #         **t_mouse_df,
        #         'day_since_start': day
        #     }
        #
        #     res_final.append(day_res)
        #
        #     print(f"souris {mouse} dans {mice}")
        #
        # df_res = pd.DataFrame(res_final)

        print("")


    def test_mice_occupation_in_LMT(self):

        # filename = "2_21_15_45_31.csv" #XP9
        # csv_path = Path('./resources/XP9')

        config = Configuration(base_dir=Path('./resources'))

        xp = Experiment.load('XP9')
        mo = xp.mice_location

        mice = list(mo._df.columns[3:])

        res_per_day: Dict[int, Dict[str, float]] = dict()

        for day, day_data in xp.mice_location._df.groupby('day_since_start'):

            res_comb: Dict[str, float] = dict()
            for index, row in day_data.iterrows():
                mice_in_lmt = [x for x in mice if row[x] == "LMT"]
                mice_key = ','.join(mice_in_lmt)

                if mice_key not in res_comb:
                    res_comb[mice_key] = row.duration
                    # print("A CREER")
                else:
                    res_comb[mice_key] += row.duration
                    # print("existe deja")

            res_per_day[day] = res_comb

        final_res = list()

        for day, values in res_per_day.items():
            for mice_comb, duration in values.items():
                final_res.append({
                    'mice_comb':mice_comb,
                    'duration': duration,
                    'day_since_start': day
                })

        df = pd.DataFrame(final_res)
        df.to_csv("./tutu.csv")
        print("ok")

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

