import unittest
from pathlib import Path

import matplotlib.pyplot as plt

from batch_process import ImportBatch, MiceSequence, MiceOccupation
from common import ROOT_DIR
from common_log import basic_config_log
from container import Container
from pre_analysis import one_step_sequence
from pre_analysis.pre_analysis import Action, OneStepSequence, MiceWeight, FeederTimeDistribution

container = Container()
container.config.from_ini(ROOT_DIR / "tests/resources/config.ini")

class TestPreAnalysis(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        basic_config_log()
        # config = Configuration(base_dir=Path('../../tests/resources'), result_dir=Path(r"C:\Users\Nicolas\Desktop\tmp"))

    def test_with_func(self):
        res = one_step_sequence(batch_name="XP11F2T", from_event=Action.TRANSITION).export_figure()
        # res = one_step_sequence(batch_name="XP11", from_event=Action.LEVER_PRESS).export_figure()


    def test_distribution_complete_seq(self):
        batch_name = "XP11"
        batch = ImportBatch(batch_name=batch_name)

        df = MiceSequence(batch=batch).df

        df.drop(df[(df.day_since_start > 21) | (df.elapsed_s > 20) | (df.nb_mice_np == 1)].index, inplace=True)
        df = df[(df.rfid_lp == df.rfid_np)]

        print(df['elapsed_s'].describe())

        res = df['elapsed_s'].hist(bins=20)


        # add labels and title
        plt.xlabel(f'Complete seq distribution: {batch_name}')
        plt.ylabel('Frequency')
        plt.title('Complete sequence Time')
        plt.show()
        print("ok")


        print("ok")


    def test_feeder_time_distribution(self):

        batch_name = "XP14F4"
        batch = ImportBatch(batch_name=batch_name)

        MiceOccupation(batch_name="XP14F4").df

        df = FeederTimeDistribution(batch=batch).compute(force_recompute=True)
        print(df['delta'].describe())
        df.drop(df[(df.delta == -1) | (df.delta > 100)].index, inplace=True)
        # df.drop(df[(df.delta == -1)].index, inplace=True)
        df = df[df['day_since_start'] == 2]
        res = df['delta'].hist(bins=20)


        # add labels and title
        plt.xlabel(f'Delivery time {batch_name}')
        plt.ylabel('Frequency')
        plt.title('Distribution Delivery Time')
        plt.show()
        print("ok")


    def test_MiceWeight(self):
        batch = ImportBatch.load(batch_name="XP11F2T")

        mice_weight = MiceWeight(batch)
        mice_weight.compute()

        mice_weight.export_figure()

        print("OK")



    def test_OneStepSeq(self):
        # config = Configuration(base_dir=Path('../../tests/resources'), result_dir=Path(r"C:\Users\Nicolas\Desktop\tmp"))

        # res = one_step_sequence(batch_name="XP11", from_event=Action.TRANSITION)
        #
        # res.to_csv()
        xp = ImportBatch.load(batch_name="XP11")

        res = OneStepSequence(batch=xp, from_event=Action.LEVER_PRESS)
        res_comp = res.compute(force_recompute=True)
        #
        print("ok")

    def test_figure(self):
        xp = ImportBatch.load(batch_name="XP11")

        res = OneStepSequence(batch=xp, from_event=Action.LEVER_PRESS)
        res.export_figure()

        res = OneStepSequence(batch=xp, from_event=Action.TRANSITION)
        res.export_figure()

