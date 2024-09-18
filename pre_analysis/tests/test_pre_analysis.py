import unittest
from pathlib import Path

from common_log import basic_config_log
from configuration import Configuration
from model import Batch
# from pre_analysis.pre_analysis import OneStepSequence, Action
import pre_analysis
from pre_analysis import one_step_sequence
from pre_analysis.pre_analysis import Action, OneStepSequence


class TestPreAnalysis(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        basic_config_log()
        config = Configuration(base_dir=Path('../../tests/resources'), result_dir=Path(r"C:\Users\Nicolas\Desktop\tmp"))

    def test_OneStepSeq(self):
        # config = Configuration(base_dir=Path('../../tests/resources'), result_dir=Path(r"C:\Users\Nicolas\Desktop\tmp"))

        # res = one_step_sequence(batch_name="XP11", from_event=Action.TRANSITION)
        #
        # res.to_csv()
        xp = Batch.load(batch_name="XP11")

        res = OneStepSequence(batch=xp, from_event=Action.LEVER_PRESS)
        res_comp = res.compute(force_recompute=True)
        #
        print("ok")

    def test_figure(self):
        xp = Batch.load(batch_name="XP11")

        res = OneStepSequence(batch=xp, from_event=Action.LEVER_PRESS)
        res.export_figure()

        res = OneStepSequence(batch=xp, from_event=Action.TRANSITION)
        res.export_figure()

