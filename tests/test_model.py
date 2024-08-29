import unittest
from pathlib import Path

from common_log import basic_config_log
from configuration import Configuration
from model import ExperimentNew


class TestModel(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        basic_config_log()

    def test_load_experiment(self):
        config = Configuration(base_dir=Path('./resources'))
        xp = ExperimentNew.load(xp_name="XP9")

    # def test_MiceOccupation(self):
    #     config = Configuration(base_dir=Path('./resources'))
    #
    #     print("ok")
    #     # self.assertEqual(Path('./resources'), config.get_base_dir())
    #     #
    #     # config2 = Configuration()
    #     #
    #     # self.assertIs(config, config2)

