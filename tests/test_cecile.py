import unittest
from pathlib import Path
from typing import List

from batch_process.batch_process import MiceSequence
from batch_process.import_batch import ImportBatch
from common import ROOT_DIR
from common_log import basic_config_log
from container import Container


from pre_analysis.pre_analysis import Action, MiceWeight

container = Container()
# container.wire(modules=["pseudo_lmt_analysis.process"])
container.config.from_ini('/Users/macminicv/Documents/Data_SC/config.ini')

class TestCecile(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        basic_config_log()

    def test_tambouille(self):
        path_str = "/Volumes/souriscity/SourisCity2.0/Sniffer local"
        path = Path(path_str).exists()
        print(path)

    def test_batch_for_all(self):

        batches = container.data_service().get_batches()

        for batch in batches:
            print(f"batch {batch.name}")
            MiceSequence(batch_name=batch.name).df
            # ImportBatch.load(batch_name=batch.name)

    def test_MiceWeight(self):
        batch = ImportBatch.load(batch_name="XP13F3")

        # mice_weight = MiceWeight(batch_name="XP11")
        # mice_weight.compute()

        # mice_weight.export_figure()

        print("OK")




