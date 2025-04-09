import logging
import unittest
from datetime import datetime

from batch_process.anomalies_detection_process import AnomaliesDetectionProcess
from batch_process.import_batch import ImportBatch
from common import ROOT_DIR
from common_log import basic_config_log
from container import Container

container = Container()
container.config.from_ini(ROOT_DIR / "tests/resources/config.ini")

import pandas as pd
import numpy as np
class TestLMTDBReader(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        basic_config_log(level=logging.DEBUG)
        logging.getLogger("matplotlib").setLevel(logging.WARNING)

    def test_update(self):
        p = ImportBatch("XP6")
        p.update(AnomaliesDetectionProcess(batch_name="XP6"))
        df = p.df


        # old_df = p.df
        # res_df = AnomaliesDetectionProcess(batch_name=p.batch_name).compute(force_recompute=True)
        #
        # # new_df = p.df
        #
        # res_df = res_df[res_df['streak'] > 1]
        print(df.error.unique())
        print("tutu")

    def test(self):
        df = ImportBatch("XP7").df
        df_lp = df[df['action'] == "id_lever"]
        df_lp['delta_t'] = (df_lp['time'].shift(-1) - df_lp['time']).dt.total_seconds()

        # Seuil en secondes
        seuil = 5.1

        ts = datetime.now()
        # Créer une condition booléenne : True si le delta < seuil
        condition = df_lp['delta_t'].lt(seuil).to_numpy()

        # Utiliser numpy pour calculer les streaks
        streaks = np.zeros(len(condition), dtype=int)
        streaks[0] = int(condition[0])

        for i in range(1, len(condition)):
            streaks[i] = streaks[i - 1] + 1 if condition[i] else 0

        df_lp['streak'] = streaks

        df_lp = df_lp[df_lp['streak'] != 0]

        print("ok")