from typing import Dict

import pandas as pd

from batch_process.import_batch import ImportBatch
from process import BatchProcess

import numpy as np

class AnomaliesDetectionProcess(BatchProcess):


    def __init__(self, batch_name: str):
        super().__init__(batch_name)

    @property
    def result_id(self) -> str:
        return f"{self.batch_name}_anomalies_detection"

    @property
    def dtype(self) -> Dict:
        pass

    def _detect_lever_stuck(self, df_events: pd.DataFrame):

        df_events['streak'] = 0

        df = df_events[df_events['action'] == "id_lever"]
        df['delta_t'] = (df['time'].shift(-1) - df['time']).dt.total_seconds()

        threshold = 5.1
        condition = df['delta_t'].lt(threshold).to_numpy()

        streaks = np.zeros(len(condition), dtype=int)
        streaks[0] = int(condition[0])

        for i in range(1, len(condition)):
            streaks[i] = streaks[i - 1] + 1 if condition[i] else 0

        df['streak'] = streaks
        df.loc[df['streak'] >= 5, 'error'] = 'LEVER_STUCK'
        df_events.update(df)




    def _compute(self) -> pd.DataFrame:

        df_events = ImportBatch(batch_name=self.batch_name).df
        df_events = df_events[['action', 'time', 'error', 'rfid']]

        self._detect_lever_stuck(df_events)

        df_events.loc[df_events['rfid'].isna()] = "LMT_STUCK"
        # df_events.update(self._detect_lever_stuck(df_events))

        return df_events
