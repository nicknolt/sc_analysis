import math
from enum import Enum
from typing import List, Dict

import pandas as pd

from batch_process import ImportBatch
from process import Process, RFigure, BatchProcess
import numpy as np

def one_step_sequence(batch_name: str, from_event: 'Action') -> 'OneStepSequence':

    batch = ImportBatch.load(batch_name=batch_name)
    res = OneStepSequence(batch=batch, from_event=from_event)
    res.compute()

    return res

class Action(Enum):
    TRANSITION = 1,
    LEVER_PRESS = 2

# class LinkXP2DB(Process):
#
#     def __init__(self):
#         super().__init__()
#
#     @property
#     def result_id(self) -> str:
#         return f"db_link"

    

class FeederTimeDistribution(Process):

    def __init__(self, batch: ImportBatch):
        super().__init__()
        self.batch = batch

    @property
    def result_id(self) -> str:
        return f"{self.batch_name}_feeder_time_distribution"

    def _compute(self) -> pd.DataFrame:
        df = self.batch.df
        df_lever = df[df['action'] == 'id_lever']
        df_feeder = df[df['action'] == 'feeder']

        df_lever['idx_next'] = df_lever.index.to_series().shift(-1)

        def get_delta_t(row: pd.Series):
            nonlocal df_feeder

            # bc of possible unexpected repetition of feeder event
            # print(f"where < {row.idx_next} and > {row.name}")
            res = np.where((df_feeder.index < row.idx_next) & (df_feeder.index > row.name), df_feeder.index, row.idx_next).min()

            # to exclude
            if res == row.idx_next or math.isnan(res):
                return -1

            feeder_row = df_feeder.loc[res]
            delta = (feeder_row.time - row.time).total_seconds()

            return delta

        df_lever['delta'] = df_lever.apply(get_delta_t, axis=1)

        return df_lever


    @property
    def batch_name(self) -> str:
        return self.batch.batch_name


class OneStepSequenceFigure(RFigure):

    def __init__(self, process: 'OneStepSequence'):
        super().__init__(process, "ND_LP_camembert.R")

    @property
    def extra_args(self) -> Dict[str, str]:
        return {"from_event": self.process.from_event.name}
    @property
    def figure_id(self) -> str:
        return f"{self.process.result_id}.jpg"


class MiceWeight(BatchProcess):

    def __init__(self, batch_name: str):
        super().__init__(batch_name=batch_name)

    @property
    def result_id(self) -> str:
        return f"{self.batch_name}_mice_weight"

    def _compute(self) -> pd.DataFrame:

        df = ImportBatch(batch_name=self.batch_name).df
        df = df[(df['action'] == 'transition') & (df['rfid'] != "0")]

        # keep only usefull columns
        df = df.loc[:, ['rfid', 'weight', 'day_since_start']]

        return df

    def initialize(self):
        self.figure = MiceWeightFigure(process=self)


class MiceWeightFigure(RFigure):

    def __init__(self, process: Process):
        super().__init__(process, "ND_Weight_from Local_sniffer_events.R")

    @property
    def figure_id(self) -> str:
        return f"{self.process.result_id}.jpg"

    @property
    def extra_args(self) -> List[str]:
        return


class OneStepSequence(Process):

    def __init__(self, batch: ImportBatch, from_event: Action):
        super().__init__()
        self.batch = batch
        self.from_event = from_event

    @property
    def result_id(self) -> str:
        return f"{self.batch.batch_name}_one_step_seq_{Action(self.from_event).name}"

    def _compute(self) -> pd.DataFrame:

        location = "LMT"

        # events = self.batch.df[['action', 'time', 'rfid', 'day_since_start']].copy()
        events = self.batch.df[['action', 'time', 'rfid', 'from_loc', 'to_loc', 'day_since_start', 'trans_group']].copy()
        events[["next_action", "time_next_action", "trans_group_next"]] = events.groupby(["rfid"])[['action', 'time', 'trans_group']].shift(-1)

        events["duration"] = (events["time_next_action"] - events['time']).dt.total_seconds()

        if self.from_event == Action.TRANSITION:
            events = events[(events['action'] == 'transition') & (events['to_loc'] == location)]
        elif self.from_event == Action.LEVER_PRESS:
            events = events[events['action'] == 'id_lever']


        df_mice_loc = self.batch.mice_location.df
        tutu = df_mice_loc.groupby(["trans_group"]).size().to_frame('size')
        tutu = tutu[tutu["size"] > 1]
        events['nb_mice'] = events.merge(df_mice_loc[['trans_group', f'nb_mice_{location}']], how='left', on='trans_group')[f'nb_mice_{location}'].values
        events['nb_mice_next'] = events.merge(df_mice_loc[['trans_group', f'nb_mice_{location}']], how='left', right_on='trans_group', left_on='trans_group_next')[f'nb_mice_{location}'].values

        return events

    def initialize(self):
        self.figure = OneStepSequenceFigure(process=self)

    @property
    def batch_name(self) -> str:
        return self.batch.batch_name