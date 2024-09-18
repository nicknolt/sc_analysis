from enum import Enum
from typing import List, Dict

import pandas as pd

from configuration import Configuration
from model import Cachable, Batch

def one_step_sequence(batch_name: str, from_event: 'Action') -> 'OneStepSequence':

    batch = Batch.load(batch_name=batch_name)
    res = OneStepSequence(batch=batch, from_event=from_event)
    res.compute()

    return res

class Action(Enum):
    TRANSITION = 1,
    LEVER_PRESS = 2

class OneStepSequence(Cachable):

    def __init__(self, batch: Batch, from_event: Action):
        super().__init__()
        self.batch = batch
        self.from_event = from_event

    @property
    def result_id(self) -> str:
        return f"{self.batch.xp_name}_one_step_seq_{Action(self.from_event).name}"

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

        events['nb_mice'] = events.merge(df_mice_loc[['trans_group', f'nb_mice_{location}']], on='trans_group')[f'nb_mice_{location}'].values
        events['nb_mice_next'] = events.merge(df_mice_loc[['trans_group', f'nb_mice_{location}']], right_on='trans_group', left_on='trans_group_next')[f'nb_mice_{location}'].values

        return events

    @property
    def xp_name(self) -> str:
        return self.batch.xp_name