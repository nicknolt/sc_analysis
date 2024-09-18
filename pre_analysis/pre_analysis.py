import os
import subprocess
from enum import Enum
from pathlib import Path
from typing import List, Dict

import pandas as pd

from common_log import create_logger
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

class RFigure():

    def __init__(self, process: 'OneStepSequence', script_name: str):
        self.logger = create_logger(self)
        self.process = process
        self.script_name = script_name

    def output_file_name(self) -> str:
        return f"{self.process.result_id}.jpg"

    def extra_args(self) -> List[str]:
        return [self.process.from_event.name]

    def export(self):

        self.process.to_csv()

        extra = str.join(',', self.extra_args())

        script_r = Path(f"..\..\scripts_R\{self.script_name}.R")
        output_file = Configuration().result_dir / self.output_file_name()

        p = subprocess.Popen(
            ["Rscript", "--vanilla",
             script_r,
             self.process.csv_output().absolute(),
             output_file.absolute(),
             extra],
            cwd=os.getcwd(),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        output, error = p.communicate()

        if p.returncode == 0:
            print('R OUTPUT:\n {0}'.format(output.decode("utf-8")))
        else:
            print('R OUTPUT:\n {0}'.format(output.decode("utf-8")))
            print('R ERROR:\n {0}'.format(error.decode("utf-8")))


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

    def export_figure(self):
        figure = RFigure(process=self, script_name="ND_LP_camembert")
        figure.export()

    @property
    def xp_name(self) -> str:
        return self.batch.xp_name