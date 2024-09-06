import datetime
import os
from abc import abstractmethod
from datetime import timedelta
from io import StringIO
from pathlib import Path
from shutil import rmtree
from typing import List, Dict, Tuple

from common import FileMerger
from common_log import create_logger
from configuration import Configuration
import numpy as np

import pandas as pd

class Cachable:

    def __init__(self):
        self.logger = create_logger(self)
        self._df: pd.DataFrame = None

    def compute(self, force_recompute: bool = False):

        base_dir = Configuration().get_base_dir()
        cache_dir = base_dir / "cache" / self.xp_name
        cache_file = cache_dir / f"{self.result_id}.csv"

        if not cache_dir.exists():
            cache_dir.mkdir(parents=True)

        if cache_file.exists() and not force_recompute:
            df = pd.read_csv(cache_file, dtype=self.dtype, index_col=0)
            self._df = df
        else:

            df = self._compute()
            self._df = df
            self.save()
            # df.to_csv(cache_file)

        self.initialize()

    def save(self):
        base_dir = Configuration().get_base_dir()
        cache_dir = base_dir / "cache" / self.xp_name
        cache_file = cache_dir / f"{self.result_id}.csv"

        self._df.to_csv(cache_file)


    @property
    @abstractmethod
    def result_id(self) -> str:
        pass

    @abstractmethod
    def _compute(self) -> pd.DataFrame:
        pass

    @property
    @abstractmethod
    def xp_name(self) -> str:
        pass

    @property
    def dtype(self) -> Dict:
        return {}

    def initialize(self):
        pass

    @staticmethod
    def delete_cache(xp_name: str):
        base_dir = Configuration().get_base_dir()
        cache_dir = base_dir / "cache" / xp_name

        rmtree(cache_dir)




class TransitionResolver:

    def __init__(self, experiment: 'Experiment'):
        self.logger = create_logger(self)
        self.experiment = experiment

        self._trans_df = experiment.transitions()
        self._mice_occupation = experiment.mice_location

    def resolve(self, time: datetime.datetime):

        df = self._trans_df
        error_trans = df[df['time'] == time]

        error_row = error_trans.iloc[0]
        from_dest = error_row.from_loc

        # get mice in from_dest when error is detected
        res_occup = self._mice_occupation.get_occupation(time)

        mouse_in_from_loc = list()

        for key, value in res_occup.items():
            if key == 'time':
                continue

            if value == from_dest:
                mouse_in_from_loc.append(key)

        for mouse in mouse_in_from_loc:
            # get next transition
            df_mouse = df[df['rfid'] == mouse]
            # next trans
            next_trans_df = df_mouse[df_mouse['time'] > time]

            if next_trans_df.empty:
                continue

            next_trans_row = next_trans_df.iloc[0]

            if next_trans_row.error == "ERROR":

                self.logger.error(f"At time {time} mouse {error_row.rfid} is swapped with mouse {next_trans_row.rfid}")
                self.experiment.df.loc[error_row.name, 'rfid'] = next_trans_row.rfid
                self.experiment.df.loc[error_row.name, 'error'] = "SWAP"
                self.experiment.df.loc[next_trans_row.name, 'error'] = "SWAP"
                break


        # print("ENDED")

class Experiment(Cachable):


    def __init__(self, xp_name: str):
        super().__init__()
        self._xp_name = xp_name

        self.mice: List[str] = None
        self.mice_location: MiceLocation = None
        self.mice_sequence: MiceSequence = None

    @staticmethod
    def load(xp_name: str, delete_cache: bool = False) -> 'Experiment':

        if delete_cache:
            Cachable.delete_cache(xp_name)

        res = Experiment(xp_name=xp_name)
        res.compute()

        return res

    def lever_press(self) -> pd.DataFrame:
        df = self._df
        filtered = df[df['action'] == 'id_lever']

        return filtered

    def transitions_error(self) -> pd.DataFrame:
        df = self.transitions()
        filtered = df[df['error'] == 'ERROR']
        return filtered

    def transitions(self) -> pd.DataFrame:
        df = self._df
        filtered = df[df['action'] == 'transition']

        return filtered

    @property
    def df(self) -> pd.DataFrame:
        return self._df

    @property
    def result_id(self) -> str:
        return f"{self.xp_name}_events"

    def _compute(self) -> pd.DataFrame:

        base_dir = Configuration().get_base_dir()
        data_dir = base_dir / "data" / self.xp_name

        csv_file = list(data_dir.glob("*.csv"))
        csv_file.sort(key=os.path.getmtime)
        file_merger = FileMerger(csv_file)
        csv_str = file_merger.merge()

        cols = [
            'action',
            'device',
            'time',
            'rfid',
            'from_loc',
            'to_loc',
            'weight',
            'error',
            'direction',
            'activate',
            'liquids'
        ]

        dtype = {
            'rfid': str,
            'error': str
        }

        # df = pd.read_csv(csv_file, dtype=dtype, sep=";", names=cols, header=None)
        df = pd.read_csv(StringIO(csv_str), dtype=dtype, sep=";", names=cols, header=None)

        # format have changed btw experiment, we have to check the dateformat
        old_date_format = '%d-%m-%Y %H:%M:%S'

        try:
            date = pd.to_datetime(df['time'], format=old_date_format)
        except ValueError as error:
            date = pd.to_datetime(df['time'])

        df['time'] = date
        df.sort_values(by='time', inplace=True)

        self._add_days(df)

        return df


    def _add_days(self, df: pd.DataFrame):

        # extract first row to get the first datetime
        self.start_time = df.iloc[0].time
        # # extract last row to get the last datetime
        # self.end_time = df.iloc[-1].time

        h_day_start: int = 19

        def get_num_day(row: pd.Series):

            delta_day: timedelta = row.time - self.start_time
            num_day = delta_day.days if row.time.hour < h_day_start else delta_day.days +1

            return num_day

        df["day_since_start"] = df.apply(get_num_day, axis=1)

    @property
    def xp_name(self) -> str:
        return self._xp_name

    def initialize(self):

        # extract mice ids
        id_mice: set = set(self._df.rfid.unique())

        id_mice = id_mice - {np.nan, '0', 'na'}
        self.mice = id_mice

        # sort by time
        self.df.sort_values(by='time', inplace=True)
        self.df['time'] = pd.to_datetime(self.df['time'])

        mo = MiceLocation(experiment=self)
        mo.compute()
        self.mice_location = mo

        self.validate()

        ms = MiceSequence(experiment=self)
        ms.compute()
        self.mice_sequence = ms


    def validate(self) -> bool:

        df = self.transitions_error()

        if df.empty:
            return True
        else:
            resolver = TransitionResolver(self)
            for row in df.itertuples():
                time = row.time
                resolver.resolve(time)

            # df could have changed after resolve
            df = self.transitions_error()

            self.logger.error(f"{len(df)} transition errors found")
            for index, row in df.iterrows():
                prev_loc = self.mice_location.get_mouse_location(time=row.time, mouse=row.rfid, just_before=True)

                self.df.loc[index, 'error'] = ''
                new_row = row.copy()
                new_row.device = 'correction'
                new_row.from_loc = row.to_loc
                new_row.to_loc = row.from_loc
                new_row.error = 'CORRECTED'

                self.df.loc[len(self.df)] = new_row
                self.logger.error(f"RFID {row.rfid} date:{row.time} from:{row.from_loc} to:{row.to_loc} previously:{prev_loc}")

            self.df.sort_values(by='time', inplace=True, ignore_index=True)

            # save after corrections
            self.save()

            # recompute mice occupation
            self.mice_location.compute(force_recompute=True)

            return False

    @property
    def dtype(self) -> Dict:
        return {
            'rfid': str,
            'error': str
        }


class MiceLocation(Cachable):

    def __init__(self, experiment: Experiment):

        super().__init__()
        self.experiment = experiment
        self.mice_occupation: MiceOccupation = None

    @property
    def result_id(self) -> str:
        return f"{self.xp_name}_location"


    def _compute(self) -> pd.DataFrame:

        df = self.experiment.df

        transitions_df = df[df['action'] == 'transition'] #.reset_index(drop=True)

        mice = dict.fromkeys(transitions_df.rfid.unique(), "BLACK_BOX")

        res_occup_list: List = list()
        row_num = 0
        for idx, row in transitions_df.iterrows():

            rfid = row.rfid
            from_loc = row.from_loc
            to_loc = row.to_loc

            prev_loc = mice[rfid]

            if from_loc != prev_loc:

                df.loc[idx, 'error'] = "ERROR"
            else:
                df.loc[idx, 'error'] = ""

            # Last transition is considered as the new location even if there is an error
            mice[rfid] = to_loc

            # compute duration with next row

            if (row_num+1) < len(transitions_df):
                next_row_time = transitions_df.iloc[row_num+1].time
                duration = (next_row_time - row.time).total_seconds()
            else:
                duration = 0

            row_num += 1

            row_occup_dict = {'time': row.time, 'day_since_start': row.day_since_start, 'duration': duration, **mice}
            res_occup_list.append(row_occup_dict)

        df_occupation = pd.DataFrame(res_occup_list)

        # df of experimentation has been modified, need to save the new datas
        self.experiment.save()

        return df_occupation

    @property
    def xp_name(self) -> str:
        return self.experiment.xp_name

    def initialize(self):
        self._df['time'] = pd.to_datetime(self._df['time'])

        mice_occupation = MiceOccupation(mice_location=self)
        mice_occupation.compute()

        self.mice_occupation = mice_occupation

    def get_mouse_location(self, time: datetime, mouse: str, just_before: bool = False) -> str:

        num_tail = 1

        df = self._df

        if just_before:
            num_tail = 2

        res = df.loc[df['time'] <= time].tail(num_tail).to_dict(orient='records')[0]

        return res[mouse]

    def get_occupation(self, time: datetime) -> Dict:

        df = self._df

        res = df.loc[df['time'] <= time].tail(1).to_dict(orient='records')[0]

        return res

class MiceOccupation(Cachable):

    def __init__(self, mice_location: MiceLocation):
        super().__init__()

        self.mice_location = mice_location

    # def initialize(self):
    #     self._df['mice_comb'] = self._df['mice_comb'].astype(str)

    @property
    def dtype(self) -> Dict:
        return {
            'mice_comb': 'string',
            'duration': int
        }

    @property
    def result_id(self) -> str:
        return f"{self.xp_name}_occupation_LMT"

    def _compute(self) -> pd.DataFrame:

        df = self.mice_location._df
        mice = list(df.columns[3:])

        res_per_day: Dict[int, Dict[str, float]] = dict()

        for day, day_data in df.groupby('day_since_start'):

            res_comb: Dict[str, float] = dict()
            for index, row in day_data.iterrows():
                mice_in_lmt = [x for x in mice if row[x] == "LMT"]

                mice_key = '|'.join(mice_in_lmt) if len(mice_in_lmt) else 'EMPTY'

                if mice_key not in res_comb:
                    res_comb[mice_key] = row.duration
                else:
                    res_comb[mice_key] += row.duration

            res_per_day[day] = res_comb

        # refactor dictionary
        final_res = list()

        for day, values in res_per_day.items():
            for mice_comb, duration in values.items():
                final_res.append({
                    'mice_comb': str(mice_comb),
                    'duration': duration,
                    'day_since_start': day
                })

        df = pd.DataFrame(final_res)

        # add extra info, the number of mice in the LMT
        df['nb_mice'] = df.apply(lambda x: 0 if x['mice_comb'] == 'EMPTY' else len(x['mice_comb'].split('|')), axis=1)

        return df

    @property
    def xp_name(self) -> str:
        return self.mice_location.xp_name

class MiceSequence(Cachable):

    def __init__(self, experiment: Experiment):
        super().__init__()

        self.experiment = experiment

    @property
    def result_id(self) -> str:
        return f"{self.xp_name}_sequences"

    def _compute(self) -> pd.DataFrame:

        res_global: List = list()

        max_delay = Configuration().max_delay_complete_sequence

        res_sequence: Dict = None

        df = self.experiment.df
        df = df[df['action'].str.contains('id_lever|nose_poke')]

        for row in df.itertuples():

            if row.action == "id_lever":

                if res_sequence:
                    res_sequence['rfid_np'] = ''
                    res_global.append(res_sequence)

                res_sequence = dict()

                res_sequence['lever_press_dt'] = row.time
                res_sequence['rfid_lp'] = str(row.rfid)
                res_sequence['day_since_start'] = row.day_since_start

            elif row.action == "nose_poke":

                if not res_sequence:
                    continue

                elapsed_time: float = (row.time - res_sequence['lever_press_dt']).total_seconds()
                res_sequence['elapsed_s'] = elapsed_time
                res_sequence['rfid_np'] = row.rfid

                res_global.append(res_sequence)
                res_sequence = None

        res_df = pd.DataFrame(res_global)

        res_df['complete_sequence'] = res_df.apply(lambda row: (row.rfid_lp == row.rfid_np) and row.elapsed_s <= max_delay, axis=1)

        return res_df

    @property
    def xp_name(self) -> str:
        return self.experiment.xp_name

# def __init__(self, mice_cycle: pd.DataFrame):
    #     self.df: pd.DataFrame = mice_cycle
    #
    #
    # @staticmethod
    # def create_from_events(data_frame: pd.DataFrame) -> 'MiceCycle':
    #
    #     mouse_id: str = None
    #     lever_pressed_time: datetime = None
    #
    #     res_global: List = list()
    #
    #     for row in data_frame.itertuples():
    #
    #         if row.action == "id_lever":
    #
    #             lever_pressed_time = row.time
    #             mouse_id = row.rfid
    #
    #         elif row.action == "nose_poke" and mouse_id:
    #
    #             if row.rfid == mouse_id and lever_pressed_time:
    #
    #                 elapsed_time: float = (row.time - lever_pressed_time).total_seconds()
    #                 res_cycle = {
    #                     'lever_press_dt': lever_pressed_time,
    #                     'rfid': mouse_id,
    #                     'elapsed_s': elapsed_time,
    #                     'valid_cycle': 'TRUE' if elapsed_time <= 3 else "FALSE",
    #                     'day_since_start': row.day_since_start
    #                 }
    #
    #                 res_global.append(res_cycle)
    #
    #                 mouse_id = None
    #
    #     res_df = pd.DataFrame(res_global)
    #
    #     return MiceCycle(mice_cycle=res_df)
    #
    #
