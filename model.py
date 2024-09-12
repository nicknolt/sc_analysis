import datetime
import os
from abc import abstractmethod
from datetime import timedelta
from io import StringIO
from pathlib import Path
from shutil import rmtree
from typing import List, Dict, Tuple

from pandas import Timestamp

from common import FileMerger
from common_log import create_logger
from configuration import Configuration
import numpy as np

import pandas as pd

class Cachable:

    def __init__(self):
        self.logger = create_logger(self)
        self._df: pd.DataFrame = None

    def compute(self, force_recompute: bool = False) -> pd.DataFrame:

        base_dir = Configuration().get_base_dir()
        cache_dir = base_dir / "cache" / self.xp_name

        cache_file = cache_dir / f"{self.result_id}.csv"

        self.logger.info(f"Search result for {self.result_id}")
        if not cache_dir.exists():
            cache_dir.mkdir(parents=True)

        if cache_file.exists() and not force_recompute:
            self.logger.info(f"'{self.result_id}' is loaded from cache")
            df = pd.read_csv(cache_file, dtype=self.dtype, index_col=0)
            self._df = df
        else:

            self.logger.info(f"Compute {self.result_id}")
            df = self._compute()
            self.logger.info(f"End Compute {self.result_id}")
            self._df = df
            self.save()
            # df.to_csv(cache_file)

        self.initialize()

        return df

    def save(self):
        base_dir = Configuration().get_base_dir()
        cache_dir = base_dir / "cache" / self.xp_name
        cache_file = cache_dir / f"{self.result_id}.csv"

        self._df.to_csv(cache_file)
    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            self._df = self.compute()

        return self._df

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
        return {
            'rfid_lp': 'string',
            'rfid_np': 'string'
        }

    def initialize(self):
        pass

    # @staticmethod
    # def delete_cache(xp_name: str):
    #     base_dir = Configuration().get_base_dir()
    #     cache_dir = base_dir / "cache" / xp_name
    #
    #     rmtree(cache_dir)




class TransitionResolver:

    def __init__(self, experiment: 'Batch'):
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
        res_occup = self._mice_occupation.get_mice_location(time)

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
class Experiment:

    def __init__(self):
        self.logger = create_logger(self)

    @property
    def batches(self) -> List[str]:

        base_dir = Configuration().get_base_dir()
        data_dir = base_dir / "data"

        batch_names = [d.name for d in data_dir.glob('*/') if d.is_dir()]

        return batch_names

    def get_percentage_lever_pressed(self) -> 'GlobalPercentageLeverPressed':
        return GlobalPercentageLeverPressed(self)

    def get_percentage_complete_sequence(self) -> 'GlobalPercentageCompleteSequence':
        return GlobalPercentageCompleteSequence(self)

class GlobalPercentageLeverPressed(Cachable):

    def __init__(self, experiment: Experiment):
        super().__init__()
        self.experiment = experiment

    @property
    def result_id(self) -> str:
        return f"global_percentage_lever_pressed"

    def _compute(self) -> pd.DataFrame:

        batche_names = self.experiment.batches

        list_df: List[pd.DataFrame] = list()

        for batch_name in batche_names:
            batch = Batch.load(batch_name)
            df = batch.get_percentage_lever_pressed().df
            df['batch'] = batch_name

            list_df.append(df)

        df_merged = pd.concat(list_df)

        return df_merged
    @property
    def xp_name(self) -> str:
        return "Global"

    @property
    def dtype(self) -> Dict:
        return {
            'rfid': str
        }

class GlobalPercentageCompleteSequence(Cachable):

    def __init__(self, experiment: Experiment):
        super().__init__()
        self.experiment = experiment

    @property
    def result_id(self) -> str:
        return f"global_complete_sequence"

    def _compute(self) -> pd.DataFrame:

        batche_names = self.experiment.batches

        list_df: List[pd.DataFrame] = list()

        for batch_name in batche_names:
            batch = Batch.load(batch_name)
            df = batch.get_percentage_complete_sequence().df
            df['batch'] = batch_name

            list_df.append(df)

        df_merged = pd.concat(list_df)

        return df_merged
    @property
    def xp_name(self) -> str:
        return "Global"

    @property
    def dtype(self) -> Dict:
        return {
            'rfid': str
        }

class PercentageLeverPressed(Cachable):

    def __init__(self, batch: 'Batch'):
        super().__init__()
        self.batch = batch

    @property
    def result_id(self) -> str:
        return f"{self.xp_name}_percentage_lever_pressed"

    def _compute(self) -> pd.DataFrame:

        df = self.batch.lever_press()

        df = df.groupby(['day_since_start', 'rfid']).size().reset_index(name='nb_lever_press')
        df["total_per_day"] = df.groupby('day_since_start')['nb_lever_press'].transform('sum')
        df["percent_pressed"] = (df['nb_lever_press'] / df["total_per_day"])*100

        return df

    @property
    def dtype(self) -> Dict:
        return {
            'rfid': str
        }

    @property
    def xp_name(self) -> str:
        return f"{self.batch.xp_name}"

class PercentageCompleteSequence(Cachable):

    def __init__(self, batch: 'Batch'):
        super().__init__()
        self.batch = batch

    @property
    def result_id(self) -> str:
        return f"{self.xp_name}_percentage_complete_sequence"

    def _compute(self) -> pd.DataFrame:

        df_lever = self.batch.get_percentage_lever_pressed().df
        df_lever = df_lever.groupby('day_since_start')['total_per_day'].first().reset_index()

        df = self.batch.mice_sequence.df
        df = df[df['complete_sequence']]

        df = df.groupby(['day_since_start', 'rfid_lp']).size().reset_index(name='nb_complete_sequence')
        df = df.merge(df_lever[['day_since_start', 'total_per_day']], how='left', on='day_since_start')
        df["percent_complete_sequence"] = (df['nb_complete_sequence'] / df["total_per_day"])*100

        return df
    @property
    def xp_name(self) -> str:
        return self.batch.xp_name

    @property
    def dtype(self) -> Dict:
        return {
            'rfid': str
        }


class Batch(Cachable):

    def __init__(self, xp_name: str):
        super().__init__()
        self._xp_name = xp_name

        self.mice: List[str] = None
        self.mice_location: MiceLocation = None
        self.mice_sequence: MiceSequence = None

    @staticmethod
    def load(xp_name: str) -> 'Batch':

        res = Batch(xp_name=xp_name)
        res.compute()

        return res

    def get_mice_occupation(self, location: str) -> 'MiceOccupation':
        return MiceOccupation(self.mice_location, location=location)

    def get_percentage_lever_pressed(self) -> PercentageLeverPressed:
        return PercentageLeverPressed(self)

    def get_percentage_complete_sequence(self):
        return PercentageCompleteSequence(self)

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
            # mixed instead of "%Y-%m-%dT%H:%M:%S.%f%z" or "ISO8601" because when ms is .000 pandas remove them and when saved in csv the format is not the same
            # and raise an error
            date = pd.to_datetime(df['time'], format="mixed")

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
        date_day_start = self.start_time.date()

        def get_num_day(row: pd.Series):

            delta_day: timedelta = (row.time.date() - date_day_start).days
            num_day = delta_day if row.time.hour < h_day_start else delta_day +1

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
        self.df['time'] = pd.to_datetime(self.df['time'], format='mixed')
        self.df.sort_values(by='time', inplace=True)

        mo = MiceLocation(batch=self)
        mo.compute()
        self.mice_location = mo

        self.validate()

        ms = MiceSequence(batch=self)
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

    def __init__(self, batch: Batch):

        super().__init__()
        self.batch = batch
        self.mice_occupation: MiceOccupation = None

    @property
    def result_id(self) -> str:
        return f"{self.xp_name}_location"


    def _compute(self) -> pd.DataFrame:

        df = self.batch.df

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
        self.batch.save()

        return df_occupation

    @property
    def xp_name(self) -> str:
        return self.batch.xp_name

    def initialize(self):
        self._df['time'] = pd.to_datetime(self._df['time'], format='mixed')

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

    def get_mice_location(self, time: datetime) -> Dict:

        df = self._df

        res = None

        first_record = df.loc[df['time'] <= time].tail(1)
        if len(first_record):
            res = first_record.to_dict(orient='records')[0]

        return res

    def get_nb_mice_in_location(self, location: str, time: datetime) -> int:
        mice_loc = self.get_mice_location(time)

        if not mice_loc:
            return None

        res = [loc for loc in mice_loc.values() if loc == location]

        return len(res)

class MiceOccupation(Cachable):

    def __init__(self, mice_location: MiceLocation, location: str = "LMT"):
        super().__init__()

        self.mice_location = mice_location
        self.location = location

    # def initialize(self):
    #     self._df['mice_comb'] = self._df['mice_comb'].astype(str)

    @property
    def dtype(self) -> Dict:
        return {
            'mice_comb': 'string',
            'duration': 'float'
        }

    @property
    def result_id(self) -> str:
        return f"{self.xp_name}_occupation_{self.location}"

    def _compute(self) -> pd.DataFrame:

        df = self.mice_location._df
        mice = list(df.columns[3:])

        res_per_day: Dict[int, Dict[str, float]] = dict()

        for day, day_data in df.groupby('day_since_start'):

            res_comb: Dict[str, float] = dict()
            for index, row in day_data.iterrows():
                mice_in_lmt = [x for x in mice if row[x] == self.location]

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
                    'duration': int(duration),
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

    def __init__(self, batch: Batch):
        super().__init__()

        self.batch = batch

    @property
    def result_id(self) -> str:
        max_delay = Configuration().max_delay_complete_sequence
        return f"{self.xp_name}_{max_delay}_sequences"

    def _compute(self) -> pd.DataFrame:

        res_global: List = list()

        max_delay = Configuration().max_delay_complete_sequence

        res_sequence: Dict = None

        # all events
        df = self.batch.df
        df = df[df['action'].str.contains('id_lever|nose_poke')]

        # mice location
        mice_loc = self.batch.mice_location

        for row in df.itertuples():

            if row.action == "id_lever":

                if res_sequence:
                    res_sequence['rfid_np'] = ''
                    res_global.append(res_sequence)

                res_sequence = dict()

                res_sequence['lever_press_dt'] = row.time
                res_sequence['nb_mice_lp'] = mice_loc.get_nb_mice_in_location(location="LMT", time=row.time)
                res_sequence['rfid_lp'] = str(row.rfid)
                res_sequence['day_since_start'] = row.day_since_start

            elif row.action == "nose_poke":

                if not res_sequence:
                    continue

                elapsed_time: float = (row.time - res_sequence['lever_press_dt']).total_seconds()
                res_sequence['elapsed_s'] = elapsed_time
                res_sequence['rfid_np'] = row.rfid
                res_sequence['nb_mice_np'] = mice_loc.get_nb_mice_in_location(location="LMT", time=row.time)

                res_global.append(res_sequence)
                res_sequence = None

        res_df = pd.DataFrame(res_global)

        res_df['complete_sequence'] = res_df.apply(lambda row: (row.rfid_lp == row.rfid_np) and row.elapsed_s <= max_delay, axis=1)

        return res_df

    @property
    def xp_name(self) -> str:
        return self.batch.xp_name

    @property
    def dtype(self) -> Dict:
        return {
            'nb_mice_lp': 'Int64',
            'nb_mice_np': 'Int64'
        }


class OccupationTime(Cachable):

    def __init__(self, experiment: Batch):
        super().__init__()
        self.experiment = experiment

    @property
    def result_id(self) -> str:
        return f"{self.xp_name}_occupation_time"

    def _compute(self) -> pd.DataFrame:

        xp = self.experiment
        df = xp.mice_location.mice_occupation._df

        to_concat: List[pd.DataFrame] = list()

        for mouse in [*xp.mice, 'EMPTY']:
            df_mouse = df[df['mice_comb'].str.contains(mouse)]
            tmp_df = df_mouse.groupby(['day_since_start', 'nb_mice'])['duration'].sum().reset_index()
            tmp_df['mouse'] = mouse
            to_concat.append(tmp_df)

        merged = pd.concat(to_concat).sort_values(['day_since_start', 'mouse']).reset_index(drop=True)

        return merged

    @property
    def xp_name(self) -> str:
        return self.experiment.xp_name

