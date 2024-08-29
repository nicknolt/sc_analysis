import datetime
from abc import abstractmethod
from datetime import timedelta
from pathlib import Path
from shutil import rmtree
from typing import List, Dict

from common_log import create_logger
from configuration import Configuration


import pandas as pd

class Cachable:

    def __init__(self):
        self.logger = create_logger(self)
        self._df: pd.DataFrame = None

    def compute(self):

        base_dir = Configuration().get_base_dir()
        cache_dir = base_dir / "cache" / self.xp_name
        cache_file = cache_dir / f"{self.result_id}.csv"

        if not cache_dir.exists():
            cache_dir.mkdir(parents=True)

        if cache_file.exists():
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
        self._mice_occupation = experiment.mice_occupation

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
                break


class Experiment(Cachable):


    def __init__(self, xp_name: str):
        super().__init__()
        self._xp_name = xp_name

        self.mice_occupation: MiceOccupation = None

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

        base_dir = Configuration().get_base_dir() / self.xp_name

        csv_file = list(base_dir.glob("*.csv"))[0]

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
            'activate'
        ]

        dtype = {
            'rfid': str,
            'error': str
        }

        df = pd.read_csv(csv_file, dtype=dtype, sep=";", names=cols, header=None)

        date_format = '%d-%m-%Y %H:%M:%S'
        df['time'] = pd.to_datetime(df['time'], format=date_format)
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

        # sort by time
        self.df.sort_values(by='time')
        self.df['time'] = pd.to_datetime(self.df['time'])

        mo = MiceOccupation(experiment=self)
        mo.compute()
        self.mice_occupation = mo

        self.validate()

    def validate(self) -> bool:

        df = self.transitions_error()

        if df.empty:
            return True
        else:
            self.logger.error(f"{len(df)} transition errors found")
            for row in df.itertuples():
                prev_loc = self.mice_occupation.get_mouse_location(time=row.time, mouse=row.rfid, just_before=True)
                self.logger.error(f"RFID {row.rfid} date:{row.time} from:{row.from_loc} to:{row.to_loc} previously:{prev_loc}")

            resolver = TransitionResolver(self)
            for row in df.itertuples():
                time = row.time
                resolver.resolve(time)


            return False

    @property
    def dtype(self) -> Dict:
        return {
            'rfid': str,
            'error': str
        }


class MiceOccupation(Cachable):

    def __init__(self, experiment: Experiment):

        super().__init__()
        self.experiment = experiment

    @property
    def result_id(self) -> str:
        return f"{self.xp_name}_occupation"

    def _compute(self) -> pd.DataFrame:

        df = self.experiment.df

        transitions_df = df[df['action'] == 'transition'] #.reset_index(drop=True)

        mice = dict.fromkeys(transitions_df.rfid.unique(), "BLACK_BOX")

        res_occup_list: List = list()

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

            if (idx+1) < len(transitions_df):
                next_row_time = transitions_df.iloc[idx+1].time
                duration = (next_row_time - row.time).total_seconds()
            else:
                duration = 0

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



