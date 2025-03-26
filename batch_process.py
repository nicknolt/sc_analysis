import datetime
from datetime import timedelta
from io import StringIO
from pathlib import Path
from typing import List, Dict

import numpy as np
import pandas as pd
import pytz
from dependency_injector.wiring import inject, Provide
from pandas.core import series
from pandas.core.groupby import GroupBy

from common_log import create_logger
from container import Container
from data_service import DataService
from lmt.lmt2batch_link_process import LMT2BatchLinkProcess
from lmt.lmt_db_reader import LMTDBReader, LMTDBException
from lmt.lmt_service import LMTService
from process import BatchProcess


class TransitionResolver:

    def __init__(self, process: BatchProcess):
        self.logger = create_logger(self)
        self.process = process

        # self._trans_df = experiment.transitions()
        # self._mice_occupation = experiment.mice_location

    def resolve(self):

        df = self.process.df
        df = df[df['error'] == 'ERROR']

        for idx, error_row in df.iterrows():
            self._resolve(time=error_row.time)

    def _resolve(self, time: datetime.datetime):

        df = self.process.df
        df = df[df['action'] == 'transition']

        # resolve mouse swap (when mice are close to the antenna and a 'wrong' mouse as been identified instead of the real one)
        # df = self._trans_df
        error_trans = df[df['time'] == time]

        error_row = error_trans.iloc[0]
        from_dest = error_row.from_loc

        # get mice in from_dest when error is detected
        mice_location = MiceLocation(batch_name=self.process.batch_name)

        res_occup = mice_location.get_mice_location(time)
        mouse_in_from_loc = [key for key, value in res_occup.items() if value == from_dest]

        # we are looking for a transition ERROR in the candidate (mice in from dest at the moment of the transition error)
        for mouse in mouse_in_from_loc:
            # filter by candidate mouse
            df_mouse = df[df['rfid'] == mouse]
            # get the next transition for this mouse
            next_trans_df = df_mouse[df_mouse['time'] > time]

            if next_trans_df.empty:
                continue

            next_trans_row = next_trans_df.iloc[0]

            # if this is an error, it means that it should be an id swap, we could resolve it and mark
            # the transition error as SWAP
            if next_trans_row.error == "ERROR":

                self.logger.error(f"At time {time} mouse {error_row.rfid} is swapped with mouse {next_trans_row.rfid}")
                self.process.df.loc[error_row.name, 'rfid'] = next_trans_row.rfid
                self.process.df.loc[error_row.name, 'error'] = "SWAP"
                self.process.df.loc[next_trans_row.name, 'error'] = "SWAP"
                break


        # print("ENDED")

# class TransitionResolver:
#
#     def __init__(self, experiment: 'TemporaryImportBatch'):
#         self.logger = create_logger(self)
#         self.experiment = experiment
#
#         self._trans_df = experiment.transitions()
#         self._mice_occupation = experiment.mice_location
#
#     def resolve(self):
#
#         df = self.experiment.transitions_error()
#
#         for idx, error_row in df.iterrows():
#             self._resolve(time=error_row.time)
#
#     def _resolve(self, time: datetime.datetime):
#
#         # resolve mouse swap (when mice are close to the antenna and a 'wrong' mouse as been identified instead of the real one)
#         df = self._trans_df
#         error_trans = df[df['time'] == time]
#
#         error_row = error_trans.iloc[0]
#         from_dest = error_row.from_loc
#
#         # get mice in from_dest when error is detected
#         res_occup = self._mice_occupation.get_mice_location(time)
#         mouse_in_from_loc = [key for key, value in res_occup.items() if value == from_dest]
#
#         # we are looking for a transition ERROR in the candidate (mice in from dest at the moment of the transition error)
#         for mouse in mouse_in_from_loc:
#             # filter by candidate mouse
#             df_mouse = df[df['rfid'] == mouse]
#             # get the next transition for this mouse
#             next_trans_df = df_mouse[df_mouse['time'] > time]
#
#             if next_trans_df.empty:
#                 continue
#
#             next_trans_row = next_trans_df.iloc[0]
#
#             # if this is an error, it means that it should be an id swap, we could resolve it and mark
#             # the transition error as SWAP
#             if next_trans_row.error == "ERROR":
#
#                 self.logger.error(f"At time {time} mouse {error_row.rfid} is swapped with mouse {next_trans_row.rfid}")
#                 self.experiment.df.loc[error_row.name, 'rfid'] = next_trans_row.rfid
#                 self.experiment.df.loc[error_row.name, 'error'] = "SWAP"
#                 self.experiment.df.loc[next_trans_row.name, 'error'] = "SWAP"
#                 break
#
#
#         # print("ENDED")
# class Experiment:
#
#     def __init__(self):
#         self.logger = create_logger(self)
#
#     @property
#     def batches(self) -> List[str]:
#
#         base_dir = Configuration().get_base_dir()
#         data_dir = base_dir / "data"
#
#         batch_names = [d.name for d in data_dir.glob('*/') if d.is_dir()]
#
#         return batch_names
#
#     def get_percentage_lever_pressed(self) -> 'GlobalPercentageLeverPressed':
#         return GlobalPercentageLeverPressed(self)
#
#     def get_percentage_complete_sequence(self) -> 'GlobalPercentageCompleteSequence':
#         return GlobalPercentageCompleteSequence(self)

class GlobalPercentageLeverPressed(BatchProcess):

    def __init__(self, experiment: 'Experiment'):
        super().__init__()
        self.experiment = experiment

    @property
    def result_id(self) -> str:
        return f"global_percentage_lever_pressed"

    def _compute(self) -> pd.DataFrame:

        batche_names = self.experiment.batches

        list_df: List[pd.DataFrame] = list()

        for batch_name in batche_names:
            batch = ImportBatch.load(batch_name)
            df = batch.get_percentage_lever_pressed().df
            df['batch'] = batch_name

            list_df.append(df)

        df_merged = pd.concat(list_df)

        return df_merged
    @property
    def batch_name(self) -> str:
        return "Global"

    @property
    def dtype(self) -> Dict:
        return {
            'rfid': str
        }

class GlobalPercentageCompleteSequence(BatchProcess):

    def __init__(self, experiment: 'Experiment'):
        super().__init__()
        self.experiment = experiment

    @property
    def result_id(self) -> str:
        return f"global_complete_sequence"

    def _compute(self) -> pd.DataFrame:

        batche_names = self.experiment.batches

        list_df: List[pd.DataFrame] = list()

        for batch_name in batche_names:
            batch = ImportBatch.load(batch_name)
            df = batch.get_percentage_complete_sequence().df
            df['batch'] = batch_name

            list_df.append(df)

        df_merged = pd.concat(list_df)

        return df_merged
    @property
    def batch_name(self) -> str:
        return "Global"

    @property
    def dtype(self) -> Dict:
        return {
            'rfid': str
        }

class PercentageLeverPressed(BatchProcess):

    def __init__(self, batch: 'ImportBatch'):
        super().__init__()
        self.batch = batch

    @property
    def result_id(self) -> str:
        return f"{self.batch_name}_percentage_lever_pressed"

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
    def batch_name(self) -> str:
        return f"{self.batch.batch_name}"

class PercentageCompleteSequence(BatchProcess):

    def __init__(self, batch: 'ImportBatch'):
        super().__init__()
        self.batch = batch

    @property
    def result_id(self) -> str:
        return f"{self.batch_name}_percentage_complete_sequence"

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
    def batch_name(self) -> str:
        return self.batch.batch_name

    @property
    def dtype(self) -> Dict:
        return {
            'rfid': str
        }

class DBEventInfo(BatchProcess):

    @property
    def result_id(self) -> str:
        return f"{self.batch_name}_db_event_info"

    @property
    def dtype(self) -> Dict:
        pass

    def _add_db_frame(self, df: pd.DataFrame, lmt_service: LMTService = Provide[Container.lmt_service]) -> pd.DataFrame:

        current_reader: LMTDBReader = None
        current_db_idx: int = None
        nb_elem = len(df)

        def get_db_idx(row: pd.Series):

            nonlocal current_reader, current_db_idx, nb_elem

            if (current_reader is None) or (not current_reader.is_date_inside(row['time'])):

                if current_reader:
                    current_reader.close()

                current_reader, current_db_idx = lmt_service.get_lmt_reader(self.batch_name, row['time'])

            if current_reader is None:
                num_frame = -1
                current_db_idx = -1
            else:
                num_frame = current_reader.get_corresponding_frame_number(row['time'], close_connexion=False)

            if row.name % 100 == 0:
                self.logger.debug(f"{row.name}/{nb_elem}")

            row["db_idx"] = current_db_idx
            row["db_frame"] = num_frame

            return row


        # df[["db_idx", "db_frame"]] = None #df_event.apply(get_db_idx, axis=1)
        res = df.apply(get_db_idx, axis=1)[["db_idx", "db_frame"]]

        return res


    def _compute(self) -> pd.DataFrame:

        p = ImportBatch(self.batch_name)
        df_event = p.df

        df_event = self._add_db_frame(df_event)

        return df_event

class ImportBatch(BatchProcess):

    @inject
    def __init__(self, batch_name: str, data_service: DataService = Provide[Container.data_service], lmt_service: LMTService = Provide[Container.lmt_service]):
        super().__init__(batch_name=batch_name)

        self._mice: List[str] = None
        self._mice_location: MiceLocation = None
        self.mice_sequence: MiceSequence = None
        self._data_service = data_service
        self._lmt_service = lmt_service


    @staticmethod
    def load(batch_name: str) -> 'ImportBatch':

        res = ImportBatch(batch_name=batch_name)
        res.compute()

        return res

    @property
    def mice(self) -> List[str]:

        if self._mice is None:
            # extract mice ids
            id_mice: set = set(self.df.rfid.unique())

            id_mice -= {np.nan, '0', 'na'}
            self._mice = id_mice

        return list(self._mice)

    @property
    def mice_location(self) -> 'MiceLocation':

        if self._mice_location is None:
            self._mice_location = MiceLocation(batch_name=self.batch_name)

        return self._mice_location

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
    def result_id(self) -> str:
        return f"{self.batch_name}_events"

    def _compute(self) -> pd.DataFrame:

        df = self._data_service.get_raw_df(batch_name=self.batch_name)

        self._find_transitions_error(df)

        # temporary save the process before correction
        self._df = df
        self.save()

        self._transition_error_correction(df)

        # add extra info
        self._add_transition_groups(self.df)
        self._add_days(self.df)

        # temp save another time to be used by mice location with updated data
        self.save()

        MiceLocation(batch_name=self.batch_name).compute(force_recompute=True)

        self._add_db_frame_info()
        self.save()

        df = self._add_lmt_loc()
        # self._add_lmt_loc()

        return df

    def _add_lmt_loc(self) -> pd.DataFrame:

        df = self._df.iloc[::100, :]

        df[['lmt_rfid', 'lmt_db_frame', 'lmt_date', 'db_error']] = None

        events = ["nose_poke", "id_lever"]
        location = None


        for event in events:

            cpt = 0

            self.logger.debug(f"LMT LOC for event : {event}")
            df_events = df[(df["action"] == event) & (df["db_idx"] != -1)]

            if event == "id_lever":
                location = self.parameters.lever_loc
            elif event == "nose_poke":
                location = self.parameters.feeder_loc

            groups = df_events.groupby("db_idx", sort=True)
            for id_db, rows in groups:

                cpt += 1
                self.logger.debug(f"DB {id_db} ({cpt}/{len(groups)})")
                date = rows.time.iloc[0]
                lmt_reader, db_idx = self._lmt_service.get_lmt_reader(self.batch_name, date)

                frame_number_list = rows["db_frame"].tolist()

                close_df = lmt_reader.get_closest_animal_batch(frame_numbers=frame_number_list,
                                                               location=location)


                close_df.index = rows.index


                # df[['lmt_rfid', 'lmt_db_frame', 'lmt_date']] = close_df[['lmt_rfid', 'lmt_db_frame', 'lmt_date']]
                df.update(close_df)

                print("ok")



        return df

    # def _add_lmt_loc(self) -> pd.DataFrame:
    #     df = self.df
    #
    #     # create these columns if not exists
    #     if 'db_error' not in df:
    #         df['db_error'] = None
    #         df['lmt_rfid'] = None
    #         df['lmt_db_frame'] = None
    #         df['lmt_date'] = None
    #
    #     df_db = LMT2BatchLinkProcess().df
    #     df_db = df_db[df_db.batch == self.batch_name]
    #
    #     def get_closest_animal(row: pd.Series, lmt_reader: LMTDBReader):
    #
    #         if row.action == 'id_lever':
    #             location = self.parameters.lever_loc
    #         elif row.action == 'nose_poke':
    #             location = self.parameters.feeder_loc
    #         else:
    #             return row
    #
    #         try:
    #             rfid, frame, ts = lmt_reader.get_closest_animal(frame_number=row['db_frame'], location=location, close_connection=False)
    #             row['lmt_rfid'] = rfid
    #             row['lmt_db_frame'] = frame
    #             row['lmt_date'] = datetime.datetime.fromtimestamp(ts).astimezone(tz=pytz.timezone("Europe/Paris"))
    #         except LMTDBException as e:
    #             err_msg = f"Unable to found closest animal for event: {row['action']} date: {row['time']} cause: {e}"
    #             self.logger.error(err_msg)
    #             row['db_error'] = e.error_type.name
    #
    #         return row
    #
    #     df.loc[df.db_idx == -1, 'db_error'] = "NO_DB"
    #
    #     for id_group, rows in df.groupby("db_idx"):
    #
    #         if id_group == -1:
    #             continue
    #
    #         db_file = df_db[df_db.db_idx == id_group].iloc[0].path
    #         lmt_reader = LMTDBReader(Path(db_file))
    #         df.update(rows.apply(get_closest_animal, lmt_reader=lmt_reader, axis=1))
    #
    #     return df

    def _add_db_frame_info(self):

        df_db = DBEventInfo(batch_name=self.batch_name).df
        self.df[["db_idx", "db_frame"]] = df_db[["db_idx", "db_frame"]]


    def _find_transitions_error(self, df: pd.DataFrame):

        # reset error to empty
        df.error = ''
        transitions_df = df[df['action'] == 'transition'] #.reset_index(drop=True)

        mice_list = transitions_df.rfid.unique()

        for mouse in mice_list:

            # detect error transitions
            tmp_df = transitions_df[transitions_df['rfid'] == mouse]
            tmp = tmp_df['to_loc'].shift(1)
            tmp.iloc[0] = "BLACK_BOX"

            # if the destination of the previous transition is different than the next transition origin
            # need to mark the column in ERROR
            idx_error = tmp_df[tmp_df['from_loc'] != tmp].index
            df.loc[idx_error, 'error'] = "ERROR"


    def _add_transition_groups(self, df: pd.DataFrame):

        last_num_group = 0

        def get_num_trans_group(row: pd.Series):

            nonlocal last_num_group

            res = last_num_group

            if row.action == 'transition':
                last_num_group += 1
                res = last_num_group

            return res

        df["trans_group"] = df.apply(get_num_trans_group, axis=1)

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


    def initialize(self):

        # sort by time
        self.df['time'] = pd.to_datetime(self.df['time'], format='mixed', utc=True)

        # mo = MiceLocation(batch=self)
        # mo.compute()
        # self._mice_location = mo


    # could be integrated into TrasistionResolver
    def _transition_error_correction(self, df: pd.DataFrame) -> bool:

        df = df[df['action'] == 'transition']
        df = df[df['error'] == 'ERROR']

        if df.empty:
            return False
        else:
            resolver = TransitionResolver(self)
            resolver.resolve()

            df = self.transitions_error()

            self.logger.error(f"{len(df)} transition errors found")
            for index, row in df.iterrows():
                prev_loc = self.mice_location.get_mouse_location(time=row.time, mouse=row.rfid, just_before=True)

                # we add 1ms before a "fake transition" from prev_loc to transition error from loc
                self.df.loc[index, 'error'] = ''
                new_row = row.copy()
                # susbstract 1 ms to keep transition consistent when sort by date
                new_row.time -= pd.Timedelta(milliseconds=1)
                new_row.device = 'correction'
                new_row.from_loc = prev_loc
                new_row.to_loc = row.from_loc
                new_row.error = 'CORRECTED'

                self.df.loc[len(self.df)] = new_row
                self.logger.error(
                    f"RFID {row.rfid} date:{row.time} from:{row.from_loc} to:{row.to_loc} previously:{prev_loc}")

            self._df.sort_values(by='time', inplace=True)
            self._df.reset_index(drop=True, inplace=True)

            return True
                # # save after corrections
                # self.save()


    @property
    def dtype(self) -> Dict:
        return {
            'rfid': str,
            'error': str,
        }


class MiceLocation(BatchProcess):

    def __init__(self, batch_name: str):

        super().__init__(batch_name)

        self.mice: List[str] = None

        # self.batch = batch
        # self.mice_occupation: MiceOccupation = None

    @property
    def result_id(self) -> str:
        return f"{self.batch_name}_location"

    @property
    def dtype(self) -> Dict:
        return {
            'duration': float
        }

    def _compute(self) -> pd.DataFrame:

        import_batch = ImportBatch(batch_name=self.batch_name)
        df = import_batch.df

        # df = TemporaryImportBatch(batch_name=self.batch_name).df

        self.mice = import_batch.mice

        transitions_df = df[df['action'] == 'transition'] #.reset_index(drop=True)

        # mice_list = transitions_df.rfid.unique()
        res_df = pd.DataFrame(columns=['time'])
        res_df[['day_since_start', 'time', 'trans_group']] = transitions_df[['day_since_start', 'time', 'trans_group']]
        res_df.index = transitions_df.index

        last_known: str = None
        def find_location(row: pd.Series, rfid: str):

            nonlocal last_known

            to_loc = row.to_loc

            if row.rfid == rfid:
                last_known = to_loc
                return to_loc
            else:
                return last_known

        for mouse in self.mice:
            last_known = "BLACK_BOX"
            # create and populate extra columns named by the rfid of all the mice
            res_df[mouse] = transitions_df.apply(find_location, args=(mouse,), axis=1)

        def get_nb_mice(row: pd.Series, location: str):
            locations = row.iloc[3:].values
            res = len([mouse_loc for mouse_loc in locations if mouse_loc == location])

            return res

        # compute the nb of mice in all locations

        for location in transitions_df.from_loc.unique():
            res_df[f"nb_mice_{location}"] = res_df.apply(get_nb_mice, args=(location,), axis=1)

        res_df["duration"] = - res_df.time.diff(periods=-1).dt.total_seconds()

        # self.batch.save()

        return res_df

    # @property
    # def mice(self) -> List[str]:
    #     # extract mice ids
    #     id_mice: set = set(self._df.rfid.unique())
    #     id_mice -= {np.nan, '0', 'na'}
    #     # self._mice = id_mice
    #
    #     return list(id_mice)



    def initialize(self):
        self._df['time'] = pd.to_datetime(self._df['time'], format="mixed", utc=True)

        self.mice = ImportBatch(batch_name=self.batch_name).mice

        # mice_occupation = MiceOccupation(mice_location=self)
        # mice_occupation.compute()
        #
        # self.mice_occupation = mice_occupation

    def get_mouse_location(self, time: datetime, mouse: str, just_before: bool = False) -> str:

        mice_location = self.get_mice_location(time, just_before)
        return mice_location[mouse]

    def get_mice_location(self, time: datetime, just_before: bool = False) -> Dict:

        res = None

        df = self.df

        num_tail = 1

        if just_before:
            num_tail = 2

        record = df.loc[df['time'] <= time].tail(num_tail)

        if len(record):
            res = record[self.mice].to_dict(orient='records')[0]

        return res

    def get_nb_mice_in_location(self, location: str, time: datetime) -> int:
        mice_loc = self.get_mice_location(time)

        if not mice_loc:
            return None

        res = [loc for loc in mice_loc.values() if loc == location]

        return len(res)

class MiceOccupation(BatchProcess):

    def __init__(self, batch_name: str, location: str = "LMT"):
        super().__init__(batch_name=batch_name)

        # self.mice_location = mice_location
        self.location = location
        # self.batch = batch

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
        return f"{self.batch_name}_occupation_{self.location}"

    def _compute(self) -> pd.DataFrame:

        df = MiceLocation(batch_name=self.batch_name).df
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
                    'duration': duration,
                    'day_since_start': day
                })

        df = pd.DataFrame(final_res)

        # add extra info, the number of mice in the LMT
        df['nb_mice'] = df.apply(lambda x: 0 if x['mice_comb'] == 'EMPTY' else len(x['mice_comb'].split('|')), axis=1)

        return df

    # @property
    # def batch_name(self) -> str:
    #     return self.batch.batch_name

class MiceSequence(BatchProcess):

    def __init__(self, batch: ImportBatch):
        super().__init__()

        self.batch = batch

    @property
    def result_id(self) -> str:

        max_delay = self.parameters.max_sequence_duration
        # max_delay = Configuration().max_delay_complete_sequence
        return f"{self.batch_name}_{max_delay}_sequences"

    def _compute(self) -> pd.DataFrame:

        res_global: List = list()

        max_delay = self.parameters.max_sequence_duration

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
    def batch_name(self) -> str:
        return self.batch.batch_name

    @property
    def dtype(self) -> Dict:
        return {
            'nb_mice_lp': 'Int64',
            'nb_mice_np': 'Int64'
        }


class OccupationTime(BatchProcess):

    def __init__(self, experiment: ImportBatch):
        super().__init__()
        self.experiment = experiment

    @property
    def result_id(self) -> str:
        return f"{self.batch_name}_occupation_time"

    def _compute(self) -> pd.DataFrame:

        xp = self.experiment
        df = MiceOccupation(batch=xp).df
        # df = xp.mice_location.mice_occupation._df

        to_concat: List[pd.DataFrame] = list()

        for mouse in [*xp.mice, 'EMPTY']:
            df_mouse = df[df['mice_comb'].str.contains(mouse)]
            tmp_df = df_mouse.groupby(['day_since_start', 'nb_mice'])['duration'].sum().reset_index()
            tmp_df['mouse'] = mouse
            to_concat.append(tmp_df)

        merged = pd.concat(to_concat).sort_values(['day_since_start', 'mouse']).reset_index(drop=True)

        return merged

    @property
    def batch_name(self) -> str:
        return self.experiment.batch_name

