import datetime
from datetime import timedelta
from typing import List, Dict

import numpy as np
import pandas as pd
from dependency_injector.wiring import inject, Provide

from batch_process.batch_process import MiceSequence, MiceOccupation, PercentageLeverPressed, \
    PercentageCompleteSequence
from common_log import create_logger
from container import Container
from data_service import DataService
from lmt.lmt_db_reader import LMTDBReader
from lmt.lmt_service import LMTService
from lmt.video2batch_link_process import Video2BatchLinkProcess
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

        # self._add_video_frame_info()
        # self.save()

        self._add_db_frame_info()
        self.save()

        df = self._add_lmt_loc()

        return df

    def _add_lmt_loc(self) -> pd.DataFrame:

        # self.logger.error("!!remove iloc!!!")
        # df = self._df.iloc[::500, :]

        df = self._df

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

                close_df = lmt_reader.get_closest_animal(frame_numbers=frame_number_list,
                                                         location=location)


                close_df.index = rows.index


                # df[['lmt_rfid', 'lmt_db_frame', 'lmt_date']] = close_df[['lmt_rfid', 'lmt_db_frame', 'lmt_date']]
                df.update(close_df)

                print("ok")



        return df

    # def _add_video_frame_info(self):
    #
    #     df_video = Video2BatchLinkProcess().df
    #     df_video = df_video[df_video["batch"] == self.batch_name]
    #
    #     res = pd.merge_asof(self.df, df_video, left_on="time", right_on="date_start")
    #     res["video_idx"][res["time"] > res["date_end"]] = -1
    #
    #     self.df["video_idx"] = res["video_idx"]


    def _add_db_frame_info(self):

        df_db = DBEventInfo(batch_name=self.batch_name).df
        self.df[["db_idx", "db_frame"]] = df_db[["db_idx", "db_frame"]]
        print("ok")


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
        self.df['time'] = pd.to_datetime(self.df['time'], format='mixed', utc=True).dt.tz_convert('Europe/Paris')

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
            'lmt_rfid': str
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


class DBEventInfo(BatchProcess):

    @property
    def result_id(self) -> str:
        return f"{self.batch_name}_db_event_info"

    @property
    def dtype(self) -> Dict:
        pass

    @inject
    def _add_db_idx(self, df: pd.DataFrame, lmt_service: LMTService = Provide[Container.lmt_service]) -> pd.DataFrame:

        current_reader: LMTDBReader = None
        current_db_idx = None

        def get_db_idx(row: pd.Series):
            nonlocal current_reader, current_db_idx

            if current_reader:
                is_inside = current_reader.is_date_inside(row['time'])
                if not is_inside:
                    current_reader = None

            if current_reader is None:
                current_reader, current_db_idx = lmt_service.get_lmt_reader(self.batch_name, row['time'])

            return current_db_idx


        df["db_idx"] = df.apply(get_db_idx, axis=1)

        return df

    def _add_db_frame(self, df: pd.DataFrame, lmt_service: LMTService = Provide[Container.lmt_service]) -> pd.DataFrame:

        df["db_frame"] = None

        groups = dict(tuple(df.groupby("db_idx")))

        for db_idx, rows in groups.items():
            self.logger.debug(f"db_idx {db_idx}")

            lmt_reader, db_idx = lmt_service.get_lmt_reader(batch_name=self.batch_name, db_idx=db_idx)
            res = lmt_reader.get_corresponding_frame_number(date_list=rows["time"].tolist())
            df_res = pd.DataFrame(data=res, columns=['db_frame'])
            df_res.index = rows.index

            df.update(df_res)

        return df


    def _compute(self) -> pd.DataFrame:

        p = ImportBatch(self.batch_name)
        # self.logger.info("!!remove iloc!!!")
        # df_event = p.df.iloc[::100, :]

        df_event = p.df


        df_event = self._add_db_idx(df_event)
        df_event = self._add_db_frame(df_event)

        return df_event[["db_idx", "db_frame"]]
