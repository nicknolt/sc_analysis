from typing import List, Dict, TYPE_CHECKING

import pandas as pd

from process import BatchProcess

if TYPE_CHECKING:
    from batch_process.import_batch import ImportBatch, MiceLocation


# class GlobalPercentageLeverPressed(BatchProcess):
#
#     def __init__(self, experiment: 'Experiment'):
#         super().__init__()
#         self.experiment = experiment
#
#     @property
#     def result_id(self) -> str:
#         return f"global_percentage_lever_pressed"
#
#     def _compute(self) -> pd.DataFrame:
#
#         batche_names = self.experiment.batches
#
#         list_df: List[pd.DataFrame] = list()
#
#         for batch_name in batche_names:
#             batch = ImportBatch.load(batch_name)
#             df = batch.get_percentage_lever_pressed().df
#             df['batch'] = batch_name
#
#             list_df.append(df)
#
#         df_merged = pd.concat(list_df)
#
#         return df_merged
#     @property
#     def batch_name(self) -> str:
#         return "Global"
#
#     @property
#     def dtype(self) -> Dict:
#         return {
#             'rfid': str
#         }

# class GlobalPercentageCompleteSequence(BatchProcess):
#
#     def __init__(self, experiment: 'Experiment'):
#         super().__init__()
#         self.experiment = experiment
#
#     @property
#     def result_id(self) -> str:
#         return f"global_complete_sequence"
#
#     def _compute(self) -> pd.DataFrame:
#
#         batche_names = self.experiment.batches
#
#         list_df: List[pd.DataFrame] = list()
#
#         for batch_name in batche_names:
#             batch = ImportBatch.load(batch_name)
#             df = batch.get_percentage_complete_sequence().df
#             df['batch'] = batch_name
#
#             list_df.append(df)
#
#         df_merged = pd.concat(list_df)
#
#         return df_merged
#     @property
#     def batch_name(self) -> str:
#         return "Global"
#
#     @property
#     def dtype(self) -> Dict:
#         return {
#             'rfid': str
#         }

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

    def __init__(self, batch: 'ImportBatch'):
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

    def __init__(self, experiment: 'ImportBatch'):
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

