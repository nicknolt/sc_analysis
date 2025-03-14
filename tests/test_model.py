import os
import subprocess
import unittest
from pathlib import Path
from typing import Dict

import pandas as pd

from batch_process import ImportBatch, MiceSequence, OccupationTime
from common import FileMerger, ROOT_DIR
from common_log import basic_config_log
from container import Container
from pre_analysis.pre_analysis import MiceWeight

container = Container()
container.config.from_ini(ROOT_DIR / "tests/resources/config.ini")

class TestModel(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        basic_config_log()
        # config = Configuration(base_dir=Path('./resources'), result_dir=Path(r"C:\Users\Nicolas\Desktop\tmp\SC_OUTPUT"))

    def test_tutu(self):
        df = ImportBatch("XP9").df
        print("ok")


    # def test_iso_format(self):
    #     date_str = "2024-09-06T16:32:14.723+02:00"
    #
    #     date = pd.to_datetime(date_str)
    #
    #     print(date)


    # def test_transition_resolver(self):
    #
    #     process = TemporaryImportBatch(batch_name="XP11")
    #
    #     TransitionResolver(process=process).resolve()
    #
    #     print("kikoo")


    def test_MiceWeight(self):

        mice_weight = MiceWeight(batch_name="XP13F3")
        mice_weight.compute()

        mice_weight.export_figure()

        print("OK")


    def test_temp_import(self):

        # df = TemporaryImportBatch(batch_name="XP11").compute(force_recompute=True)

        df = ImportBatch(batch_name="XP13F3").compute(force_recompute=True)

        print("kikoo")

    def test_transition_interval(self):
        # XP11 have transition ERROR
        # config = Configuration(base_dir=Path('./resources'))
        batch = ImportBatch.load(batch_name="XP11")

        df = batch.compute()

        # tmp = df.groupby('trans_group')['time'].transform('first')
        #
        # MiceSequence(batch=batch).compute(force_recompute=True)
        print("ok")

    def test_compute_cluster(self):
        # https://stackoverflow.com/questions/22219004/how-to-group-dataframe-rows-into-list-in-pandas-groupby

        # https://stackoverflow.com/questions/47152691/how-can-i-pivot-a-dataframe

        config = Configuration(base_dir=Path('./resources'))
        xp = Experiment()

        lever_pressed = xp.get_percentage_lever_pressed()
        sequence = xp.get_percentage_complete_sequence()

        df = lever_pressed.df
        df = df[df.day_since_start.between(1, 22)].reset_index(drop=True)
        tutu = df.groupby(['rfid', 'day_since_start'])['percent_pressed'].apply(list)
        # table = pd.pivot_table(df, values='D', index=['A', 'B'],
        #
        #                        columns=['C'], aggfunc="sum")

        tata = df.pivot_table(index=['rfid'], columns='day_since_start', values='percent_pressed')
        # toto = df.set_index(['rfid', 'day_since_start']).unstack(level=0)
        print("ok")

    def test_Experiment_batches(self):
        config = Configuration(base_dir=Path('./resources'))
        xp = Experiment()
        res = xp.batches

        res = xp.get_percentage_lever_pressed().df
        res_seq = xp.get_percentage_complete_sequence().df

        print("ok")


    def test_mice_location_compute(self):
        config = Configuration(base_dir=Path('./resources'))
        batch = ImportBatch.load(batch_name="XP11")

        # batch.mice_location.compute(force_recompute=True)
        #
        # print("ok")


    def test_load_experiment(self):
        # config = Configuration(base_dir=Path('./resources'))
        batch = ImportBatch(batch_name="XP11F2T")

        batch.compute(force_recompute=True)

        print("ok")
        # res = PercentageCompleteSequence(xp).compute(force_recompute=True)


        # tutu = xp.get_mice_occupation(location="LMT")

        # df = xp.get_percentage_lever_pressed().df
        # # df2 = xp.get_percentage_complete_sequence().df
        #
        print("OK")

    def test_compute_pourcentage_lever_press(self):
        # https://stackoverflow.com/questions/23377108/pandas-percentage-of-total-with-groupby

        config = Configuration(base_dir=Path('./resources'))
        xp = ImportBatch.load(batch_name="XP11")

        df = xp.lever_press()

        df = df.groupby(['day_since_start', 'rfid']).size().reset_index(name='nb_lever_press')
        df["total_per_day"] = df.groupby('day_since_start')['nb_lever_press'].transform('sum')
        df["percent_pressed"] = (df['nb_lever_press'] / df["total_per_day"])*100


        ################################

        df = xp.mice_sequence.df
        df = df[df['complete_sequence']]
        df = df.groupby(['day_since_start', 'rfid_lp']).size().reset_index(name='nb_complete_sequence')
        df["total_per_day"] = df.groupby('day_since_start')['nb_complete_sequence'].transform('sum')
        df["percent_complete_sequence"] = (df['nb_complete_sequence'] / df["total_per_day"])*100
        print("ok")

    def test_compute_sequences(self):
        config = Configuration(base_dir=Path('./resources'))
        xp = ImportBatch.load(batch_name="XP11")

        ms = MiceSequence(xp)
        res = ms.compute(force_recompute=True)

        print("ok")


    def test_execute_r_script(self):

        output_file = Configuration().result_dir / 'XP11_one_step_seq_LEVER_PRESS_CAMEMBERT.jpg'
        p = subprocess.Popen(
            ["Rscript", "--vanilla",
             r"..\scripts_R\ND_LP_camembert.R",
             r"C:\Users\Nicolas\PycharmProjects\SC_Analysis\tests\resources\cache\XP11\XP11_one_step_seq_LEVER_PRESS.csv",
             output_file.absolute()],
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

        # with Image.open(output_file) as img:
        #     img.show()


        print("ok")

    def test_occupation_time_each_mouse(self):
        config = Configuration(base_dir=Path('./resources'))
        xp = ImportBatch.load(batch_name="XP11")

        ot = OccupationTime(experiment=xp)

        res = ot.compute()

        print("OK")





    def test_mice_occupation_in_LMT(self):

        # filename = "2_21_15_45_31.csv" #XP9
        # csv_path = Path('./resources/XP9')

        config = Configuration(base_dir=Path('./resources'))

        xp = ImportBatch.load('XP9')
        mo = xp.mice_location

        mice = list(mo._df.columns[3:])

        res_per_day: Dict[int, Dict[str, float]] = dict()

        for day, day_data in xp.mice_location._df.groupby('day_since_start'):

            res_comb: Dict[str, float] = dict()
            for index, row in day_data.iterrows():
                mice_in_lmt = [x for x in mice if row[x] == "LMT"]
                mice_key = ','.join(mice_in_lmt)

                if mice_key not in res_comb:
                    res_comb[mice_key] = row.duration
                    # print("A CREER")
                else:
                    res_comb[mice_key] += row.duration
                    # print("existe deja")

            res_per_day[day] = res_comb

        final_res = list()

        for day, values in res_per_day.items():
            for mice_comb, duration in values.items():
                final_res.append({
                    'mice_comb':mice_comb,
                    'duration': duration,
                    'day_since_start': day
                })

        df = pd.DataFrame(final_res)
        df.to_csv("./tutu.csv")
        print("ok")

    def test_FileMerger(self):
        dir = Path("resources/data/XP6")

        files = list(dir.glob(pattern="*.csv"))
        files.sort(key=os.path.getmtime)



        fm = FileMerger(files=files)
        tutu = fm.merge()

        print("ok")
    # def test_MiceOccupation(self):
    #     config = Configuration(base_dir=Path('./resources'))
    #
    #     print("ok")
    #     # self.assertEqual(Path('./resources'), config.get_base_dir())
    #     #
    #     # config2 = Configuration()
    #     #
    #     # self.assertIs(config, config2)

