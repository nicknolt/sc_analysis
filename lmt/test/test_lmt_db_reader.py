import logging
import os
import subprocess
import unittest
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import pytz

from batch_process.import_batch import ImportBatch
from common import ROOT_DIR
from common_log import basic_config_log
from container import Container
from lmt.lmt2batch_link_process import LMT2BatchLinkProcess
from lmt.lmt_db_reader import DBInfo
from lmt.video2batch_link_process import Video2BatchLinkProcess

matplotlib.use('Qt5Agg')

container = Container()
container.config.from_ini(ROOT_DIR / "tests/resources/config.ini")

import pandas as pd

class TestLMTDBReader(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        basic_config_log(level=logging.DEBUG)
        logging.getLogger("matplotlib").setLevel(logging.WARNING)


    def test_tmp_frame_number(self):
        date_str = "2023-11-10 01:45:37+01:00"

        lmt_reader = container.lmt_service().get_lmt_reader("XP6", datetime.fromisoformat(date_str))

        print("ok")

    def test_LMT2BatchLinkProcess(self):

        # lmt_service = container.lmt_service()
        # path_str = "//192.168.25.175/souriscity/SourisCity2.0/LMT_DATA/SC1/Experience4/Experience6mice3LMT_0_4/2023_07_05/2023_07_05.sqlite"
        # path = Path(path_str)
        # tutu = path.relative_to(lmt_service.lmt_dir)


        res = LMT2BatchLinkProcess().compute(force_recompute=True)
        print("ok")


    def test_SC6(self):
        df = ImportBatch("XP6").df

        tutu = df.db_error.unique()
        # df2 = df[['action', 'device', 'rfid', 'lmt_rfid', 'time', 'db_error']][(df["rfid"] != df["lmt_rfid"]) & (~df['action'].isin(["transition", "feeder"]))]
        # df2.to_csv("rfid.csv")
        print('ok')




    def test_get_all_db_files(self):
        lmt_service = container.lmt_service()
        res = lmt_service.get_db_infos("SC2")

        print("ok")

    def test_process_batch(self):
        batch_name = "XP6"
        df = ImportBatch(batch_name=batch_name).df


        print("ok")

    def test_process_all(self):
        date_service = container.data_service()

        for batch in date_service.get_batches():
            df = ImportBatch(batch_name=batch.name).df

        print("ok")

    def test_lmt_service(self):
        lmt_service = container.lmt_service()

        res = lmt_service.get_db_infos("SC1")
        print("ok")


    def test_get_corresponding_frame_number(self):
        lmt_service = container.lmt_service()

        date = datetime.fromisoformat("2024-04-02 16:47:09+02:00")

        # df = ImportBatch(batch_name="XP10F1").df

        lmt_reader, db_idx = lmt_service.get_lmt_reader("XP10F1", date)
        # groups = dict(tuple(df.groupby("db_idx")))
        #
        # lmt_reader, db_idx = lmt_service.get_lmt_reader("XP5", db_idx=0)
        #
        # events = groups[0]
        # # frame_number = lmt_reader._get_corresponding_frame_number(from_ref_frame=[1, lmt_reader.date_start], date=date)
        #
        # # groups = dict(df.groupby("db_idx"))
        #
        res = lmt_reader.get_corresponding_frame_number(date_list=[date])
        print("ok")


    def test_get_corresponding_frame_number(self):
        lmt_service = container.lmt_service()

        date = datetime.fromisoformat("2024-04-02 16:47:09+02:00")

        df = ImportBatch(batch_name="XP10F1").df

        lmt_reader, db_idx = lmt_service.get_lmt_reader("XP10F1", date)
        groups = dict(tuple(df.groupby("db_idx")))
        #
        # lmt_reader, db_idx = lmt_service.get_lmt_reader("XP5", db_idx=0)
        #
        # events = groups[0]
        # # frame_number = lmt_reader._get_corresponding_frame_number(from_ref_frame=[1, lmt_reader.date_start], date=date)
        #
        # # groups = dict(df.groupby("db_idx"))
        rows = groups[db_idx].iloc[::200, :]

        # res_1 = lmt_reader.get_corresponding_frame_number(date_list=rows["time"].tolist())

        # 156 events => 5m25 avec methode robin VS 1.7s avec ma methode
        res_2 = lmt_reader.get_corresponding_frame_number(date_list=rows["time"].tolist())
        print("ok")





    def test_tmp2(self):
        date_str = "2023-09-10 23:54:36+02:00"
        date = datetime.fromisoformat(date_str).astimezone(tz=pytz.timezone("Europe/Paris"))

        lmt_service = container.lmt_service()
        lmt_reader, db_idx = lmt_service.get_lmt_reader("XP5", date)
        # frame_number = lmt_reader.get_corresponding_frame_number_ori(date)
        frame_number = lmt_reader._get_corresponding_frame_number(from_ref_frame=[1, lmt_reader.date_start], date=date)


        print("ok")
        # loc = container.parameters().lever_loc
        #
        # res = lmt_reader.get_closest_animal_batch(frame_numbers=[frame_number], location=loc)


        print("ok")

    def test_get_trajectory(self):

        #  https://stackoverflow.com/questions/43913457/how-do-i-name-columns-in-a-values-clause

        lmt_service = container.lmt_service()

        date_str = "2024-01-17 21:09:24+01:00"
        date = datetime.fromisoformat(date_str)

        lmt_reader, db_idx = lmt_service.get_lmt_reader("XP8", date)

        res = lmt_reader.get_trajectory(date_start=date, duration_s=6, rfid="001043737108")

        plt.plot(res.X, res.Y)
        plt.show()
        print('ok')

    def test_get_trajectories(self):
        lmt_service = container.lmt_service()
        batch_name = "XP10F1"

        df = ImportBatch(batch_name=batch_name).df
        rfid = "002026311716"
        df_filt = df[(df["rfid"] == rfid) & (df['action'] == "id_lever") & (df["day_since_start"] == 4)]

        lmt_reader, db_idx = lmt_service.get_lmt_reader(batch_name, df_filt['time'].iloc[0])

        res = lmt_reader.get_trajectories((df_filt['time'].tolist()), duration_s=6, rfid=rfid)

        for gp_id, gp_rows in res.groupby("id"):
            plt.plot(gp_rows.X, gp_rows.Y, c="r", linewidth=0.5)
            # plt.plot(gp_rows.X, gp_rows.Y)

        plt.show()
        print("kikoo")

    def test_get_many_trajectories(self):
        lmt_service = container.lmt_service()

        batch_name = "XP10F1"
        df = ImportBatch(batch_name=batch_name).df

        rfid = "002026311673"

        df_filt = df[(df["rfid"] == rfid) & (df['action'] == "id_lever")]

        for idx, row in df_filt.iterrows():

            date = row['time']

            lmt_reader, db_idx = lmt_service.get_lmt_reader(batch_name, date)

            res = lmt_reader.get_trajectories(date_list=[date], duration_s=6, rfid=rfid)

            plt.plot(res.X, res.Y, c="r", linewidth=0.5)

            # plt.plot(res.X, res.Y)

        plt.show()



    def test_batch_closest_animal_by_date(self):
        lmt_service = container.lmt_service()
        parameters = container.parameters()

        df = ImportBatch(batch_name="XP8").df
        df[['lmt_rfid', 'lmt_date', 'lmt_db_frame', 'lmt_date']] = None

        df = df[(df["action"] == "nose_poke") & (df["db_idx"] != -1)].iloc[::100, :]

        groups = dict(list(df.groupby("db_idx")))

        # for id_db, rows in groups.items():
        rows = groups[1]
        # print(id_db)

        date = rows.time.iloc[0]
        lmt_reader, db_idx = lmt_service.get_lmt_reader("XP8", date)

        # frame_number_list = rows["db_frame"].tolist()
        frame_date_list = rows["time"].tolist()

        close_df = lmt_reader.get_closest_animal_batch_time(frame_date=frame_date_list, location=parameters.feeder_loc)
        close_df.index = rows.index
        df[['lmt_rfid', 'lmt_db_frame', 'lmt_date']] = close_df[['lmt_rfid', 'lmt_db_frame', 'lmt_date']]
        print("ok")

    def test_batch_closest_animal(self):
        lmt_service = container.lmt_service()
        parameters = container.parameters()

        df = ImportBatch(batch_name="XP8").df
        df[['lmt_rfid', 'lmt_date', 'lmt_db_frame', 'lmt_date']] = None

        df = df[(df["action"] == "nose_poke") & (df["db_idx"] != -1)].iloc[::100, :]

        groups = dict(list(df.groupby("db_idx")))

        # for id_db, rows in groups.items():
        rows = groups[1]
        # print(id_db)

        date = rows.time.iloc[0]
        lmt_reader, db_idx = lmt_service.get_lmt_reader("XP8", date)

        frame_number_list = rows["db_frame"].tolist()

        close_df = lmt_reader.get_closest_animal(frame_numbers=frame_number_list, location=parameters.feeder_loc)
        close_df.index = rows.index
        df[['lmt_rfid', 'lmt_db_frame', 'lmt_date']] = close_df[['lmt_rfid', 'lmt_db_frame', 'lmt_date']]
        print("ok")

    def test_import_batch(self):

        # batch_name = "XP8"
        # batch_name = "XP5"
        batch_name = "XP11F2"

        # df = ImportBatch(batch_name="XP8").compute(force_recompute=True)
        df = ImportBatch(batch_name=batch_name).compute(force_recompute=True)
        # p = DBEventInfo(batch_name="SAMPLE_XP6").compute()

        # infos = container.data_service().get_batch_info(batch_name)

        # df_rfid = df[df.lmt_rfid != df.rfid]

        print("ok")

    def test_get_closest_animal(self):

        lmt_service = container.lmt_service()
        date_str = "2024-01-17 21:09:24+01:00"
        # date_str = "2024-01-17 10:42:19+01:00"
        # date_str = "2024-01-18 10:51:00+01:00"
        date = datetime.fromisoformat(date_str)

        delta = (1931600 - 1931482)/30
        feeder_loc = (100, 200)
        lever_loc = (410,200)
        loc = feeder_loc

        lmt_reader, db_idx = lmt_service.get_lmt_reader("XP8", date)
        frame_number = lmt_reader.get_corresponding_frame_number(date)
        res = lmt_reader.get_closest_animal(frame_number=frame_number, location=loc)

        print("ok")
        # lmt_reader, db_idx = lmt_service.get_lmt_reader("XP8", date)
        #
        # frame_number = lmt_reader.get_corresponding_frame_number(date)
        #
        # lmt_reader.get_closest_animal(frame_number=frame_number, location=())


    def test_tmp_check_db_binding(self):
        # naive_str = "2024-01-14 21:15:53"
        # naive_date = datetime.strptime(naive_str, "%Y-%m-%d %H:%M:%S")
        # naive_date_local_tz = naive_date.astimezone(timezone.utc)

        date_str = "2024-01-14 21:15:53+01:00"
        date = datetime.fromisoformat(date_str)
        ts = date.timestamp()

        date_changed = date.replace(tzinfo=None)

        new_ts = date_changed.timestamp()

        lmt_service = container.lmt_service()
        lmt_reader, db_idx = lmt_service.get_lmt_reader("XP8", date)
        num_frame = lmt_reader.get_corresponding_frame_number(date - timedelta(hours=1))

        # previous
        # num_frame = 10871638
        # \\192.168.25.177\SourisCity/SourisCity_2/Experience6mice3LMT_0_8/2024_01_10/corrected_2024_01_10.sqlite
        ts = 1705263352998 / 1000

        date_from_db = datetime.fromtimestamp(ts)

        print("ok")

    def test_get_db_infos(self):
        lmt_service = container.lmt_service()
        res = lmt_service.get_db_infos()

        for db in res:
            print(f"{db.path.name} date_start: {db.date_start} date_end: {db.date_end}")

    def test_copy_db_in_error(self):
        lmt_service = container.lmt_service()

        res = lmt_service.get_db_infos()

        to_correct = list(filter(lambda dbinfo: dbinfo.date_start is None or dbinfo.date_end is None, res))

        # sqlite3 2023_03_23.sqlite ".recover" | sqlite3 new.sqlite

        elem: DBInfo
        for elem in to_correct:
            # tmp = elem.path

            # to_dest = Path(r"C:\Users\Nicolas\Desktop\tmp\CORRECT_DB") / f"{elem.path.name}"
            to_dest = Path(r"C:\Users\Nicolas\Desktop\tmp\CORRECT_DB\\")
            # # sqlite3 2023_03_23.sqlite ".recover" | sqlite3 new.sqlite
            # cmd = f"{elem.path} \".recover\" | sqlite3 '{to_dest}'"
            cmd = f"xcopy /d /i {elem.path} {to_dest}"
            print(f"{cmd}")
            subprocess.check_output(cmd, shell=True)


            # subprocess.call(["sqlite3", cmd], shell=True)
            # os.system(cmd)

            print("ok")
        print("ok")

    def test_recover_db_in_error(self):
        # https://janakiev.com/blog/python-shell-commands/

        db_dir = Path(r"C:\Users\Nicolas\Desktop\tmp\CORRECT_DB\\")

        sqlite3_path = Path(r"C:\SANS_INSTALL\sqlite_tools\sqlite-tools-win-x64-3490100\sqlite3.exe")
        for db_file in db_dir.glob("*.sqlite"):
            to_dest = db_dir / "corrected" / f"corrected_{db_file.name}"
            cmd = f"{sqlite3_path} {db_file} \".recover\" | {sqlite3_path} {to_dest}"

            try:
                grepOut = subprocess.check_output(cmd, shell=True)
                print(grepOut)
            except subprocess.CalledProcessError as grepexc:
                print("error code", grepexc.returncode, grepexc.output)

    def test_tmp_utility_restore_file_date(self):
        dir_path = Path(r"\\192.168.25.175\souriscity\SourisCity2.0\LMT_DATA\Experience6mice3LMT_0_8")

        file: Path
        for file in dir_path.glob("./**/*.*"):
            ts = os.path.getmtime(file)
            new_m_date = datetime.fromtimestamp(ts) - timedelta(hours=1)
            # os.utime(file, (new_m_date.timestamp(), new_m_date.timestamp()))
            # os.path.set
            print(new_m_date)

    def test_tmp_utility_correct_XP8_date(self):
        old_date_format = '%d-%m-%Y %H:%M:%S'
        tz_str = pytz.timezone("Europe/Paris")

        # substract one hour to all events
        filename = Path(r"/tests/resources/data/XP8/1_10_17_37_56.csv.bak")
        str_io = StringIO()

        with open(filename, 'r') as f:
            for line in f:
                tab_line = line.split(';')
                date_str = tab_line[2]
                date = datetime.strptime(date_str, old_date_format)
                new_date = (date - timedelta(hours=1)).astimezone(tz=tz_str)

                tab_line[2] = str(new_date)

                new_line = str.join(';', tab_line)
                str_io.write(new_line)


        f_output = Path(r"/tests/resources/data/XP8/1_10_17_37_56_corrected.csv.bak")
        with open(f_output, 'w+') as f:
            f.write(str_io.getvalue())


        # res = container.data_service().get_raw_df("XP8")
        print("ok")
        pass

if __name__ == '__main__':
    unittest.main()
