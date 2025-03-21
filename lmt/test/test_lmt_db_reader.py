import logging
import os
import subprocess
import unittest
from datetime import datetime, timezone, timedelta
from io import StringIO
from pathlib import Path

import pytz

from batch_process import ImportBatch, DBEventInfo
from common import ROOT_DIR
from common_log import basic_config_log
from container import Container
from lmt.lmt_db_reader import LMTDBReader, DBInfo
from lmt.lmt_service import LMTService
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
matplotlib.use('Qt5Agg')

container = Container()
container.config.from_ini(ROOT_DIR / "tests/resources/config.ini")

class TestLMTDBReader(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        basic_config_log(level=logging.DEBUG)
        logging.getLogger("matplotlib").setLevel(logging.WARNING)

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
        df = ImportBatch(batch_name="XP8").df
        rfid = "001043737108"
        df_filt = df[(df["rfid"] == rfid) & (df['action'] == "id_lever") & (df["day_since_start"] == 2)]

        lmt_reader, db_idx = lmt_service.get_lmt_reader("XP8", df_filt['time'].iloc[0])

        res = lmt_reader.get_trajectories(df_filt['time'].tolist(), duration_s=6, rfid="001043737108")

        for gp_id, gp_rows in res.groupby("id"):
            plt.plot(gp_rows.X, gp_rows.Y)

        plt.show()
        print("kikoo")

    def test_get_many_trajectories(self):
        lmt_service = container.lmt_service()

        df = ImportBatch(batch_name="XP8").df

        rfid = "001043737108"

        df_filt = df[(df["rfid"] == rfid) & (df['action'] == "id_lever")]

        for idx, row in df_filt.iterrows():

            date = row['time']

            lmt_reader, db_idx = lmt_service.get_lmt_reader("XP8", date)

            res = lmt_reader.get_trajectory(date_start=date, duration_s=6, rfid="001043737108")

            plt.plot(res.X, res.Y)

        plt.show()




    def test_import_batch(self):

        df = ImportBatch(batch_name="XP8").compute()
        # p = DBEventInfo(batch_name="SAMPLE_XP6").compute()

        df_rfid = df[df.lmt_rfid != df.rfid]

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

    def test_tmp_add_db_event_info(self):
        from lmt.lmt2batch_link_process import LMT2BatchLinkProcess

        lmt_service = container.lmt_service()

        df_event = ImportBatch("XP6").df

        current_reader: LMTDBReader = None
        current_db_idx: int = None
        nb_elem = len(df_event)

        def get_db_idx(row: pd.Series):

            nonlocal current_reader, current_db_idx

            if (current_reader is None) or (not current_reader.is_date_inside(row['time'])):

                if current_reader:
                    current_reader.close()

                current_reader, current_db_idx = lmt_service.get_lmt_reader("XP6", row['time'])

            if current_reader is None:
                num_frame = -1
                current_db_idx = -1
            else:
                num_frame = current_reader.get_corresponding_frame_number(row['time'], close_connexion=False)
            # print(f"{row.name}/{nb_elem}")
            row["db_idx"] = current_db_idx
            row["db_frame"] = num_frame

            return row


        df_event[["db_idx", "db_frame"]] = None #df_event.apply(get_db_idx, axis=1)
        df_event = df_event.apply(get_db_idx, axis=1)

        df_event.to_csv("./tutu.csv")

        print("ok")

    def test_tmp_groupy_etc(self):
        df = ImportBatch("XP8").df

        df_event = df[df.action == 'nose_poke']
        df['extra_field'] = df_event.apply(lambda row: "NP", axis=1)
        df_event = df[df.action == 'id_lever']
        df['extra_field'] = df_event.apply(lambda row: "LP", axis=1)
        print("ok")

    def test_get_db_path(self):
        from lmt.lmt2batch_link_process import LMT2BatchLinkProcess

        p = LMT2BatchLinkProcess()

        date_str = "2024-01-16 08:04:34.499000+01:00"
        date = datetime.fromisoformat(date_str)

        res = p.get_db_path(batch_name="XP8", date=date)

        print("ok")

    def test_extract_db_event_infos(self):

        p = ExtractDBEventInfo(batch_name="XP6")
        df = p.df

        print("ok")




    def test_something(self):
        db_file = Path(r"\\192.168.25.177\SourisCity\SourisCity_2\Experience6mice3LMT_0_2\2023_04_12\2023_04_12.sqlite")
        reader = LMTDBReader(db_file)

        begin = reader.date_start
        end = reader.date_end

        res = reader.get_corresponding_frame_number(date=datetime(year=2023, month=4, day=12, hour=10))
        print("kikoo")

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
