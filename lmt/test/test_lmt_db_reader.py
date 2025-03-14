import logging
import os
import subprocess
import unittest
from datetime import datetime
from pathlib import Path

from batch_process import ImportBatch, DBEventInfo
from common import ROOT_DIR
from common_log import basic_config_log
from container import Container
from lmt.lmt_db_reader import LMTDBReader, DBInfo
from lmt.lmt_service import LMTService
import pandas as pd

container = Container()
container.config.from_ini(ROOT_DIR / "tests/resources/config.ini")

class TestLMTDBReader(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        basic_config_log(level=logging.DEBUG)


    def test_import_batch(self):

        df = ImportBatch(batch_name="SAMPLE_XP6").compute(force_recompute=True)
        # p = DBEventInfo(batch_name="SAMPLE_XP6").compute()

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

if __name__ == '__main__':
    unittest.main()
