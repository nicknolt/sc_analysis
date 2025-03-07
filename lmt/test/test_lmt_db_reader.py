import os
import subprocess
import unittest
from datetime import datetime
from pathlib import Path

from common import ROOT_DIR
from common_log import basic_config_log
from container import Container
from lmt.lmt2batch_link_process import LMT2BatchLinkProcess
from lmt.lmt_db_reader import LMTDBReader, DBInfo
from lmt.lmt_service import LMTService

container = Container()
container.config.from_ini(ROOT_DIR / "tests/resources/config.ini")

class TestLMTDBReader(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        basic_config_log()

    def test_lmt2batch(self):
        process = LMT2BatchLinkProcess()

        res = process.df

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
