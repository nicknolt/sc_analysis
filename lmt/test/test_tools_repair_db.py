import logging
import os
import subprocess
import unittest
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path

import matplotlib
import pytz

from common import ROOT_DIR
from common_log import basic_config_log
from container import Container
from lmt.lmt_db_reader import DBInfo

matplotlib.use('Qt5Agg')

container = Container()
container.config.from_ini(ROOT_DIR / "tests/resources/config.ini")

class TestToolRepairDB(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        basic_config_log(level=logging.DEBUG)
        logging.getLogger("matplotlib").setLevel(logging.WARNING)

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
        filename = Path(r"C:\Users\Nicolas\Desktop\tmp\TMP_XP_7_9\XP9\2_21_15_45_31.csv")
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


        f_output = Path(r"C:\Users\Nicolas\Desktop\tmp\TMP_XP_7_9\XP9\2_21_15_45_31_corrected.csv")
        with open(f_output, 'w+') as f:
            f.write(str_io.getvalue())


        # res = container.data_service().get_raw_df("XP8")
        print("ok")
        pass

if __name__ == '__main__':
    unittest.main()
