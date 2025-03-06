import os
import unittest
from datetime import datetime
from io import StringIO
from pathlib import Path

import dateutil.tz
import pytz

from common import FileMerger, ROOT_DIR
from common_log import basic_config_log
from container import Container

container = Container()
container.config.from_ini(ROOT_DIR / "tests/resources/config.ini")

class TestDataService(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        basic_config_log()
        # config = Configuration(base_dir=Path('./resources'), result_dir=Path(r"C:\Users\Nicolas\Desktop\tmp\SC_OUTPUT"))



    def test_tmp_convert_xp6(self):
        data_service = container.data_service()

        data_dir = Path(r"\\192.168.25.175\souriscity\SourisCity2.0\Sniffer local") / "XP6"

        csv_file = list(data_dir.glob("*.csv"))
        csv_file.sort(key=os.path.getmtime)
        file_merger = FileMerger(csv_file)
        csv_str = file_merger.merge()

        # format have changed btw experiment, we have to check the dateformat
        old_date_format = '%d-%m-%Y %H:%M:%S'

        new_csv = ""
        last_date = None
        before = True

        tz_str = pytz.timezone("Europe/Paris")

        for line in csv_str.splitlines():
            row = line.split(';')
            date_str = row[2]

            old_date = datetime.strptime(date_str, old_date_format)

            if last_date and (old_date < last_date):
                print("retour vers le futur")
                before = False
            #  2025-02-21T17:10:17.022+01:00

            date_str = old_date.isoformat()

            if before: # +2 after +1
                new_date_str = f"{date_str}+02:00"
            else:
                new_date_str = f"{date_str}+01:00"

            row[2] = new_date_str

            new_row = str.join(';', row)
            new_csv += new_row + '\n'

            last_date = old_date
            # print("ok")


        # df = data_service.get_raw_df("XP6")
        print(new_csv)

        with open("corrected.csv", "w") as text_file:
            text_file.write(new_csv)


        print("ok")

    def test_get_batch_info(self):
        data_service = container.data_service()

        df = data_service.get_raw_df("XP6")

        print("ok")

    def test_gel_all_batches(self):
        data_service = container.data_service()

        res = data_service.get_batches()

        print("ok")
