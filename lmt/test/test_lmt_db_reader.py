import unittest
from datetime import datetime
from pathlib import Path

from lmt.lmt_db_reader import LMTDBReader


class TestLMTDBReader(unittest.TestCase):

    def test_something(self):
        db_file = Path(r"\\192.168.25.177\SourisCity\SourisCity_2\Experience6mice3LMT_0_2\2023_04_12\2023_04_12.sqlite")
        reader = LMTDBReader(db_file)

        begin = reader.date_start
        end = reader.date_end

        res = reader.get_corresponding_frame_number(date=datetime(year=2023, month=4, day=12, hour=10))
        print("kikoo")

if __name__ == '__main__':
    unittest.main()
