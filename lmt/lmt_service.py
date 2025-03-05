from pathlib import Path
from typing import List, TYPE_CHECKING

from common_log import create_logger
from lmt.lmt_db_reader import LMTDBReader

if TYPE_CHECKING:
    from lmt.lmt_db_reader import DBInfo

class LMTService:

    def __init__(self, lmt_dir: str):
        self.logger = create_logger(self)
        self.lmt_dir = Path(lmt_dir)

    def _get_all_db_files(self) -> List[Path]:

        res = list(self.lmt_dir.glob('**/*.sqlite'))

        self.logger.info(f"{len(res)} sqlite files found")

        return res


    def get_db_infos(self) -> List['DBInfo']:

        self.logger.info(f"Getting DB infos in directory '{self.lmt_dir}'")

        res = map(lambda db_path: LMTDBReader(db_path).db_info, self._get_all_db_files())

        return list(res)
