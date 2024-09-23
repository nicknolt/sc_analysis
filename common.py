import os
from pathlib import Path
from typing import List

from common_log import create_logger

# define the top dir of the project
ROOT_DIR = Path(__file__).parent

class FileMerger:

    def __init__(self, files: List[Path] = list()):
        self.logger = create_logger(self)
        self._files = files

    def merge(self) -> str:

        res = ""

        for fname in self._files:
            with open(fname) as infile:
                res += infile.read()

        return res