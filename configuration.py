from pathlib import Path
from typing import Tuple


class Singleton(type):

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class Configuration(metaclass=Singleton):

    def __init__(self,
                 base_dir: Path,
                 max_delay_complete_sequence: int = 3,
                 analysis_interval: Tuple[int] = (1, 22),
                 result_dir: Path = None
                 ):#valeur calculée en s pour 80% souris LP-NP
        self._base_dir = base_dir

        # for complete sequences
        self.max_delay_complete_sequence: int = max_delay_complete_sequence
        self.result_dir = result_dir
        # for all 'final' results and figures
        self.analysis_interval: Tuple[int] = analysis_interval

    def get_base_dir(self) -> Path:
        return self._base_dir
