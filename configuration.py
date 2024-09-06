from pathlib import Path


class Singleton(type):

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class Configuration(metaclass=Singleton):

    def __init__(self, base_dir: Path, max_delay_complete_sequence: int = 5):
        self._base_dir = base_dir
        self.max_delay_complete_sequence: int = max_delay_complete_sequence

    def get_base_dir(self) -> Path:
        return self._base_dir
