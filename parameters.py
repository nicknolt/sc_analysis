from typing import List, Tuple


class Parameters:

    def __init__(self, max_sequence_duration: int, lever_loc: str, feeder_loc: str) -> None:
        
        self.max_sequence_duration = max_sequence_duration
        self.lever_loc = tuple(map(lambda elem: int(elem), lever_loc.split(',')))
        self.feeder_loc = tuple(map(lambda elem: int(elem), feeder_loc.split(',')))

        self._mice: List[str] = None
    
    @property
    def mice(self) -> List[str]:
        return self._mice
    
    @mice.setter
    def mice(self, value: List[str]) -> None:
        self._mice = value

