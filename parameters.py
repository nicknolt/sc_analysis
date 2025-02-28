from typing import List


class Parameters:

    def __init__(self, max_sequence_duration: int) -> None:
        
        self.max_sequence_duration = max_sequence_duration

        self._mice: List[str] = None
    
    @property
    def mice(self) -> List[str]:
        return self._mice
    
    @mice.setter
    def mice(self, value: List[str]) -> None:
        self._mice = value

