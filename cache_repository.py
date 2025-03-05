from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from process import Process


class CacheRepository:

    def __init__(self, result_dir: str):
        self.result_dir = Path(result_dir)

    @property
    def cache_dir(self) -> Path:

        res = Path(self.result_dir) / "cache"

        if not res.exists():
            res.mkdir(parents=True)

        return res

    def _get_cache_file(self, process: 'Process') -> Path:

        from process import BatchProcess, GlobalProcess

        if isinstance(process, BatchProcess):
            dir_dest = self.cache_dir / process.batch_name
            if not dir_dest.exists():
                dir_dest.mkdir(parents=True)
        elif isinstance(process, GlobalProcess):
            dir_dest = self.cache_dir
        else:
            raise NotImplementedError(f"Process type {type(process)} need to be subclassed to BatchProcess or GlobalProcess")

        cache_file = dir_dest / f"{process.result_id}.csv"

        return cache_file

    def save(self, process: 'Process'):
        process.df.to_csv(self._get_cache_file(process))

    def load(self, process: 'Process') -> pd.DataFrame:

        cache_file = self._get_cache_file(process)

        if cache_file.exists():
            df = pd.read_csv(cache_file, dtype=process.dtype, index_col=0)
            return df
        else:
            return None
