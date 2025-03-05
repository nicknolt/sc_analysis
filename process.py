import json
import os
import subprocess
from abc import abstractmethod
from pathlib import Path
from threading import Thread
from typing import Dict, TYPE_CHECKING

import pandas as pd
from PIL import Image

from dependency_injector.wiring import inject, Provide


from common import ROOT_DIR
from common_log import create_logger
from container import Container
from data_service import DataService
from parameters import Parameters

if TYPE_CHECKING:
    from cache_repository import CacheRepository

class Process:

    # @inject
    # def __init__(self, data_service: DataService = Provide[Container.data_service]):
    @inject
    def __init__(self, parameters: Parameters = Provide[Container.parameters]):

        self.logger = create_logger(self)
        self._df: pd.DataFrame = None
        self.parameters: Parameters = parameters

        self.figure: RFigure = None

    @property
    @abstractmethod
    def result_id(self) -> str:
        pass

    @property
    @abstractmethod
    def dtype(self) -> Dict:
        pass

    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            self.compute()

        return self._df

    @inject
    def compute(self, force_recompute: bool = False, cache_repo: 'CacheRepository' = Provide[Container.cache_repository]) -> pd.DataFrame:

        res = cache_repo.load(self)

        if res is not None and not force_recompute:
            self._df = res
        else:
            self._df = self._compute()
            self.save()

        self.initialize()

        return self.df

    @inject
    def get_result_dir(self, ds: DataService = Provide[Container.data_service]) -> Path:
        return ds.result_dir

    @inject
    def save(self, cache_repo: 'CacheRepository' = Provide[Container.cache_repository]):
        cache_repo.save(self)

    def to_csv(self) -> Path:

        result_dir = self.get_result_dir()

        if not result_dir.exists():
            result_dir.mkdir(parents=True)

        dest_dir = result_dir / f"{self.result_id}.csv"

        self.df.to_csv(dest_dir)

        return dest_dir

    def export_figure(self):

        def run():

            figure_path = self.figure.export()

            image = Image.open(figure_path)
            image.show()

        thread = Thread(target=run)
        thread.start()

    @abstractmethod
    def _compute(self) -> pd.DataFrame:
        pass

    def initialize(self):
        pass

class BatchProcess(Process):

    def __init__(self, batch_name: str):
        super().__init__()
        self.batch_name = batch_name

class GlobalProcess(Process):
    pass

# class Process:
#
#     def __init__(self):
#         self.logger = create_logger(self)
#         self._df: pd.DataFrame = None
#
#         self.figure: RFigure = None
#
#     def compute(self, force_recompute: bool = False) -> pd.DataFrame:
#
#         base_dir = Configuration().get_base_dir()
#         cache_dir = base_dir / "cache" / self.batch_name
#
#         cache_file = cache_dir / f"{self.result_id}.csv"
#
#         self.logger.info(f"Search result for {self.result_id}")
#         if not cache_dir.exists():
#             cache_dir.mkdir(parents=True)
#
#         if cache_file.exists() and not force_recompute:
#             self.logger.info(f"'{self.result_id}' is loaded from cache")
#             df = pd.read_csv(cache_file, dtype=self.dtype, index_col=0)
#             self._df = df
#         else:
#
#             self.logger.info(f"Compute {self.result_id}")
#             df = self._compute()
#             self.logger.info(f"End Compute {self.result_id}")
#             self._df = df
#             self.save()
#             # df.to_csv(cache_file)
#
#         self.initialize()
#
#         return df
#
#     def save(self):
#         base_dir = Configuration().get_base_dir()
#         cache_dir = base_dir / "cache" / self.batch_name
#         cache_file = cache_dir / f"{self.result_id}.csv"
#
#         self._df.to_csv(cache_file)
#
#     def export_figure(self):
#         def run():
#
#             self.figure.export()
#
#             image = Image.open(self.figure.figure_output_dir / self.figure.figure_id)
#             image.show()
#
#             # fig_files = list(self.figure.figure_output_dir.glob(pattern=f"{self.figure.figure_id}"))
#             #
#             # for fig in fig_files:
#             #     image = Image.open(fig)
#             #     image.show()
#
#         thread = Thread(target=run)
#         thread.start()
#
#     def csv_output(self) -> Path:
#         return Path(Configuration().result_dir / f"{self.result_id}.csv")
#
#     def to_csv(self):
#
#         if not Configuration().result_dir.exists():
#             Configuration().result_dir.mkdir(parents=True)
#
#         self.df.to_csv(self.csv_output())
#
#     @property
#     def df(self) -> pd.DataFrame:
#         if self._df is None:
#             self._df = self.compute()
#
#         return self._df
#
#     @property
#     @abstractmethod
#     def result_id(self) -> str:
#         pass
#
#     @abstractmethod
#     def _compute(self) -> pd.DataFrame:
#         pass
#
#     @property
#     @abstractmethod
#     def batch_name(self) -> str:
#         pass
#
#     @property
#     def dtype(self) -> Dict:
#         return {
#             'rfid_lp': 'string',
#             'rfid_np': 'string'
#         }
#
#     def initialize(self):
#         pass
#
#     # @staticmethod
#     # def delete_cache(xp_name: str):
#     #     base_dir = Configuration().get_base_dir()
#     #     cache_dir = base_dir / "cache" / xp_name
#     #
#     #     rmtree(cache_dir)


class RFigure:

    def __init__(self, process: Process, script_name: str):
        self.logger = create_logger(self)
        self.process = process
        self.script_name = script_name

    # @property
    # def figure_output_dir(self) -> Path:
    #     return Configuration().result_dir

    @property
    @abstractmethod
    def figure_id(self) -> str:
        pass

    @property
    @abstractmethod
    def extra_args(self) -> Dict[str, str]:
        pass

    def export(self) -> Path:

        csv_dir = self.process.to_csv()

        script_r = ROOT_DIR / "scripts_R" / self.script_name

        output_file = self.process.get_result_dir() / self.figure_id

        res_dic = {
            "figure_file": str(output_file),
            "csv_file": str(csv_dir.absolute())

        }

        if self.extra_args:
            res_dic.update(self.extra_args)

        json_args = json.dumps(res_dic)

        p = subprocess.Popen(
            ["Rscript", "--vanilla",
             script_r,
             json_args],
            cwd=os.getcwd(),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        output, error = p.communicate()

        if p.returncode == 0:
            print('R OUTPUT:\n {0}'.format(output.decode("utf-8")))
        else:
            print('R OUTPUT:\n {0}'.format(output.decode("utf-8")))
            print('R ERROR:\n {0}'.format(error.decode("utf-8")))

        return output_file
