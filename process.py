import json
import os
import subprocess
from abc import abstractmethod
from pathlib import Path
from threading import Thread
from typing import Dict, List

import pandas as pd
from PIL import Image

from common_log import create_logger
from configuration import Configuration


class Process:

    def __init__(self):
        self.logger = create_logger(self)
        self._df: pd.DataFrame = None

        self.figure: RFigure = None

    def compute(self, force_recompute: bool = False) -> pd.DataFrame:

        base_dir = Configuration().get_base_dir()
        cache_dir = base_dir / "cache" / self.batch_name

        cache_file = cache_dir / f"{self.result_id}.csv"

        self.logger.info(f"Search result for {self.result_id}")
        if not cache_dir.exists():
            cache_dir.mkdir(parents=True)

        if cache_file.exists() and not force_recompute:
            self.logger.info(f"'{self.result_id}' is loaded from cache")
            df = pd.read_csv(cache_file, dtype=self.dtype, index_col=0)
            self._df = df
        else:

            self.logger.info(f"Compute {self.result_id}")
            df = self._compute()
            self.logger.info(f"End Compute {self.result_id}")
            self._df = df
            self.save()
            # df.to_csv(cache_file)

        self.initialize()

        return df

    def save(self):
        base_dir = Configuration().get_base_dir()
        cache_dir = base_dir / "cache" / self.batch_name
        cache_file = cache_dir / f"{self.result_id}.csv"

        self._df.to_csv(cache_file)

    def export_figure(self):
        def run():

            self.figure.export()

            image = Image.open(self.figure.figure_output_dir / self.figure.figure_id)
            image.show()

            # fig_files = list(self.figure.figure_output_dir.glob(pattern=f"{self.figure.figure_id}"))
            #
            # for fig in fig_files:
            #     image = Image.open(fig)
            #     image.show()

        thread = Thread(target=run)
        thread.start()

    def csv_output(self) -> Path:
        return Path(Configuration().result_dir / f"{self.result_id}.csv")

    def to_csv(self):

        if not Configuration().result_dir.exists():
            Configuration().result_dir.mkdir(parents=True)

        self.df.to_csv(self.csv_output())

    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            self._df = self.compute()

        return self._df

    @property
    @abstractmethod
    def result_id(self) -> str:
        pass

    @abstractmethod
    def _compute(self) -> pd.DataFrame:
        pass

    @property
    @abstractmethod
    def batch_name(self) -> str:
        pass

    @property
    def dtype(self) -> Dict:
        return {
            'rfid_lp': 'string',
            'rfid_np': 'string'
        }

    def initialize(self):
        pass

    # @staticmethod
    # def delete_cache(xp_name: str):
    #     base_dir = Configuration().get_base_dir()
    #     cache_dir = base_dir / "cache" / xp_name
    #
    #     rmtree(cache_dir)


class RFigure:

    def __init__(self, process: Process, script_name: str):
        self.logger = create_logger(self)
        self.process = process
        self.script_name = script_name

    @property
    def figure_output_dir(self) -> Path:
        return Configuration().result_dir

    @property
    @abstractmethod
    def figure_id(self) -> str:
        pass

    @property
    @abstractmethod
    def extra_args(self) -> Dict[str, str]:
        pass

    def export(self):

        self.process.to_csv()

        script_r = Path(f"..\..\scripts_R\{self.script_name}")
        output_file = self.figure_output_dir / self.figure_id

        res_dic = {
            "figure_file": str(output_file),
            "csv_file": str(self.process.csv_output().absolute())

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

        # p = subprocess.Popen(
        #     ["Rscript", "--vanilla",
        #      script_r,
        #      self.process.csv_output().absolute(),
        #      output_file.absolute(),
        #      extra],
        #     cwd=os.getcwd(),
        #     stdin=subprocess.PIPE,
        #     stdout=subprocess.PIPE,
        #     stderr=subprocess.PIPE
        # )

        output, error = p.communicate()

        if p.returncode == 0:
            print('R OUTPUT:\n {0}'.format(output.decode("utf-8")))
        else:
            print('R OUTPUT:\n {0}'.format(output.decode("utf-8")))
            print('R ERROR:\n {0}'.format(error.decode("utf-8")))
