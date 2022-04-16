from abc import ABC
from argparse import ArgumentParser, Namespace

from app.abc.base_cryptocurrency import SparkBaseAppCryptocurrency
from pyspark.sql import DataFrame


class SparkBaseAppCryptoCurrencyTmpToSrc(
    SparkBaseAppCryptocurrency, ABC
):  # pragma: no cover
    def get_arg_parser(self) -> ArgumentParser:
        arg_parser = super().get_arg_parser()
        arg_parser.add_argument("--operation-date-str", required=True)
        return arg_parser

    def get_src_path(self, args: Namespace) -> str:
        path_prefix = self.get_path_prefix

        return f"{path_prefix}/_tmp/{args.operation_date_str}"

    def get_dest_path(self, args: Namespace) -> str:
        path_prefix = self.get_path_prefix
        return f"{path_prefix}/sourcing"

    def read(self, path: str) -> DataFrame:
        return self.spark_session.read.format("json").load(path)
