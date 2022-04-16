from abc import ABC
from argparse import ArgumentParser
from functools import cached_property

from app.abc.base import SparkBaseApp
from constants import BUCKET_NAME


class SparkBaseAppCryptocurrency(SparkBaseApp, ABC):  # pragma: no cover
    @cached_property
    def get_path_prefix(
        self,
    ) -> str:
        return f"s3a://{BUCKET_NAME}"

    def get_arg_parser(self) -> ArgumentParser:
        arg_parser = ArgumentParser()

        return arg_parser
