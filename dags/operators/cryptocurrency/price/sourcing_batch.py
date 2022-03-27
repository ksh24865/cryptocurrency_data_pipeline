from contextlib import contextmanager
from dataclasses import asdict, dataclass
from io import BytesIO
from typing import Any, Dict, Iterable, List, Optional

from airflow.models.taskinstance import Context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from constants import CRYPTO_COMPARE_TO_SYMBOL
from hooks.wrappers.http_stream import HttpStreamHook
from operators.cryptocurrency.price.base import CryptocurrencyBaseOperator
from utils.cryptocurrency.top_list import TOP_SYMBOL_LIST_BY_MARKET_CAP
from utils.request import get_request_temporary_file
from utils.s3 import upload_file_s3


@dataclass
class CryptocurrencyPriceApiData:
    limit: int
    toTs: int
    fsym: str
    aggregate: int = 1
    tsym: str = CRYPTO_COMPARE_TO_SYMBOL


class CryptocurrencyPriceSourcingBatchOperator(CryptocurrencyBaseOperator):
    template_fields = ("execution_date",)

    def __init__(
        self,
        bucket_name: str,
        symbol_list: List[str],
        execution_date: str,
        api_chunk_size: int = 2000,
        api_aggregate: int = 1,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.symbol_list = symbol_list
        self.execution_date = execution_date
        self.http_hook = HttpStreamHook(
            http_conn_id=self.cryptocurrency_http_conn_id,
        )
        self.s3_hook = S3Hook()
        self.api_chunk_size = api_chunk_size
        self.api_aggregate = api_aggregate

    @contextmanager
    def read(
        self,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        back_off_base: Optional[float] = None,
        back_off_cap: Optional[float] = None,
        headers: Optional[Dict[str, Any]] = None,
    ):
        with get_request_temporary_file(
            http_hook=self.http_hook,
            endpoint=endpoint,
            data=data,
            back_off_base=back_off_base,
            back_off_cap=back_off_cap,
            headers=headers,
        ) as f:
            yield f

    def write(
        self,
        temporary_file_io: BytesIO,
        key: str,
    ) -> None:
        upload_file_s3(
            s3_hook=self.s3_hook,
            bucket_name=self.bucket_name,
            key=key,
            file_obj=temporary_file_io,
        )

    @property
    def api_endpoint(self) -> str:
        raise NotImplementedError()

    def timestamp_generator(self) -> Any:
        raise NotImplementedError()

    def api_data_generator(
        self,
        symbol: str,
    ) -> Iterable[CryptocurrencyPriceApiData]:
        for timestamp, limit in self.timestamp_generator():
            yield CryptocurrencyPriceApiData(
                fsym=symbol,
                aggregate=self.api_aggregate,
                limit=limit,
                toTs=timestamp,
            )

    def execute(self, context: Context) -> str:
        for data in self.api_data_generator(symbol=TOP_SYMBOL_LIST_BY_MARKET_CAP[0]):
            with self.read(
                endpoint=self.api_endpoint,
                data=asdict(data),
                back_off_cap=0,
                back_off_base=0,
            ) as f:
                self.write(temporary_file_io=f, key="test/test.json")

        #
        # for symbol in self.symbol_list:
        #     with temporary_file() as f:
        #
        #         symbol
        #
        # with http_hook.run(
        #     endpoint="histoday?fsym=BTC&tsym=USD&limit=30&aggregate=1&toTs=1280160054",
        # ) as r:
        #     r.raise_for_status()
        #     print(f"response:{r.json()}")
