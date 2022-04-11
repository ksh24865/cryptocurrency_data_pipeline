from dataclasses import asdict, dataclass
from datetime import date
from typing import Any, Dict, Final, Iterable, List, Optional, Tuple, Union

from airflow.models.taskinstance import Context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.hooks.http import HttpHook
from constants import CRYPTO_COMPARE_TO_SYMBOL
from hooks.wrappers.http_stream import HttpStreamHook
from operators.cryptocurrency.price.base import CryptocurrencyBaseOperator
from utils.cryptocurrency.top_list import TOP_SYMBOL_LIST_BY_MARKET_CAP
from utils.date import date_range, datetime_to_timestamp
from utils.exception import raise_airflow_exception
from utils.request import get_request_json
from utils.s3 import upload_json_to_s3


@dataclass
class CryptocurrencyPriceApiData:
    limit: int
    toTs: int
    fsym: str
    aggregate: int = 1
    tsym: str = CRYPTO_COMPARE_TO_SYMBOL


class CryptocurrencyPriceSourcingBatchOperator(CryptocurrencyBaseOperator):
    YEARS_BEFORE_FOR_CRYPTOCURRENCY_PRICE_DAILY: Final[int] = 7
    DAYS_BEFORE_FOR_CRYPTOCURRENCY_PRICE_DAILY: Final[int] = 7

    template_fields = ("execution_date",)

    def __init__(
        self,
        bucket_name: str,
        symbol_list: List[str],
        execution_date: str,
        api_chunk_size: int = 2000,
        api_aggregate: int = 1,
        back_off_base: float = 0,
        back_off_cap: float = 0,
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
        self.back_off_base = back_off_base
        self.back_off_cap = back_off_cap

    @property
    def batch_unit(self):
        raise NotImplementedError()

    @property
    def a_day_per_batch_unit(self):
        raise NotImplementedError()

    @property
    def time_interval(self):
        return self.api_chunk_size // self.a_day_per_batch_unit

    @property
    def api_endpoint(self) -> str:
        raise NotImplementedError()

    @property
    def start_date_of_date_range(self) -> date:
        raise NotImplementedError()

    @property
    def end_date_of_date_range(self) -> date:
        raise NotImplementedError()

    def try_to_get_request_json(
        self,
        http_hook: Union[HttpHook, HttpStreamHook],
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
        retry_count: int = 1,
        err_msg: str = "",
    ) -> Dict[str, Any]:
        if retry_count <= 0:
            raise_airflow_exception(
                error_msg=err_msg,
                logger=self.log,
            )
        response_json = get_request_json(
            http_hook=http_hook,
            endpoint=endpoint,
            data=data,
            headers=headers,
            back_off_cap=self.back_off_cap,
            back_off_base=self.back_off_base,
        )

        response_status = response_json.get("Response")
        if response_status == "Success":
            return response_json
        response_message = response_json.get("Message")
        return self.try_to_get_request_json(
            http_hook=http_hook,
            endpoint=endpoint,
            data=data,
            retry_count=retry_count - 1,
            err_msg=f"{err_msg} retry_count : {retry_count}\nerr_msg : {response_message} \n\n",
        )

    def read(
        self,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
    ):
        return self.try_to_get_request_json(
            http_hook=self.http_hook,
            endpoint=endpoint,
            data=data,
            headers=headers,
        )

    def write(
        self,
        json_data: Dict[str, Any],
        key: str,
    ) -> None:
        upload_json_to_s3(
            s3_hook=self.s3_hook,
            bucket_name=self.bucket_name,
            data_key=key,
            json_data=json_data,
        )

    def timestamp_generator(self) -> Iterable[Tuple[int, int]]:
        for start_date, end_date in date_range(
            start_date=self.start_date_of_date_range,
            end_date=self.end_date_of_date_range,
            time_interval=self.time_interval,
        ):
            print(f"start_date ~ end_date: {start_date} ~ {end_date}")
            to_ts = datetime_to_timestamp(end_date)
            days_interval = (end_date - start_date).days
            yield to_ts, days_interval * self.a_day_per_batch_unit

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

    def execute(self, context: Context) -> None:

        data_idx = 0
        for symbol in TOP_SYMBOL_LIST_BY_MARKET_CAP:
            data_key_prefix = f"test/test/{symbol}/{self.batch_unit}"
            for data in self.api_data_generator(symbol=symbol):
                json_data = self.read(
                    endpoint=self.api_endpoint,
                    data=asdict(data),
                )
                self.write(
                    json_data=json_data, key=f"{data_key_prefix}/{data_idx}.json"
                )
                data_idx += 1
