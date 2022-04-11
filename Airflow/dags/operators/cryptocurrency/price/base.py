from abc import ABC
from typing import Final

from airflow.models import BaseOperator
from constants import CRYPTO_COMPARE_V2_HTTP_CONN_ID


class CryptocurrencyBaseOperator(BaseOperator, ABC):
    cryptocurrency_http_conn_id: Final[str] = CRYPTO_COMPARE_V2_HTTP_CONN_ID
