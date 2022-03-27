from datetime import timedelta
from typing import Final, Iterable, Tuple

from constants import CRYPTO_COMPARE_OLDEST_DATE
from operators.cryptocurrency.price.sourcing_batch import (
    CryptocurrencyPriceSourcingBatchOperator,
)
from utils.date import (
    date_range,
    datetime_to_timestamp,
    get_date_years_before,
    str_to_date,
)


class CryptocurrencyPriceSourcingBatchHourlyOperator(
    CryptocurrencyPriceSourcingBatchOperator
):
    YEARS_BEFORE_FOR_CRYPTOCURRENCY_PRICE_DAILY: Final[int] = 7

    @property
    def api_endpoint(self):
        return "histohour"

    def timestamp_generator(self) -> Iterable[Tuple[int, timedelta]]:
        execution_date = str_to_date(self.execution_date)
        for start_date, end_date in date_range(
            start_date=CRYPTO_COMPARE_OLDEST_DATE,
            end_date=get_date_years_before(
                current_date=execution_date,
                years=self.YEARS_BEFORE_FOR_CRYPTOCURRENCY_PRICE_DAILY,
            ),
            time_interval=self.api_chunk_size,
        ):
            to_ts = datetime_to_timestamp(end_date)
            days_interval = (end_date - start_date).days
            yield to_ts, days_interval
