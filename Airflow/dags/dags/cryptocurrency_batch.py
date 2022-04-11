from typing import Final, Optional, TypedDict

from airflow import DAG

# from airflow.operators.python import PythonOperator
from constants import CRYPTOCURRENCY_BUCKET_NAME
from operators.cryptocurrency.price.sourcing_batch_daily import (
    CryptocurrencyPriceSourcingBatchDailyOperator,
)
from operators.cryptocurrency.price.sourcing_batch_hourly import (
    CryptocurrencyPriceSourcingBatchHourlyOperator,
)
from operators.cryptocurrency.price.sourcing_batch_minutely import (
    CryptocurrencyPriceSourcingBatchMinutelyOperator,
)
from utils.cryptocurrency.top_list import (  # top_symbol_list_by_market_cap_generator,
    TOP_SYMBOL_LIST_BY_MARKET_CAP,
)
from utils.date import utc_to_kst

DAG_ID: Final[str] = "cryptocurrency_batch"

default_args = {
    "owner": "sam",
    "start_date": "2022-03-20T00:00:00Z",
}

SOURCING_DAILY_TASK_ID: Final[str] = "sourcing_daily"
SOURCING_HOURLY_TASK_ID: Final[str] = "sourcing_hourly"
SOURCING_MINUTELY_TASK_ID: Final[str] = "sourcing_minutely"


class BatchInfo(TypedDict):
    batch_type: str
    start_date: Optional[str]
    current_date: Optional[str]


class DagRunConf(TypedDict):
    account_id: Optional[str]
    account_name: Optional[str]
    data_category: str
    sourcing_method: str
    provider: str
    user_id: Optional[int]
    batch_info: BatchInfo


def data_refresh(t):
    print(f"t:{t}")


CRYPTOCURRENCY_PRICE_SOURCING_BATCH_TASK_ID_PREFIX = (
    "cryptocurrency_price_sourcing_batch_task"
)


with DAG(
    dag_id=DAG_ID,
    description="cryptocurrency_batch",
    default_args=default_args,
    schedule_interval=None,
    user_defined_macros={
        "utc_to_kst": utc_to_kst,
    },
    render_template_as_native_obj=True,
) as dag:
    # data_refresh_tasks = [
    #     PythonOperator(
    #         task_id=f"{CRYPTOCURRENCY_PRICE_SOURCING_BATCH_TASK_ID_PREFIX}_{idx}",
    #         python_callable=data_refresh,
    #         op_kwargs={"t": top_symbol_list_by_market_cap},
    #     )
    #     for idx, top_symbol_list_by_market_cap in enumerate(
    #         top_symbol_list_by_market_cap_generator()
    #     )
    # ]

    sourcing_daily = CryptocurrencyPriceSourcingBatchDailyOperator(
        task_id=SOURCING_DAILY_TASK_ID,
        bucket_name=CRYPTOCURRENCY_BUCKET_NAME,
        symbol_list=TOP_SYMBOL_LIST_BY_MARKET_CAP,
        execution_date="{{ utc_to_kst(ts) }}",
    )

    sourcing_hourly = CryptocurrencyPriceSourcingBatchHourlyOperator(
        task_id=SOURCING_HOURLY_TASK_ID,
        bucket_name=CRYPTOCURRENCY_BUCKET_NAME,
        symbol_list=TOP_SYMBOL_LIST_BY_MARKET_CAP,
        execution_date="{{ utc_to_kst(ts) }}",
    )

    sourcing_minutely = CryptocurrencyPriceSourcingBatchMinutelyOperator(
        task_id=SOURCING_MINUTELY_TASK_ID,
        bucket_name=CRYPTOCURRENCY_BUCKET_NAME,
        symbol_list=TOP_SYMBOL_LIST_BY_MARKET_CAP,
        execution_date="{{ utc_to_kst(ts) }}",
    )

    # data_refresh_tasks >> cryp
