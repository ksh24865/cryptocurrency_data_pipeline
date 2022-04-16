from contextlib import contextmanager
from typing import Iterator

from pyspark.sql import SparkSession


@contextmanager
def get_spark_session(
    app_name: str = "AnonymousLaplaceSparkApp",
) -> Iterator[SparkSession]:  # pragma: no cover
    spark_session = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    try:
        yield spark_session
    finally:
        spark_session.stop()
