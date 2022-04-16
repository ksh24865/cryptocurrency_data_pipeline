from typing import List  # pragma: no cover

from pyspark.sql import DataFrame  # pragma: no cover


def check_nested_column_if_exists(
    df: DataFrame, column_name: str
) -> bool:  # pragma: no cover
    def check_column_if_exists(
        now_df: DataFrame, nested_column_list: List[str]
    ) -> bool:
        if not nested_column_list:
            return False
        now_column = nested_column_list.pop(0)
        if now_column in now_df.columns:
            if not nested_column_list:
                return True
            return check_column_if_exists(
                now_df=now_df.select(f"{now_column}.*"),
                nested_column_list=nested_column_list,
            )
        else:
            return False

    return check_column_if_exists(
        now_df=df,
        nested_column_list=column_name.split("."),
    )
