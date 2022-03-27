from io import BytesIO

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def upload_file_s3(
    s3_hook: S3Hook,
    bucket_name: str,
    key: str,
    file_obj: BytesIO,
) -> None:
    s3_hook.load_file_obj(
        file_obj=file_obj,
        key=key,
        bucket_name=bucket_name,
    )
