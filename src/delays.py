from typing import List

import duckdb
import pandas as pd
import pendulum
from azure.storage.blob import BlobServiceClient, ContainerClient

from src.blob_storage import get_csv_as_df

DELAYS_BUCKET = "traffic"


def _get_delay_files(
    container_client: ContainerClient,
    as_of: pendulum.Date,
) -> List[str]:
    prefix = as_of.strftime("%Y/%m/%d")
    return [
        blob.name
        for blob in container_client.list_blobs(name_starts_with=prefix)
        if blob.name.endswith(".csv")
    ]


def _merge_delay_files(
    container_client: ContainerClient,
    as_of: pendulum.Date,
) -> pd.DataFrame:
    files = _get_delay_files(container_client, as_of)
    dfs = [get_csv_as_df(container_client, f) for f in files]
    return pd.DataFrame() if not dfs else pd.concat(dfs)


def _normalize_delay(delay_str: str) -> int:
    sign = -1 if "min przed czasem" in delay_str else 1
    cleaned_str = delay_str.replace(" min przed czasem", "").replace(" min", "")
    return sign * int(cleaned_str)


# we use hourly granularity, truncating rest of the timestamp to be joinable to TimeDim timestamps
def _normalize_timestamp(timestamp_str: str) -> str:
    dt = pendulum.parse(timestamp_str)
    return dt.strftime("%Y-%m-%dT%H:00:00.000000")


def load_delays_into_duckdb(
    blob_service_client: BlobServiceClient,
    as_of: pendulum.Date,
    dbsession: duckdb.DuckDBPyConnection,
):
    container_client = blob_service_client.get_container_client(DELAYS_BUCKET)
    df = _merge_delay_files(container_client, as_of)

    df["Vehicle No"] = df["Vehicle No"].apply(lambda x: None if pd.isna(x) else str(x))
    df["Delay"] = df["Delay"].apply(_normalize_delay)
    df["Timestamp"] = df["Timestamp"].apply(_normalize_timestamp)

    tmp_view_name = "_tmp_delays"
    dbsession.register(tmp_view_name, df)
    dbsession.execute(
        f"create or replace table delays as select * from {tmp_view_name}"
    )
    dbsession.unregister(tmp_view_name)
