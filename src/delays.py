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


def load_delays_into_duckdb(
    blob_service_client: BlobServiceClient,
    as_of: pendulum.Date,
    dbsession: duckdb.DuckDBPyConnection,
):
    container_client = blob_service_client.get_container_client(DELAYS_BUCKET)
    df = _merge_delay_files(container_client, as_of)

    # Ensure 'Vehicle No' is always a string (preserve missing values as None)
    if "Vehicle No" in df.columns:
        # Convert values to Python str while keeping NaNs as None so DuckDB sees them as NULLs
        df["Vehicle No"] = df["Vehicle No"].apply(
            lambda x: None if pd.isna(x) else str(x)
        )

    tmp_view_name = "_tmp_delays"
    dbsession.register(tmp_view_name, df)
    dbsession.execute("drop table if exists delays")
    dbsession.execute("create table delays as select * from _tmp_delays")
    dbsession.unregister(tmp_view_name)
