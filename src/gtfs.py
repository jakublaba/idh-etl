from itertools import pairwise

import duckdb
import pendulum
from azure.storage.blob import ContainerClient, BlobServiceClient

from src.blob_storage import get_csv_as_df, date_prefixes_for_container

# we only need a subset of GTFS files for our analysis
GTFS_FILES = [
    "routes",
    "stop_times",
    "stops",
    "trips",
]
GTFS_FILE_EXTENSION = "csv"
GTFS_BUCKET = "gtfs"


def _load_gtfs_into_duckdb(
    container_client: ContainerClient,
    feed_prefix: str,
    dbsession: duckdb.DuckDBPyConnection,
):
    """
    Helper for loading a whole GTFS feed from Azure Blob Storage into a DuckDB session.

    :param container_client: Client pointing to the desired container (bucket).
    :param feed_prefix: Path/prefix pointing to the desired GTFS feed (without bucket), e.g. '2024/01/01/'.
    :param dbsession: Existing DuckDB session to use.
    """
    for file_name in GTFS_FILES:
        blob_name = f"{feed_prefix}/{file_name}.{GTFS_FILE_EXTENSION}"
        df = get_csv_as_df(
            container_client,
            blob_name,
        )
        temp_reg_name = f"_tmp_{file_name}"
        dbsession.execute(f"drop table if exists {file_name}")
        dbsession.register(temp_reg_name, df)
        dbsession.execute(f"create table {file_name} as select * from {temp_reg_name}")
        dbsession.unregister(temp_reg_name)


def load_gtfs_into_duckdb(
    blob_service_client: BlobServiceClient,
    as_of: pendulum.Date,
    dbsession: duckdb.DuckDBPyConnection,
):
    """
    Load GTFS data from Azure Blob Storage for the given date into a DuckDB session.
    Each file is loaded into a separate corresponding view, e.g. 'stops.txt/csv' -> 'stops'

    GTFS is only updated if it changes, so the exact date might be missing - in that case, the latest available feed
    before the given date is the correct one to load.

    :param blob_service_client: Client pointing to the desired Azure Blob Storage account.
    :param as_of: Date for which to load the GTFS feed.
    :param dbsession: Existing DuckDB session to use.
    """
    date_fmt = "YYYY/MM/DD"
    container_client = blob_service_client.get_container_client(GTFS_BUCKET)
    for p1, p2 in pairwise(date_prefixes_for_container(container_client)):
        d1 = pendulum.from_format(p1, date_fmt).date()
        d2 = pendulum.from_format(p2, date_fmt).date()

        if d1 <= as_of < d2:
            return _load_gtfs_into_duckdb(container_client, p1, dbsession)
        elif as_of == d2:
            return _load_gtfs_into_duckdb(container_client, p2, dbsession)

    raise ValueError(f"No GTFS feed available for {as_of}")
