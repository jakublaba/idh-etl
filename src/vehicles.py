import duckdb
from azure.storage.blob import BlobServiceClient

from src.blob_storage import get_csv_as_df

VEHICLES_BUCKET = "vehicles"
VEHICLES_FILE_NAME = "ztm_vehicles_detailed.csv"


def load_vehicles_into_duckdb(
    blob_service_client: BlobServiceClient,
    dbsession: duckdb.DuckDBPyConnection,
):
    container_client = blob_service_client.get_container_client(VEHICLES_BUCKET)
    df = get_csv_as_df(
        container_client,
        VEHICLES_FILE_NAME,
    )
    dbsession.register("vehicles", df)
