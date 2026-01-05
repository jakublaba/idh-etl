import datetime
import os

import dotenv
import duckdb
from airflow.decorators import dag, task, task_group
from airflow.utils.log.logging_mixin import LoggingMixin
from azure.storage.blob import BlobServiceClient
from google.cloud import bigquery
from google.oauth2 import service_account
from pendulum import DateTime

from src.gtfs import load_gtfs_into_duckdb, GTFS_FILES
from src.vehicles import load_vehicles_into_duckdb


def duckdb_path(logical_date: DateTime):
    return f"/tmp/idh-{logical_date.strftime('%Y%m%d_%H%M%S')}.duckdb"


@dag(
    schedule="@hourly",
    start_date=datetime.datetime(2024, 12, 7),
    end_date=datetime.datetime(2024, 12, 31),
    catchup=True,
    is_paused_upon_creation=True,
)
def idh_etl():
    dotenv.load_dotenv()
    az_blob_conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    gcp_credentials_file = "gcp-credentials.json"
    bigquery_project_id = os.getenv("BIGQUERY_PROJECT_ID")
    dataset_id = os.getenv("DATASET_ID")

    log = LoggingMixin().log

    blob_service_client = BlobServiceClient.from_connection_string(az_blob_conn_str)
    bigquery_client = bigquery.Client(
        credentials=service_account.Credentials.from_service_account_file(
            filename=gcp_credentials_file,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        ),
        project=bigquery_project_id,
    )

    @task
    def time_dim(logical_date: DateTime):
        log.info(f"Logical date: {logical_date}")

    @task_group
    def load_duckdb():
        @task
        def gtfs(logical_date: DateTime):
            with duckdb.connect(duckdb_path(logical_date)) as dbsession:
                load_gtfs_into_duckdb(
                    blob_service_client,
                    logical_date.date(),
                    dbsession,
                )
                tables = dbsession.execute("show tables").df()
                log.info(f"Tables after load: {tables}")
            log.info("GTFS loaded into DuckDB")

        @task
        def traffic(logical_date: DateTime):
            pass

        @task
        def vehicles(logical_date: DateTime):
            with duckdb.connect(duckdb_path(logical_date)) as dbsession:
                load_vehicles_into_duckdb(
                    blob_service_client,
                    dbsession,
                )
                tables = dbsession.execute("show tables").df()
                log.info(f"Tables after load: {tables}")
            log.info("VEHICLES loaded into DuckDB")

        gtfs() >> traffic() >> vehicles()

    @task
    def verify_duckdb(logical_date: DateTime):
        tables = [*GTFS_FILES, "vehicles"]
        with duckdb.connect(duckdb_path(logical_date)) as dbsession:
            show_tables = dbsession.execute("show tables").df()
            log.info(f"Tables at verification step: {show_tables}")
            for t in tables:
                log.info(f"Verifying table: {t}")
                try:
                    dbsession.execute(f"select * from {t} limit 1").df()
                    log.info(f"Successfully queried table: {t}")
                except Exception as e:
                    log.error(f"Failed to query table: {t} - {e}")

    @task
    def clean_up_duckdb_file(logical_date: DateTime):
        path = duckdb_path(logical_date)
        if os.path.exists(path):
            os.remove(path)
            log.info(f"Removed DuckDB file at {path}")

    load_duckdb() >> verify_duckdb() >> clean_up_duckdb_file()


idh_etl()
