import datetime
import os

import dotenv
import duckdb
from airflow.decorators import dag, task, task_group, short_circuit_task
from airflow.utils.log.logging_mixin import LoggingMixin
from azure.storage.blob import BlobServiceClient
from google.cloud import bigquery
from google.oauth2 import service_account
from pendulum import DateTime

from src.bigquery import write_df_to_bigquery
from src.enums import Table
from src.gtfs import load_gtfs_into_duckdb, GTFS_FILES
from src.queries import LINE_DIM_QUERY, STOP_DIM_QUERY, VEHICLE_DIM_QUERY
from src.schemas import LINE_DIM_SCHEMA, STOP_DIM_SCHEMA, VEHICLE_DIM_SCHEMA
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

        @short_circuit_task
        def delays(logical_date: DateTime):
            with duckdb.connect(duckdb_path(logical_date)) as dbsession:
                # todo
                pass
                tables = dbsession.execute("show tables").df()
                log.info(f"Tables after load: {tables}")
            log.info("DELAYS loaded into DuckDB")
            return True

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

        # sequentially to avoid having to configure duckdb concurrency
        gtfs() >> delays() >> vehicles()

    @task
    def weather_dim(logical_date: DateTime):
        log.info("Writing WeatherDim to BigQuery")

    @task
    def line_dim(logical_date: DateTime):
        log.info("Writing LineDim to BigQuery")
        with duckdb.connect(duckdb_path(logical_date)) as dbsession:
            write_df_to_bigquery(
                bigquery_client,
                dbsession.execute(LINE_DIM_QUERY).df(),
                LINE_DIM_SCHEMA,
                Table.LINE,
            )
        log.info("Successfully written LineDim to BigQuery")

    @task
    def stop_dim(logical_date: DateTime):
        log.info("Writing StopDim to BigQuery")
        with duckdb.connect(duckdb_path(logical_date)) as dbsession:
            write_df_to_bigquery(
                bigquery_client,
                dbsession.execute(STOP_DIM_QUERY).df(),
                STOP_DIM_SCHEMA,
                Table.STOP,
            )
        log.info("Successfully written StopDim to BigQuery")

    @task
    def vehicle_dim(logical_date: DateTime):
        log.info("Writing VehicleDim to BigQuery")
        with duckdb.connect(duckdb_path(logical_date)) as dbsession:
            write_df_to_bigquery(
                bigquery_client,
                dbsession.execute(VEHICLE_DIM_QUERY).df(),
                VEHICLE_DIM_SCHEMA,
                Table.VEHICLE,
            )
        log.info("Successfully written VehicleDim to BigQuery")

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

    (
        load_duckdb()
        >> verify_duckdb()
        # >> line_dim()
        >> stop_dim()
        >> vehicle_dim()
        >> clean_up_duckdb_file()
    )



idh_etl()
