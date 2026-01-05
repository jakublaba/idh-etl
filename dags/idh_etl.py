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

from src.bigquery import write_df_to_bigquery
from src.enums import Table
from src.gtfs import load_gtfs_into_duckdb
from src.queries import LINE_DIM_QUERY, STOP_DIM_QUERY, VEHICLE_DIM_QUERY
from src.schemas import LINE_DIM_SCHEMA, STOP_DIM_SCHEMA, VEHICLE_DIM_SCHEMA
from src.vehicles import load_vehicles_into_duckdb


@dag(
    schedule="@hourly",
    start_date=datetime.datetime(2024, 12, 8),
    end_date=datetime.datetime(2025, 1, 2),
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

    # in-mem db by default - this is fine
    dbsession = duckdb.connect()

    @task
    def time_dim(logical_date: DateTime):
        log.info(f"Logical date: {logical_date}")

    @task_group
    def load_duckdb():
        @task
        def gtfs(logical_date: DateTime):
            load_gtfs_into_duckdb(
                blob_service_client,
                logical_date.date(),
                dbsession,
            )
            log.info("GTFS loaded into DuckDB")

        @task
        def traffic(logical_date: DateTime):
            pass

        @task
        def vehicles():
            load_vehicles_into_duckdb(
                blob_service_client,
                dbsession,
            )
            log.info("VEHICLES loaded into DuckDB")

        [gtfs(), traffic(), vehicles()]

    @task
    def weather_dim(logical_date: DateTime):
        pass

    @task
    def line_dim():
        write_df_to_bigquery(
            bigquery_client=bigquery_client,
            df=dbsession.sql(LINE_DIM_QUERY).df(),
            schema=LINE_DIM_SCHEMA,
            table=Table.LINE,
        )

    @task
    def stop_dim():
        write_df_to_bigquery(
            bigquery_client=bigquery_client,
            df=dbsession.sql(STOP_DIM_QUERY).df(),
            schema=STOP_DIM_SCHEMA,
            table=Table.STOP,
        )

    @task
    def vehicle_dim():
        write_df_to_bigquery(
            bigquery_client=bigquery_client,
            df=dbsession.sql(VEHICLE_DIM_QUERY).df(),
            schema=VEHICLE_DIM_SCHEMA,
            table=Table.VEHICLE,
        )

    @task
    def delay_fact(logical_date: DateTime):
        pass

    delay_fact = delay_fact()

    load_duckdb() >> [line_dim(), stop_dim(), vehicle_dim()] >> delay_fact
    [time_dim(), weather_dim()] >> delay_fact


idh_etl()
