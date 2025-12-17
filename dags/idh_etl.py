import datetime
import os

import dotenv
import duckdb
from airflow.decorators import dag, task, task_group
from airflow.utils.log.logging_mixin import LoggingMixin
from azure.storage.blob import BlobServiceClient
from pendulum import DateTime

from src.gtfs import load_gtfs_into_duckdb
from src.vehicles import load_vehicles_into_duckdb


@dag(
    schedule="@hourly",
    start_date=datetime.datetime(2024, 12, 1),
    end_date=datetime.datetime(2025, 1, 2),
    catchup=True,
    is_paused_upon_creation=True,
)
def idh_etl():
    dotenv.load_dotenv()
    az_blob_conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

    log = LoggingMixin().log

    blob_service_client = BlobServiceClient.from_connection_string(az_blob_conn_str)

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
                logical_date,
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
    def line_dim(logical_date: DateTime):
        pass

    @task
    def stop_dim(logical_date: DateTime):
        pass

    @task
    def vehicle_dim(logical_date: DateTime):
        pass

    @task
    def delay_fact(logical_date: DateTime):
        pass

    delay_fact = delay_fact()

    load_duckdb() >> [line_dim(), stop_dim(), vehicle_dim()] >> delay_fact
    [time_dim(), weather_dim()] >> delay_fact


idh_etl()
