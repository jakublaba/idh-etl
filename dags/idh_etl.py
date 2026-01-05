import datetime
import os

import dotenv
import duckdb
import pandas as pd
from airflow.decorators import dag, task, task_group
from airflow.utils.log.logging_mixin import LoggingMixin
from azure.storage.blob import BlobServiceClient
from google.cloud import bigquery
from google.oauth2 import service_account
from pendulum import DateTime

from src.bigquery import write_df_to_bigquery
from src.delays import load_delays_into_duckdb
from src.enums import DimTable
from src.gtfs import load_gtfs_into_duckdb, GTFS_FILES
from src.time_utils import MONTH_MAP, get_season, get_time_of_day
from src.vehicles import load_vehicles_into_duckdb
from src.weather import load_weather_into_duckdb


def duckdb_path(logical_date: DateTime):
    return f"/tmp/idh-{logical_date.strftime('%Y%m%d_%H%M%S')}.duckdb"


DEFAULT_ARGS = {
    "retries": 5,
    "retry_delay": datetime.timedelta(seconds=30),
}


@dag(
    schedule="@daily",
    start_date=datetime.datetime(2024, 12, 8),
    end_date=datetime.datetime(2025, 1, 2),
    catchup=True,
    is_paused_upon_creation=True,
    default_args=DEFAULT_ARGS,
)
def idh_etl():
    dotenv.load_dotenv()
    az_blob_conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    gcp_credentials_file = "gcp-credentials.json"
    bigquery_project_id = os.getenv("BIGQUERY_PROJECT_ID")

    log = LoggingMixin().log

    blob_service_client = BlobServiceClient.from_connection_string(az_blob_conn_str)
    bigquery_client = bigquery.Client(
        credentials=service_account.Credentials.from_service_account_file(
            filename=gcp_credentials_file,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        ),
        project=bigquery_project_id,
    )

    @task_group
    def load_duckdb():
        @task
        def time(logical_date: DateTime):
            df = pd.DataFrame(
                {
                    "id": [int(logical_date.strftime("%Y%m%d"))],
                    "full_timestamp": [pd.to_datetime(logical_date)],
                    "hour_": [logical_date.hour],
                    "weekday": [logical_date.day_of_week.name],
                    "weekday_num": [logical_date.weekday() + 1],
                    "month_": [MONTH_MAP[logical_date.month]],
                    "month_num": [logical_date.month],
                    "season": [get_season(logical_date.month).value],
                    "year_": [logical_date.year],
                    "time_of_day": [get_time_of_day(logical_date.hour).value],
                    "is_business_day": [logical_date.weekday() < 5],
                }
            )

            with duckdb.connect(duckdb_path(logical_date)) as dbsession:
                tmp_view_name = "_tmp_time"
                dbsession.register(tmp_view_name, df)
                dbsession.execute("drop table if exists time_dim")
                dbsession.execute(
                    f"create table time_dim as select * from {tmp_view_name}"
                )
                dbsession.unregister(tmp_view_name)

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
        def delays(logical_date: DateTime):
            with duckdb.connect(duckdb_path(logical_date)) as dbsession:
                load_delays_into_duckdb(
                    blob_service_client,
                    logical_date.date(),
                    dbsession,
                )
                tables = dbsession.execute("show tables").df()
                log.info(f"Tables after load: {tables}")
            log.info("DELAYS loaded into DuckDB")

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

        @task
        def weather(logical_date: DateTime):
            with duckdb.connect(duckdb_path(logical_date)) as dbsession:
                load_weather_into_duckdb(
                    blob_service_client,
                    logical_date.date(),
                    dbsession,
                )
                tables = dbsession.execute("show tables").df()
                log.info(f"Tables after load: {tables}")
            log.info("WEATHER loaded into DuckDB")

        @task
        def verify(logical_date: DateTime):
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

        # sequentially to avoid having to configure duckdb concurrency
        time() >> gtfs() >> delays() >> vehicles() >> weather() >> verify()

    @task
    def write_dim_to_bigquery(
        table: DimTable,
        logical_date: DateTime,
    ):
        log.info(f"Writing {table.bigquery_table} to BigQuery")
        with duckdb.connect(duckdb_path(logical_date), read_only=True) as dbsession:
            write_df_to_bigquery(
                bigquery_client,
                dbsession.execute(table.duckdb_query).df(),
                table.schema,
                table.bigquery_table,
            )
        log.info(f"Successfully written {table.bigquery_table} to BigQuery")

    @task
    def clean_up_duckdb_file(logical_date: DateTime):
        path = duckdb_path(logical_date)
        if os.path.exists(path):
            os.remove(path)
            log.info(f"Removed DuckDB file at {path}")

    (
        load_duckdb()
        >> write_dim_to_bigquery.expand(table=list(DimTable))
        >> clean_up_duckdb_file()
    )


idh_etl()
