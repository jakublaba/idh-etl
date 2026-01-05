import datetime
import os
from typing import Optional

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
from src.enums import Table
from src.gtfs import load_gtfs_into_duckdb, GTFS_FILES
from src.time_utils import MONTH_MAP, get_season, get_time_of_day
from src.vehicles import load_vehicles_into_duckdb
from src.weather import load_weather_into_duckdb

DUCKDB_SHARDS = {
    "time": ["time_dim"],
    "gtfs": GTFS_FILES,
    "delays": ["delays"],
    "vehicles": ["vehicles"],
    "weather": ["weather"],
}


def duckdb_path(logical_date: DateTime, shard: Optional[str] = None) -> str:
    path = f"/tmp/idh-{logical_date.strftime('%Y%m%d_%H%M%S')}.duckdb"
    if shard is not None:
        path = path.replace(".duckdb", f"-{shard}.duckdb")
    return path


DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": datetime.timedelta(seconds=30),
}


@dag(
    schedule="@hourly",
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

            with duckdb.connect(duckdb_path(logical_date, "time")) as dbsession:
                tmp_view_name = "_tmp_time"
                dbsession.register(tmp_view_name, df)
                dbsession.execute("drop table if exists time_dim")
                dbsession.execute(
                    f"create table time_dim as select * from {tmp_view_name}"
                )
                dbsession.unregister(tmp_view_name)

        @task
        def gtfs(logical_date: DateTime):
            with duckdb.connect(duckdb_path(logical_date, "gtfs")) as dbsession:
                load_gtfs_into_duckdb(
                    blob_service_client,
                    logical_date.date(),
                    dbsession,
                )
            log.info("GTFS loaded into DuckDB")

        @task
        def delays(logical_date: DateTime):
            with duckdb.connect(duckdb_path(logical_date, "delays")) as dbsession:
                load_delays_into_duckdb(
                    blob_service_client,
                    logical_date.date(),
                    dbsession,
                )
            log.info("DELAYS loaded into DuckDB")

        @task
        def vehicles(logical_date: DateTime):
            with duckdb.connect(duckdb_path(logical_date, "vehicles")) as dbsession:
                load_vehicles_into_duckdb(
                    blob_service_client,
                    dbsession,
                )
            log.info("VEHICLES loaded into DuckDB")

        @task
        def weather(logical_date: DateTime):
            with duckdb.connect(duckdb_path(logical_date, "weather")) as dbsession:
                load_weather_into_duckdb(
                    blob_service_client,
                    logical_date.date(),
                    dbsession,
                )
            log.info("WEATHER loaded into DuckDB")

        @task
        def merge_shards(logical_date: DateTime):
            target_path = duckdb_path(logical_date)
            with duckdb.connect(target_path) as dbsession:
                for shard_name, tables in DUCKDB_SHARDS.items():
                    log.info(f"Merging shard: {shard_name}")
                    shard_path = duckdb_path(logical_date, shard_name)
                    if not os.path.exists(shard_path):
                        log.warning(
                            f"Shard path does not exist: {shard_path}, skipping"
                        )
                        continue
                    shard_alias = f"shard_{shard_name}"
                    dbsession.execute(
                        f"attach database '{shard_path}' as {shard_alias}"
                    )

                    for t in tables:
                        dbsession.execute(
                            f"create or replace table {t} as select * from {shard_alias}.{t}"
                        )

                    dbsession.execute(f"detach database {shard_alias}")
                    os.remove(shard_path)
                    log.info(
                        f"Successfully merged {shard_alias} and removed {shard_path}"
                    )

        @task
        def verify(logical_date: DateTime):
            tables = [*GTFS_FILES, "delays", "vehicles", "weather", "time_dim"]
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

        [time(), gtfs(), delays(), vehicles(), weather()] >> merge_shards() >> verify()

    @task
    def write_table_to_bigquery(
        table: Table,
        logical_date: DateTime,
    ):
        log.info(f"Writing {table.bigquery_table} to BigQuery")
        with duckdb.connect(duckdb_path(logical_date), read_only=True) as dbsession:
            df = dbsession.execute(table.duckdb_query).df()
            log.info(f"Writing {len(df)} rows to {table.bigquery_table}")
            log.info(f"Sample: {df.head(10)}")
            write_df_to_bigquery(
                bigquery_client,
                df,
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
        >> write_table_to_bigquery.expand(table=list(Table))
        >> clean_up_duckdb_file()
    )


idh_etl()
