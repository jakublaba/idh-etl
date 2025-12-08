import datetime
from airflow.decorators import dag, task, task_group
from airflow.utils.log.logging_mixin import LoggingMixin
from pendulum import DateTime


@dag(
    schedule="@hourly",
    start_date=datetime.datetime(2024, 12, 1),
    end_date=datetime.datetime(2025, 1, 2),
    catchup=True,
    is_paused_upon_creation=True,
)
def idh_etl():
    log = LoggingMixin().log

    @task
    def time_dim(logical_date: DateTime):
        log.info(f"Logical date: {logical_date}")

    @task_group
    def load_duckdb():
        @task
        def gtfs(logical_date: DateTime):
            pass

        @task
        def traffic(logical_date: DateTime):
            pass

        @task
        def vehicles(logical_date: DateTime):
            pass

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
