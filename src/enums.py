import enum
from typing import List

from google.cloud.bigquery import SchemaField

from src.queries import (
    LINE_DIM_QUERY,
    STOP_DIM_QUERY,
    VEHICLE_DIM_QUERY,
    WEATHER_DIM_QUERY,
    TIME_DIM_QUERY,
    DELAY_FACT_QUERY,
)
from src.schemas import (
    LINE_DIM_SCHEMA,
    STOP_DIM_SCHEMA,
    VEHICLE_DIM_SCHEMA,
    WEATHER_DIM_SCHEMA,
    TIME_DIM_SCHEMA,
    DELAY_FACT_SCHEMA,
)


class Table(enum.Enum):
    LINE = ("LineDim", LINE_DIM_SCHEMA, LINE_DIM_QUERY)
    STOP = ("StopDim", STOP_DIM_SCHEMA, STOP_DIM_QUERY)
    VEHICLE = ("VehicleDim", VEHICLE_DIM_SCHEMA, VEHICLE_DIM_QUERY)
    WEATHER = ("WeatherDim", WEATHER_DIM_SCHEMA, WEATHER_DIM_QUERY)
    TIME = ("TimeDim", TIME_DIM_SCHEMA, TIME_DIM_QUERY)
    DELAY = ("DelayFact", DELAY_FACT_SCHEMA, DELAY_FACT_QUERY)

    def __init__(
        self,
        bigquery_table: str,
        schema: List[SchemaField],
        duckdb_query: str,
    ):
        self.bigquery_table = bigquery_table
        self.schema = schema
        self.duckdb_query = duckdb_query
