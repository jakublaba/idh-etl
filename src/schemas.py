from google.cloud.bigquery import SchemaField

LINE_DIM_SCHEMA = [
    SchemaField("id", "STRING", mode="REQUIRED"),
    SchemaField("operator", "STRING"),
    SchemaField("line_type", "STRING", mode="REQUIRED"),
    SchemaField("route_length_km", "FLOAT", mode="REQUIRED"),
    SchemaField("stops_amount", "INT64", mode="REQUIRED"),
]

STOP_DIM_SCHEMA = [
    SchemaField("id", "STRING", mode="REQUIRED"),
    SchemaField("name", "STRING", mode="REQUIRED"),
    SchemaField("lat", "FLOAT", mode="REQUIRED"),
    SchemaField("lon", "FLOAT", mode="REQUIRED"),
]

VEHICLE_DIM_SCHEMA = [
    SchemaField("id", "STRING", mode="REQUIRED"),
    SchemaField("brand", "STRING", mode="REQUIRED"),
    SchemaField("v_model", "STRING", mode="REQUIRED"),
    SchemaField("year_produced", "INT64", mode="REQUIRED"),
]

WEATHER_DIM_SCHEMA = [
    SchemaField("id", "STRING", mode="REQUIRED"),
    SchemaField("temperature", "FLOAT", mode="REQUIRED"),
    SchemaField("fall_mm", "INT64", mode="REQUIRED"),
    SchemaField("fall_type", "STRING", mode="REQUIRED"),
    SchemaField("wind_speed_mps", "INT64", mode="REQUIRED"),
    SchemaField("wind_direction_deg", "INT64", mode="REQUIRED"),
    SchemaField("humidity_percent", "FLOAT", mode="REQUIRED"),
    SchemaField("pressure_hpa", "INT64", mode="REQUIRED"),
    SchemaField("general_circumstances", "STRING", mode="REQUIRED"),
]

TIME_DIM_SCHEMA = [
    SchemaField("id", "INT64", mode="REQUIRED"),
    SchemaField("full_timestamp", "TIMESTAMP", mode="REQUIRED"),
    SchemaField("hour_", "INT64", mode="REQUIRED"),
    SchemaField("weekday", "STRING", mode="REQUIRED"),
    SchemaField("weekday_num", "INT64", mode="REQUIRED"),
    SchemaField("month_", "STRING", mode="REQUIRED"),
    SchemaField("month_num", "INT64", mode="REQUIRED"),
    SchemaField("season", "STRING", mode="REQUIRED"),
    SchemaField("year_", "INT64", mode="REQUIRED"),
    SchemaField("time_of_day", "STRING", mode="REQUIRED"),
    SchemaField("is_business_day", "BOOL", mode="REQUIRED"),
]

DELAY_FACT_SCHEMA = [
    SchemaField("delay_mins", "INT64", mode="REQUIRED"),
    SchemaField("time_id", "INT64", mode="REQUIRED"),
    SchemaField("weather_id", "STRING", mode="REQUIRED"),
    SchemaField("vehicle_id", "STRING", mode="REQUIRED"),
    SchemaField("line_id", "STRING", mode="REQUIRED"),
    SchemaField("stop_id", "STRING", mode="REQUIRED"),
]
