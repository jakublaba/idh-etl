from google.cloud.bigquery import SchemaField

LINE_DIM_SCHEMA = [
    SchemaField("id", "STRING", mode="REQUIRED"),
    SchemaField("line_label", "STRING", mode="REQUIRED"),
    SchemaField("operator", "STRING", mode="REQUIRED"),
    SchemaField("line_type", "STRING", mode="REQUIRED"),
    SchemaField("route_length_km", "FLOAT", mode="REQUIRED"),
    SchemaField("stops_amount", "INTEGER"),
]

STOP_DIM_SCHEMA = [
    SchemaField("id", "STRING", mode="REQUIRED"),
    SchemaField("name", "STRING", mode="REQUIRED"),
    SchemaField("lat", "FLOAT", mode="REQUIRED"),
    SchemaField("lon", "FLOAT", mode="REQUIRED"),
]

DELAY_DIM_SCHEMA = []

VEHICLE_DIM_SCHEMA = [
    SchemaField("id", "STRING", mode="REQUIRED"),
    SchemaField("brand", "STRING", mode="REQUIRED"),
    SchemaField("v_model", "STRING", mode="REQUIRED"),
    SchemaField("year_produced", "INT64", mode="REQUIRED"),
]

WEATHER_DIM_SCHEMA = [
    SchemaField("id", "STRING", mode="REQUIRED"),
    SchemaField("temperature", "FLOAT", mode="REQUIRED"),
    SchemaField("fall_mm", "INTEGER", mode="REQUIRED"),
    SchemaField("fall_type", "STRING", mode="REQUIRED"),
    SchemaField("wind_speed_mps", "INTEGER", mode="REQUIRED"),
    SchemaField("wind_direction_deg", "INTEGER", mode="REQUIRED"),
    SchemaField("humidity_percent", "FLOAT", mode="REQUIRED"),
    SchemaField("pressure_hpa", "INTEGER", mode="REQUIRED"),
    SchemaField("general_circumstances", "STRING", mode="REQUIRED"),
]
