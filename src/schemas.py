from google.cloud.bigquery import SchemaField

LINE_DIM_SCHEMA = [
    SchemaField("id", "STRING", mode="REQUIRED"),
    SchemaField("line_label", "STRING", mode="REQUIRED"),
    SchemaField("operator", "STRING", mode="REQUIRED"),
    SchemaField("line_type", "STRING", mode="REQUIRED"),
    SchemaField("route_length_km", "FLOAT", mode="REQUIRED"),
    SchemaField("stops_amount", "INTEGER"),
]

STOP_DIM_SCHEMA = []

VEHICLE_DIM_SCHEMA = []
