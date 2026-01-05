import os
from typing import List

import dotenv
import pandas as pd
from google.cloud import bigquery

dotenv.load_dotenv()
PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
DATESET_ID = os.getenv("DATASET_ID")


def write_df_to_bigquery(
    bigquery_client: bigquery.Client,
    df: pd.DataFrame,
    schema: List[bigquery.SchemaField],
    bigquery_table: str,
):
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    write_job = bigquery_client.load_table_from_dataframe(
        dataframe=df,
        destination=f"{PROJECT_ID}.{DATESET_ID}.{bigquery_table}",
        job_config=job_config,
    )

    write_job.result()
