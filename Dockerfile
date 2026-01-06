FROM quay.io/astronomer/astro-runtime:13.3.0

WORKDIR /usr/local/airflow

COPY gcp-credentials.json .

RUN mkdir -p /usr/local/airflow/duckdb

VOLUME ["/usr/local/airflow/duckdb"]
