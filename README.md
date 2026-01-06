# ETL project for Data Warehouse course on Polish-Japanese Academy of Information Technology (Winter 2025/26)

## How to run?
[How to install Astro CLI](https://www.astronomer.io/docs/astro/cli/overview)

A lot of astronomer junk is `.gitignore`d, init the astro stuff first:

```sh
astro dev init
```

Then run with 
```sh
astro dev start
```

Astro stores airflow data in some automatically created volumes, so if you want to start over you'll have to
locate and delete them manually, e.g.
```sh
docker volume ls
DRIVER    VOLUME NAME
local     a357edfa27e78e1ce9452643cd0f057ed160e9f6d0aa9761cd6b942c36e88ac8
local     b8306eaaa9c6ad67b597a8ff6b10953666b1ba29008793b94cbcc6f5dd3991db
local     idh-etl_e5e8b9_airflow_logs
local     idh-etl_e5e8b9_postgres_data
```

```sh
docker volume rm idh-etl_e5e8b9_airflow_logs idh-etl_e5e8b9_postgres_data ...
```

Or you can just nuke every volume on your system if you're lazy (`docker system prune -af --volumes` misses them for some reason):
```sh
for v in `docker volume ls -q` ; do
docker volume rm $v
done
```

## Connecting to DuckDB for debugging

*Note: Your jobs will get permission errors if the mount point for duckdb files is not writable (reasonable) and executable (insane).*
Before running the job, make sure to create it manually:
```sh
mkdir .duckdb
chmod 777 .duckdb
```

In my case the job also created the db files inside as read-only, so I had to `chmod 666 .duckdb/*`.

### 1. Comment out duckdb cleanup in `dags/idh_etl.py`
```python
(
    load_duckdb()
    >> write_table_to_bigquery.expand(table=list(Table))
    # >> clean_up_duckdb_file()
)
```

### 2. Locate your scheduler container and the db you want to connect to (separate one is created for each dag run)
```sh
docker ps

> docker ps
CONTAINER ID   IMAGE                           COMMAND                  CREATED          STATUS          PORTS                      NAMES
08c6805fc1b5   idh-etl_e5e8b9/airflow:latest   "tini -- /entrypoint…"   27 minutes ago   Up 27 minutes   127.0.0.1:8080->8080/tcp   idh-etl_e5e8b9-webserver-1
10dcb1e4b517   idh-etl_e5e8b9/airflow:latest   "tini -- /entrypoint…"   27 minutes ago   Up 27 minutes                              idh-etl_e5e8b9-scheduler-1
4385a666018d   idh-etl_e5e8b9/airflow:latest   "tini -- /entrypoint…"   27 minutes ago   Up 27 minutes                              idh-etl_e5e8b9-triggerer-1
c603457ae43f   postgres:12.6                   "docker-entrypoint.s…"   27 minutes ago   Up 27 minutes   127.0.0.1:5432->5432/tcp   idh-etl_e5e8b9-postgres-1
```

```sh
> docker exec -it idh-etl_e5e8b9-scheduler-1 bash

astro@10dcb1e4b517:/usr/local/airflow$ ls -l duckdb/
total 3581564
-rw-rw-rw- 1 astro astro 152317952 Jan  6 16:26 idh-20241225_000000.duckdb
-rw-rw-rw- 1 astro astro 153890816 Jan  6 16:26 idh-20241225_010000.duckdb
-rw-rw-rw- 1 astro astro 152055808 Jan  6 16:26 idh-20241225_020000.duckdb
-rw-rw-rw- 1 astro astro 152317952 Jan  6 16:26 idh-20241225_030000.duckdb
-rw-rw-rw- 1 astro astro 152842240 Jan  6 16:26 idh-20241225_040000.duckdb
-rw-rw-rw- 1 astro astro 152317952 Jan  6 16:26 idh-20241225_050000.duckdb
-rw-rw-rw- 1 astro astro 152055808 Jan  6 16:26 idh-20241225_060000.duckdb
-rw-rw-rw- 1 astro astro 152842240 Jan  6 16:26 idh-20241225_070000.duckdb
-rw-rw-rw- 1 astro astro 153366528 Jan  6 16:26 idh-20241225_080000.duckdb
-rw-rw-rw- 1 astro astro 153366528 Jan  6 16:26 idh-20241225_090000.duckdb
-rw-rw-rw- 1 astro astro 152580096 Jan  6 16:26 idh-20241225_100000.duckdb
-rw-rw-rw- 1 astro astro 151793664 Jan  6 16:26 idh-20241225_110000.duckdb
-rw-rw-rw- 1 astro astro 153104384 Jan  6 16:26 idh-20241225_120000.duckdb
-rw-rw-rw- 1 astro astro 153890816 Jan  6 16:26 idh-20241225_130000.duckdb
-rw-rw-rw- 1 astro astro 152580096 Jan  6 16:26 idh-20241225_140000.duckdb
-rw-rw-rw- 1 astro astro 152842240 Jan  6 16:26 idh-20241225_150000.duckdb
-rw-r--r-- 1 astro astro 153366528 Jan  6 16:27 idh-20241225_160000.duckdb
-rw-r--r-- 1 astro astro 152842240 Jan  6 16:27 idh-20241225_170000.duckdb
-rw-r--r-- 1 astro astro 152580096 Jan  6 16:27 idh-20241225_180000.duckdb
-rw-r--r-- 1 astro astro 152055808 Jan  6 16:27 idh-20241225_190000.duckdb
-rw-r--r-- 1 astro astro 153104384 Jan  6 16:27 idh-20241225_200000.duckdb
-rw-r--r-- 1 astro astro 152580096 Jan  6 16:27 idh-20241225_210000.duckdb
-rw-r--r-- 1 astro astro 153366528 Jan  6 16:27 idh-20241225_220000.duckdb
-rw-r--r-- 1 astro astro 153366528 Jan  6 16:27 idh-20241225_230000.duckdb
```

```sh
# requires duckdb cli installation on your host
# or nix-shell -p duckdb if you're using the most glorious distro in the world
duckdb .duckdb/idh-20241225_160000.duckdb
```
