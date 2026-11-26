# de-clickhouse-warehouse

Stage 3-6: realtime (NiFi) and batch (Airflow + dbt) pipelines into a ClickHouse medallion warehouse.

> Work in progress. This README grows with every step. Sections marked *(coming)* are not built yet.

## Overview

Two public data sources are loaded into one ClickHouse warehouse with **ELT**: the raw data is loaded first,
then transformed inside ClickHouse into a **Medallion Architecture**.

| Layer | ClickHouse database | Content |
|---|---|---|
| Bronze | `bronze` | Raw data exactly as ingested |
| Silver | `silver` | Cleaned and conformed data |
| Gold | `gold` | Business-ready aggregates |

| Pipeline | Source | Tooling | Status |
|---|---|---|---|
| Batch | Open-Meteo weather (historical + daily) | Airflow + dbt | bronze load done, dbt models *(coming)* |
| Realtime | Public websocket feed | Apache NiFi | *(coming)* |

Weather data by [Open-Meteo.com](https://open-meteo.com/) (licence CC BY 4.0), used for non-commercial learning purposes.

## Repository layout

```text
.
├── docker-compose.yaml        # ClickHouse + Airflow (+ NiFi later)
├── Dockerfile                 # Airflow image with dbt in its own virtualenv
├── .env.example               # placeholders only, copy to .env
├── clickhouse/init/           # SQL that creates the bronze / silver / gold databases
└── batch/weather/
    ├── cities.csv             # the 10 cities to fetch
    ├── dags/                  # Airflow DAGs: weather_bronze_dag.py, weather_common.py (helpers)
    └── dbt/                   # dbt project: silver and gold models  (coming)
```

## Prerequisites

- Docker and Docker Compose
- About 4 GB of memory available to Docker

## Quick start

1. Copy the settings file and fill in the values:

   ```bash
   cp .env.example .env
   ```

   - `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD`: choose any values.
   - `FERNET_KEY`: generate one with
     `python3 -c "import base64,os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"`
   - `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD`: the Airflow web login.

2. Start the stack (the first build takes a few minutes):

   ```bash
   docker compose up -d --build
   docker compose ps
   ```

   Every service should become `healthy` (the one-shot `airflow-init` exits after it finishes).

3. Check the warehouse. All three medallion databases must exist:

   ```bash
   docker compose exec clickhouse sh -c 'clickhouse-client -u "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" -q "SHOW DATABASES"'
   ```

   The output must contain `bronze`, `silver` and `gold`.

4. Open the Airflow UI at <http://localhost:8081> and sign in with the credentials from `.env`.

5. Check that dbt is available inside the Airflow image:

   ```bash
   docker compose exec airflow-scheduler /opt/dbt_venv/bin/dbt --version
   ```

Stop the stack with `docker compose down`. Do not add `-v` unless you want to delete the stored data.

## Batch pipeline: bronze load

The DAG `weather_bronze_load` loads hourly weather for the 10 cities in `batch/weather/cities.csv`
from the [Open-Meteo historical API](https://open-meteo.com/en/docs/historical-weather-api)
into `bronze.weather_hourly_raw`.

- **Backfill:** the first run loads every city from 2026-01-01 (change it with `WEATHER_BACKFILL_START`).
- **Daily incremental:** later runs (02:00 UTC) load everything newer and re-fetch the last 3 days,
  because the newest ERA5 values can still be revised. The archive is about 5 days behind real time.
- **Idempotent:** the table is a `ReplacingMergeTree` ordered by `(city, observed_at)`. Loading the same
  hour twice keeps the newest copy. Until ClickHouse merges the parts, read with `FINAL`.
- **Metadata:** every row carries `_source`, `_batch_id` and `_ingested_at`.
- **Friendly to the free API:** one request covers up to 31 days of one city, with a short pause between
  requests (about 250 weighted calls for the full backfill, the free limit is 10,000 per day).

Run it:

```bash
docker compose exec airflow-scheduler airflow dags unpause weather_bronze_load
docker compose exec airflow-scheduler airflow dags trigger weather_bronze_load
```

Watch the run in the Airflow UI (<http://localhost:8081>), then check the data:

```bash
docker compose exec clickhouse sh -c 'clickhouse-client -u "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" -q "SELECT city, count() AS hours, min(observed_at), max(observed_at) FROM bronze.weather_hourly_raw FINAL GROUP BY city ORDER BY city"'
```

Run the DAG again and the counts stay the same.

## Troubleshooting

- **`silver` or `gold` is missing:** the init SQL only runs when ClickHouse starts with an empty data volume.
  If the volume already existed, create the databases by hand:

  ```bash
  docker compose exec -T clickhouse sh -c 'clickhouse-client -u "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' < clickhouse/init/01_create_medallion_databases.sql
  ```

- **A new or changed DAG file is not visible in Airflow** (`ls /opt/airflow/dags` is empty inside the container, or `DagNotFound`): the folder was replaced on the host (for example by `git switch` or `git pull`) and the containers still point at the old one. Recreate them; the data is kept in the named volumes:

  ```bash
  docker compose up -d --force-recreate
  ```

- **Port already in use:** another stack (for example the Stage 1/2 Airflow on 8080 or MinIO on 9000) is running.
  Stop it first with `docker compose down` in that project's folder.

## Next steps

- dbt models for `silver` and `gold` *(coming)*
- NiFi realtime pipeline *(coming)*
