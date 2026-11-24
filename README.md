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
| Batch | Open-Meteo weather (historical + daily) | Airflow + dbt | in progress |
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
    ├── dags/                  # Airflow DAGs  (coming)
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

## Troubleshooting

- **`silver` or `gold` is missing:** the init SQL only runs when ClickHouse starts with an empty data volume.
  If the volume already existed, create the databases by hand:

  ```bash
  docker compose exec -T clickhouse sh -c 'clickhouse-client -u "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' < clickhouse/init/01_create_medallion_databases.sql
  ```

- **Port already in use:** another stack (for example the Stage 1/2 Airflow on 8080 or MinIO on 9000) is running.
  Stop it first with `docker compose down` in that project's folder.

## Next steps

- Batch DAG that loads Open-Meteo weather into `bronze` *(coming)*
- dbt models for `silver` and `gold` *(coming)*
- NiFi realtime pipeline *(coming)*
