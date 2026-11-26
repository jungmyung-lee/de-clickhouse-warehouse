"""Open-Meteo hourly weather -> ClickHouse bronze.weather_hourly_raw.

* First run: backfills every city from WEATHER_BACKFILL_START (default 2026-01-01).
* Later runs: re-fetch the last few days and add anything newer (daily incremental).
* Safe to re-run: the bronze table is a ReplacingMergeTree, so a repeated day replaces itself.

Weather data by Open-Meteo.com (CC BY 4.0), used for non-commercial learning purposes.
"""
from __future__ import annotations

import logging
import os
import time
import uuid
from datetime import date, datetime, timedelta, timezone

import pendulum
import weather_common as wc
from airflow.sdk import dag, task

log = logging.getLogger(__name__)

CITIES_FILE = "/opt/airflow/data/cities.csv"
DEFAULT_BACKFILL_START = "2026-01-01"
# Pause between API calls: far below the free limit (600/min), but polite.
THROTTLE_SECONDS = 0.5


@dag(
    dag_id="weather_bronze_load",
    description="Open-Meteo hourly weather -> ClickHouse bronze (backfill + daily incremental)",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
    },
    tags=["stage3", "weather", "bronze"],
    doc_md=__doc__,
)
def weather_bronze_load():
    @task
    def create_bronze_table() -> None:
        wc.create_bronze_table()

    @task
    def read_cities() -> list[dict]:
        return wc.read_cities(CITIES_FILE)

    @task(max_active_tis_per_dag=2)
    def load_city(city: dict) -> dict:
        backfill_start = date.fromisoformat(os.environ.get("WEATHER_BACKFILL_START", DEFAULT_BACKFILL_START))
        end = wc.archive_end_date(datetime.now(timezone.utc).date())
        window = wc.plan_window(wc.get_last_loaded_date(city["city"]), backfill_start, end)
        if window is None:
            log.info("%s: nothing to load", city["city"])
            return {"city": city["city"], "rows": 0}

        batch_id = uuid.uuid4().hex
        total = 0
        for chunk_start, chunk_end in wc.chunk_dates(*window):
            payload = wc.fetch_archive(city, chunk_start, chunk_end)
            rows = wc.rows_from_response(city, payload, batch_id)
            total += wc.insert_rows(rows)
            log.info("%s: %s..%s -> %d rows", city["city"], chunk_start, chunk_end, len(rows))
            time.sleep(THROTTLE_SECONDS)
        return {"city": city["city"], "from": window[0].isoformat(), "to": window[1].isoformat(), "rows": total}

    @task
    def summarize(results: list[dict]) -> None:
        for result in results:
            log.info("loaded: %s", result)
        log.info("total rows inserted: %d", sum(result["rows"] for result in results))

    table_ready = create_bronze_table()
    cities = read_cities()
    table_ready >> cities
    summarize(load_city.expand(city=cities))


weather_bronze_load()
