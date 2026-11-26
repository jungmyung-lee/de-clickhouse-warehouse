"""Helpers for the Open-Meteo -> ClickHouse bronze load.

Nothing in this file imports Airflow, so the logic can be unit tested on its own.
The DAG (weather_bronze_dag.py) only wires these functions together.
"""
from __future__ import annotations

import csv
import json
import os
from datetime import date, datetime, timedelta

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
BRONZE_TABLE = "bronze.weather_hourly_raw"

# Up to 10 variables count as one call per 2 weeks of data (Open-Meteo call weighting).
HOURLY_VARIABLES = (
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "precipitation",
    "pressure_msl",
    "cloud_cover",
    "wind_speed_10m",
    "weather_code",
)

INSERT_COLUMNS = (
    "city",
    "country",
    "latitude",
    "longitude",
    "observed_at",
    *HOURLY_VARIABLES,
    "_batch_id",
)

# The ERA5 archive is about 5 days behind real time.
ARCHIVE_LAG_DAYS = 5
# Every run re-fetches the last few days, because the newest ERA5 values can still be revised.
OVERLAP_DAYS = 3
# One request covers at most this many days (keeps responses small and retries cheap).
CHUNK_DAYS = 31

# Bronze = the data as it arrived, plus ingestion metadata (_source, _batch_id, _ingested_at).
# ReplacingMergeTree keeps one row per (city, observed_at); the newest _ingested_at wins.
# That is what makes a re-load of the same days safe (idempotent).
CREATE_BRONZE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS bronze.weather_hourly_raw
(
    city                  String,
    country               String,
    latitude              Float64,
    longitude             Float64,
    observed_at           DateTime('UTC'),
    temperature_2m        Nullable(Float32),
    relative_humidity_2m  Nullable(Float32),
    apparent_temperature  Nullable(Float32),
    precipitation         Nullable(Float32),
    pressure_msl          Nullable(Float32),
    cloud_cover           Nullable(Float32),
    wind_speed_10m        Nullable(Float32),
    weather_code          Nullable(Int16),
    _source               LowCardinality(String) DEFAULT 'open-meteo-archive',
    _batch_id             String,
    _ingested_at          DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(_ingested_at)
PARTITION BY toYYYYMM(observed_at)
ORDER BY (city, observed_at)
"""


# --------------------------------------------------------------------------- pure logic


def read_cities(path: str) -> list[dict]:
    """Read cities.csv (city,country,latitude,longitude)."""
    with open(path, newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    cities = [
        {
            "city": row["city"].strip(),
            "country": row["country"].strip(),
            "latitude": float(row["latitude"]),
            "longitude": float(row["longitude"]),
        }
        for row in rows
    ]
    if not cities:
        raise ValueError(f"No cities found in {path}")
    return cities


def archive_end_date(today: date, lag_days: int = ARCHIVE_LAG_DAYS) -> date:
    """Newest day the archive can be asked for."""
    return today - timedelta(days=lag_days)


def plan_window(
    last_loaded: date | None,
    backfill_start: date,
    end: date,
    overlap_days: int = OVERLAP_DAYS,
) -> tuple[date, date] | None:
    """Which days to fetch for one city, or None when there is nothing to do.

    First run (nothing in bronze): backfill from `backfill_start`.
    Later runs: start `overlap_days` days back from the last loaded day.
    """
    if last_loaded is None:
        start = backfill_start
    else:
        start = max(backfill_start, last_loaded - timedelta(days=overlap_days - 1))
    if start > end:
        return None
    return start, end


def chunk_dates(start: date, end: date, chunk_days: int = CHUNK_DAYS) -> list[tuple[date, date]]:
    """Split [start, end] into consecutive chunks of at most `chunk_days` days."""
    chunks = []
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return chunks


def build_params(city: dict, start: date, end: date) -> dict:
    return {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": "UTC",
    }


def to_clickhouse_datetime(value: str) -> str:
    """'2026-01-01T00:00' -> '2026-01-01 00:00:00'."""
    return datetime.strptime(value, "%Y-%m-%dT%H:%M").strftime("%Y-%m-%d %H:%M:%S")


def rows_from_response(city: dict, payload: dict, batch_id: str) -> list[dict]:
    """Turn one Open-Meteo response into bronze rows. Raises ValueError on a malformed response."""
    hourly = payload.get("hourly") if isinstance(payload, dict) else None
    if not hourly or "time" not in hourly:
        raise ValueError("Open-Meteo response has no hourly.time")
    times = hourly["time"]
    for variable in HOURLY_VARIABLES:
        if variable not in hourly:
            raise ValueError(f"Open-Meteo response is missing hourly.{variable}")
        if len(hourly[variable]) != len(times):
            raise ValueError(f"hourly.{variable} has {len(hourly[variable])} values for {len(times)} timestamps")

    rows = []
    for index, stamp in enumerate(times):
        values = {variable: hourly[variable][index] for variable in HOURLY_VARIABLES}
        # Hours the archive does not have yet come back as all-null: skip them.
        if all(value is None for value in values.values()):
            continue
        if values["weather_code"] is not None:
            values["weather_code"] = int(round(values["weather_code"]))
        rows.append(
            {
                "city": city["city"],
                "country": city["country"],
                "latitude": city["latitude"],
                "longitude": city["longitude"],
                "observed_at": to_clickhouse_datetime(stamp),
                **values,
                "_batch_id": batch_id,
            }
        )
    return rows


def to_json_each_row(rows: list[dict]) -> str:
    return "\n".join(json.dumps(row, separators=(",", ":")) for row in rows)


# --------------------------------------------------------------------------- I/O


def fetch_archive(city: dict, start: date, end: date, timeout: int = 60) -> dict:
    """Call the Open-Meteo archive API for one city and one date range."""
    import requests  # imported here so the pure functions above need no extra package

    response = requests.get(ARCHIVE_URL, params=build_params(city, start, end), timeout=timeout)
    if response.status_code != 200:
        raise RuntimeError(f"Open-Meteo HTTP {response.status_code}: {response.text[:300]}")
    return response.json()


def clickhouse_request(sql: str, *, body: str | None = None, params: dict | None = None, timeout: int = 120) -> str:
    """Run one statement through the ClickHouse HTTP interface.

    Connection settings come from the environment (set in docker-compose.yaml / .env).
    `params` fill {name:Type} placeholders in the SQL, so values never get pasted into the query text.
    """
    import requests

    url = f"http://{os.environ['CLICKHOUSE_HOST']}:{os.environ.get('CLICKHOUSE_HTTP_PORT', '8123')}/"
    headers = {
        "X-ClickHouse-User": os.environ["CLICKHOUSE_USER"],
        "X-ClickHouse-Key": os.environ["CLICKHOUSE_PASSWORD"],
    }
    query_params = {f"param_{name}": value for name, value in (params or {}).items()}
    if body is None:
        response = requests.post(url, params=query_params, data=sql.encode("utf-8"), headers=headers, timeout=timeout)
    else:
        # INSERT: the statement goes in the URL, the data in the body.
        response = requests.post(
            url,
            params={**query_params, "query": sql},
            data=body.encode("utf-8"),
            headers=headers,
            timeout=timeout,
        )
    if response.status_code != 200:
        raise RuntimeError(f"ClickHouse HTTP {response.status_code}: {response.text[:500]}")
    return response.text


def create_bronze_table() -> None:
    clickhouse_request(CREATE_BRONZE_TABLE_SQL)


def get_last_loaded_date(city_name: str) -> date | None:
    """Day of the newest row already in bronze for this city, or None when there is none."""
    output = clickhouse_request(
        f"SELECT count(), toString(toDate(max(observed_at))) FROM {BRONZE_TABLE} "
        "WHERE city = {city:String} FORMAT TSV",
        params={"city": city_name},
    )
    count, day = output.strip().split("\t")
    return None if int(count) == 0 else date.fromisoformat(day)


def insert_rows(rows: list[dict]) -> int:
    if not rows:
        return 0
    columns = ", ".join(INSERT_COLUMNS)
    clickhouse_request(
        f"INSERT INTO {BRONZE_TABLE} ({columns}) FORMAT JSONEachRow",
        body=to_json_each_row(rows),
    )
    return len(rows)
