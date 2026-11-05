"""Daily Binance 1h klines: API -> validate -> MinIO raw (Parquet) -> Postgres staging.

Each run processes exactly one UTC day, its data interval [start, end).
Re-running a day overwrites the same raw files and upserts the same rows: idempotent.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable

SOURCE_NAME = "binance_klines"
CONFIG_PATH = os.environ.get("PIPELINE_CONFIG", "/opt/pipeline/config/sources.yml")


@dag(
    dag_id="binance_klines",
    # Interval timetable: the run for day D starts after D has ended, with
    # data_interval_start = D 00:00 UTC and data_interval_end = D+1 00:00 UTC.
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
    catchup=False,  # history is loaded explicitly with a backfill (episode 11)
    max_active_runs=4,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "execution_timeout": timedelta(minutes=10),
    },
    tags=["ingestion", "binance"],
)
def binance_klines():
    @task
    def ingest_to_raw(**context) -> list[str]:
        """Fetch one day from Binance, validate it, write Parquet (+ quarantine) to MinIO."""
        # Imports live inside tasks so the dag-processor can parse this file quickly
        from ingestion.settings import MinioSettings
        from ingestion.sources.registry import load_sources
        from ingestion.storage.minio import make_filesystem, write_quarantine, write_raw
        from ingestion.validation import validate_candles

        start, end = context["data_interval_start"], context["data_interval_end"]
        source = load_sources(CONFIG_PATH)[SOURCE_NAME]
        records = source.fetch(start, end)
        result = validate_candles(records, now=datetime.now(timezone.utc))

        settings = MinioSettings.from_env()
        s3 = make_filesystem(settings)
        paths = write_raw(result.valid, SOURCE_NAME, s3, settings.bucket)
        write_quarantine(result.quarantined, SOURCE_NAME, start.date(), s3, settings.bucket)
        return paths  # small list of file paths -> fine to pass via XCom

    @task
    def load_to_staging(paths: list[str]) -> int:
        """Upsert the day's raw files into staging.binance_klines."""
        from ingestion.settings import MinioSettings, WarehouseSettings
        from ingestion.storage.minio import make_filesystem
        from ingestion.storage.postgres import connect, load_files

        s3 = make_filesystem(MinioSettings.from_env())
        conn = connect(WarehouseSettings.from_env())
        try:
            return load_files(conn, s3, paths)
        finally:
            conn.close()

    load_to_staging(ingest_to_raw())


binance_klines()
