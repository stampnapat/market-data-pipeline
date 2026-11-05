"""One daily ingestion DAG per entry in config/sources.yml (DAG factory).

Each DAG: API -> validate -> MinIO raw (Parquet) -> Postgres staging, for exactly one
UTC day (its data interval). Re-running a day overwrites the same raw files and
upserts the same rows, so it is idempotent. Adding a source to the YAML adds a DAG.
A successful staging load emits the asset `ingested.<source>`, which schedules dbt.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
from pipeline_assets import CONFIG_PATH, ingested_asset, source_entries


def build_ingestion_dag(source_name: str, source_type: str) -> None:
    @dag(
        dag_id=source_name,
        # Interval timetable: the run for day D starts after D has ended, with
        # data_interval_start = D 00:00 UTC and data_interval_end = D+1 00:00 UTC.
        schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
        start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
        catchup=False,  # history is loaded explicitly with `airflow backfill create`
        max_active_runs=4,
        default_args={
            "retries": 3,
            "retry_delay": timedelta(minutes=1),
            "retry_exponential_backoff": True,
            "execution_timeout": timedelta(minutes=10),
        },
        tags=["ingestion", source_type],
    )
    def ingestion():
        @task
        def ingest_to_raw(**context) -> list[str]:
            """Fetch one day, validate it, write Parquet (+ quarantine) to MinIO."""
            # Imports live inside tasks so the dag-processor can parse this file quickly
            from ingestion.settings import MinioSettings
            from ingestion.sources.registry import load_sources
            from ingestion.storage.minio import make_filesystem, write_quarantine, write_raw
            from ingestion.validation import validate_records

            start, end = context["data_interval_start"], context["data_interval_end"]
            source = load_sources(CONFIG_PATH)[source_name]
            records = source.fetch(start, end)
            result = validate_records(records, source.spec, now=datetime.now(timezone.utc))

            settings = MinioSettings.from_env()
            s3 = make_filesystem(settings)
            paths = write_raw(result.valid, source.spec, source_name, s3, settings.bucket)
            write_quarantine(
                result.quarantined, source.spec, source_name, start.date(), s3, settings.bucket
            )
            return paths  # small list of file paths -> fine to pass via XCom

        # On success, mark "ingested.<source>" as updated -> triggers the dbt DAG
        @task(outlets=[ingested_asset(source_name)])
        def load_to_staging(paths: list[str]) -> int:
            """Upsert the day's raw files into the source's staging table."""
            from ingestion.settings import MinioSettings, WarehouseSettings
            from ingestion.sources.registry import load_sources
            from ingestion.storage.minio import make_filesystem
            from ingestion.storage.postgres import connect, load_files

            spec = load_sources(CONFIG_PATH)[source_name].spec
            s3 = make_filesystem(MinioSettings.from_env())
            conn = connect(WarehouseSettings.from_env())
            try:
                return load_files(conn, s3, paths, spec)
            finally:
                conn.close()

        load_to_staging(ingest_to_raw())

    ingestion()


for entry in source_entries():
    build_ingestion_dag(entry["name"], entry["type"])
