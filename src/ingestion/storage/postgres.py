"""Load raw Parquet files from MinIO into Postgres staging with an idempotent upsert.

Generic over datasets: table name, DDL, columns and key come from the DatasetSpec.
"""

from __future__ import annotations

import logging
from datetime import date

import psycopg2
import pyarrow.parquet as pq
from psycopg2.extensions import connection as Connection
from psycopg2.extras import execute_values
from pyarrow import fs

from ingestion.datasets import DatasetSpec
from ingestion.settings import WarehouseSettings

log = logging.getLogger(__name__)


def staging_columns(spec: DatasetSpec) -> list[str]:
    """Dataset columns + control columns, in the order we insert them."""
    return [*spec.arrow_schema.names, "_ingested_at", "_source_file"]


def upsert_sql(spec: DatasetSpec) -> str:
    """Same key again -> update the row instead of inserting a duplicate (like Delta MERGE)."""
    columns = staging_columns(spec)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c not in spec.key_fields)
    return (
        f"INSERT INTO {spec.staging_table} ({', '.join(columns)}) VALUES %s "
        f"ON CONFLICT ({', '.join(spec.key_fields)}) DO UPDATE SET {updates}, _loaded_at = now();"
    )


def connect(settings: WarehouseSettings) -> Connection:
    return psycopg2.connect(
        host=settings.host,
        port=settings.port,
        dbname=settings.dbname,
        user=settings.user,
        password=settings.password,
    )


def list_day_files(filesystem: fs.FileSystem, bucket: str, dataset: str, day: date) -> list[str]:
    """Find every symbol's Parquet file for one day: <bucket>/<dataset>/symbol=*/date=<day>/."""
    symbol_dirs = filesystem.get_file_info(fs.FileSelector(f"{bucket}/{dataset}"))
    candidates = [f"{d.path}/date={day.isoformat()}/data.parquet" for d in symbol_dirs]
    return [p for p in candidates if filesystem.get_file_info(p).type == fs.FileType.File]


def load_files(
    conn: Connection, filesystem: fs.FileSystem, paths: list[str], spec: DatasetSpec
) -> int:
    """Upsert all rows from the given Parquet files in ONE transaction. Returns row count."""
    columns = staging_columns(spec)
    sql = upsert_sql(spec)
    total = 0
    with conn, conn.cursor() as cur:  # `with conn` = commit on success, rollback on error
        cur.execute(spec.staging_ddl)
        for path in paths:
            table = pq.read_table(path, filesystem=filesystem)
            rows = [
                tuple(r[c] for c in columns[:-1]) + (f"s3://{path}",) for r in table.to_pylist()
            ]
            execute_values(cur, sql, rows)
            log.info("Upserted %d rows from s3://%s into %s", len(rows), path, spec.staging_table)
            total += len(rows)
    return total
