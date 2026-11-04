"""Write raw records to MinIO as Parquet, one file per (symbol, date) partition.

Re-running the same day overwrites the same files, so loads are idempotent.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs

from ingestion.settings import MinioSettings
from ingestion.sources.base import Record

log = logging.getLogger(__name__)

PRICE = pa.decimal128(28, 8)  # exact decimals, never float, for money
UTC_MS = pa.timestamp("ms", tz="UTC")

CANDLE_SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("open_time", UTC_MS),
        ("close_time", UTC_MS),
        ("open", PRICE),
        ("high", PRICE),
        ("low", PRICE),
        ("close", PRICE),
        ("volume", PRICE),
        ("quote_volume", PRICE),
        ("trade_count", pa.int64()),
        ("_ingested_at", UTC_MS),  # control column: when this file was written
    ]
)
QUARANTINE_SCHEMA = CANDLE_SCHEMA.append(pa.field("_reject_reason", pa.string()))


def make_filesystem(settings: MinioSettings) -> fs.S3FileSystem:
    return fs.S3FileSystem(
        access_key=settings.access_key,
        secret_key=settings.secret_key,
        endpoint_override=settings.endpoint,
        scheme="http",
        region="us-east-1",  # MinIO ignores it, but the S3 client wants one
    )


def partition_path(bucket: str, dataset: str, symbol: str, day: date) -> str:
    return f"{bucket}/{dataset}/symbol={symbol}/date={day.isoformat()}/data.parquet"


def quarantine_path(bucket: str, dataset: str, day: date) -> str:
    return f"{bucket}/_quarantine/{dataset}/date={day.isoformat()}/rejects.parquet"


def write_raw(
    records: list[Record], dataset: str, filesystem: fs.FileSystem, bucket: str
) -> list[str]:
    """Write valid records, one Parquet file per (symbol, UTC date). Returns written paths."""
    ingested_at = datetime.now(timezone.utc)
    groups: dict[tuple[str, date], list[Record]] = defaultdict(list)
    for record in records:
        groups[(record["symbol"], record["open_time"].date())].append(record)

    paths = []
    for (symbol, day), rows in sorted(groups.items()):
        rows = sorted(rows, key=lambda r: r["open_time"])
        table = pa.Table.from_pylist(
            [{**r, "_ingested_at": ingested_at} for r in rows], schema=CANDLE_SCHEMA
        )
        path = partition_path(bucket, dataset, symbol, day)
        pq.write_table(table, path, filesystem=filesystem)  # overwrites -> idempotent
        log.info("Wrote %d rows to s3://%s", table.num_rows, path)
        paths.append(path)
    return paths


def write_quarantine(
    rejects: list[Record], dataset: str, day: date, filesystem: fs.FileSystem, bucket: str
) -> str | None:
    """Write this day's rejected rows; remove a stale file if the day is now clean."""
    path = quarantine_path(bucket, dataset, day)
    if not rejects:
        if filesystem.get_file_info(path).type == fs.FileType.File:
            filesystem.delete_file(path)
            log.info("Removed stale quarantine file s3://%s", path)
        return None

    ingested_at = datetime.now(timezone.utc)
    table = pa.Table.from_pylist(
        [{**r, "_ingested_at": ingested_at} for r in rejects], schema=QUARANTINE_SCHEMA
    )
    pq.write_table(table, path, filesystem=filesystem)
    log.warning("Quarantined %d rows to s3://%s", table.num_rows, path)
    return path
