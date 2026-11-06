"""Write raw records to MinIO as Parquet, one file per (symbol, date) partition.

Generic over datasets: the DatasetSpec says which columns, types and date field to use.
Re-running the same day overwrites the same files, so loads are idempotent.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import UTC, date, datetime

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs

from ingestion.datasets import UTC_MS, DatasetSpec, Record
from ingestion.settings import MinioSettings

log = logging.getLogger(__name__)


def raw_schema(spec: DatasetSpec) -> pa.Schema:
    """Dataset columns + control column recording when the file was written."""
    return spec.arrow_schema.append(pa.field("_ingested_at", UTC_MS))


def quarantine_schema(spec: DatasetSpec) -> pa.Schema:
    return raw_schema(spec).append(pa.field("_reject_reason", pa.string()))


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


def _as_date(value: date | datetime) -> date:
    return value.date() if isinstance(value, datetime) else value


def _write_parquet(table: pa.Table, path: str, filesystem: fs.FileSystem) -> None:
    """Write (overwrite) one Parquet file, creating parent dirs on filesystems that need them."""
    filesystem.create_dir(path.rsplit("/", 1)[0], recursive=True)
    pq.write_table(table, path, filesystem=filesystem)


def write_raw(
    records: list[Record], spec: DatasetSpec, dataset: str, filesystem: fs.FileSystem, bucket: str
) -> list[str]:
    """Write valid records, one Parquet file per (symbol, date). Returns written paths."""
    ingested_at = datetime.now(UTC)
    groups: dict[tuple[str, date], list[Record]] = defaultdict(list)
    for record in records:
        groups[(record["symbol"], _as_date(record[spec.partition_field]))].append(record)

    paths = []
    for (symbol, day), rows in sorted(groups.items()):
        rows = sorted(rows, key=lambda r: tuple(r[f] for f in spec.key_fields))
        table = pa.Table.from_pylist(
            [{**r, "_ingested_at": ingested_at} for r in rows], schema=raw_schema(spec)
        )
        path = partition_path(bucket, dataset, symbol, day)
        _write_parquet(table, path, filesystem)  # overwrites -> idempotent
        log.info("Wrote %d rows to s3://%s", table.num_rows, path)
        paths.append(path)
    return paths


def write_quarantine(
    rejects: list[Record],
    spec: DatasetSpec,
    dataset: str,
    day: date,
    filesystem: fs.FileSystem,
    bucket: str,
) -> str | None:
    """Write this day's rejected rows; remove a stale file if the day is now clean."""
    path = quarantine_path(bucket, dataset, day)
    if not rejects:
        if filesystem.get_file_info(path).type == fs.FileType.File:
            filesystem.delete_file(path)
            log.info("Removed stale quarantine file s3://%s", path)
        return None

    ingested_at = datetime.now(UTC)
    table = pa.Table.from_pylist(
        [{**r, "_ingested_at": ingested_at} for r in rejects], schema=quarantine_schema(spec)
    )
    _write_parquet(table, path, filesystem)
    log.warning("Quarantined %d rows to s3://%s", table.num_rows, path)
    return path
