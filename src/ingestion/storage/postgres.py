"""Load raw Parquet files from MinIO into Postgres staging with an idempotent upsert."""

from __future__ import annotations

import logging
from datetime import date

import psycopg2
import pyarrow.parquet as pq
from psycopg2.extensions import connection as Connection
from psycopg2.extras import execute_values
from pyarrow import fs

from ingestion.settings import WarehouseSettings

log = logging.getLogger(__name__)

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS staging.binance_klines (
    symbol        text           NOT NULL,
    open_time     timestamptz    NOT NULL,
    close_time    timestamptz    NOT NULL,
    open          numeric(28, 8) NOT NULL,
    high          numeric(28, 8) NOT NULL,
    low           numeric(28, 8) NOT NULL,
    close         numeric(28, 8) NOT NULL,
    volume        numeric(28, 8) NOT NULL,
    quote_volume  numeric(28, 8) NOT NULL,
    trade_count   bigint         NOT NULL,
    _ingested_at  timestamptz    NOT NULL,  -- when the raw file was written
    _source_file  text           NOT NULL,  -- lineage: which raw file this row came from
    _loaded_at    timestamptz    NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, open_time)
);
"""

COLUMNS = [
    "symbol", "open_time", "close_time", "open", "high", "low", "close",
    "volume", "quote_volume", "trade_count", "_ingested_at", "_source_file",
]

# Same key again -> update the row instead of inserting a duplicate (Delta MERGE equivalent)
UPSERT = f"""
INSERT INTO staging.binance_klines ({", ".join(COLUMNS)})
VALUES %s
ON CONFLICT (symbol, open_time) DO UPDATE SET
    {", ".join(f"{c} = EXCLUDED.{c}" for c in COLUMNS if c not in ("symbol", "open_time"))},
    _loaded_at = now();
"""


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


def load_files(conn: Connection, filesystem: fs.FileSystem, paths: list[str]) -> int:
    """Upsert all rows from the given Parquet files in ONE transaction. Returns row count."""
    total = 0
    with conn, conn.cursor() as cur:  # `with conn` = commit on success, rollback on error
        cur.execute(CREATE_TABLE)
        for path in paths:
            table = pq.read_table(path, filesystem=filesystem)
            rows = [
                tuple(r[c] for c in COLUMNS[:-1]) + (f"s3://{path}",)
                for r in table.to_pylist()
            ]
            execute_values(cur, UPSERT, rows)
            log.info("Upserted %d rows from s3://%s", len(rows), path)
            total += len(rows)
    return total
