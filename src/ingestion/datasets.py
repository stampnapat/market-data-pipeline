"""What makes one dataset different from another, in one place.

Validation, raw storage and the staging loader are generic: they read a
DatasetSpec instead of hardcoding columns for Binance, FX, gold, ...
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pyarrow as pa

Record = dict[str, Any]
RowCheck = Callable[[Record, datetime], "str | None"]

# Shared column types
PRICE = pa.decimal128(28, 8)  # exact decimals, never float, for money
UTC_MS = pa.timestamp("ms", tz="UTC")


@dataclass(frozen=True)
class DatasetSpec:
    key_fields: tuple[str, ...]  # natural key: one row per key (dedup + upsert target)
    partition_field: str  # date/datetime field that decides the date= partition
    arrow_schema: pa.Schema  # columns and types of the raw Parquet files
    staging_table: str  # e.g. "staging.binance_klines"
    staging_ddl: str  # CREATE TABLE IF NOT EXISTS ... for that table
    check: RowCheck  # returns a reject reason, or None if the row is valid
    allow_empty: bool = False  # True when "no rows" is normal (e.g. FX on weekends)
