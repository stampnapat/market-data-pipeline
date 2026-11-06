"""Binance spot klines (OHLCV candles) from the public REST API."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pyarrow as pa
import requests

from ingestion.datasets import PRICE, UTC_MS, DatasetSpec
from ingestion.sources.base import Record, Source
from ingestion.validation import check_candle

log = logging.getLogger(__name__)

BASE_URL = "https://api.binance.com/api/v3/klines"
MAX_LIMIT = 1000  # Binance returns at most 1000 candles per request
INTERVALS = {"1h": timedelta(hours=1), "1d": timedelta(days=1)}


def to_ms(dt: datetime) -> int:
    """Binance timestamps are Unix time in milliseconds."""
    return int(dt.timestamp() * 1000)


def from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


CANDLE_SPEC = DatasetSpec(
    key_fields=("symbol", "open_time"),
    partition_field="open_time",
    arrow_schema=pa.schema(
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
        ]
    ),
    staging_table="staging.binance_klines",
    staging_ddl="""
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
""",
    check=check_candle,
)


class BinanceSource(Source):
    type_name = "binance"
    spec = CANDLE_SPEC

    def fetch(self, start: datetime, end: datetime) -> list[Record]:
        interval = self.params["interval"]
        step = INTERVALS[interval]
        # Compare in seconds: Airflow passes pendulum datetimes, whose durations
        # can't be divided by a stdlib timedelta
        if (end - start).total_seconds() / step.total_seconds() > MAX_LIMIT:
            raise ValueError(f"Window {start}..{end} exceeds {MAX_LIMIT} {interval} candles")

        records: list[Record] = []
        for symbol in self.params["symbols"]:
            raw = self._request(symbol, interval, start, end)
            records.extend(self._normalize(symbol, row) for row in raw)
            log.info("Fetched %d %s candles for %s", len(raw), interval, symbol)
        return records

    def _request(self, symbol: str, interval: str, start: datetime, end: datetime) -> list[list]:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": to_ms(start),
            "endTime": to_ms(end) - 1,  # endTime is inclusive -> make the window half-open
            "limit": MAX_LIMIT,
        }
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _normalize(symbol: str, row: list) -> Record:
        """Turn Binance's positional array into a named record with proper types."""
        return {
            "symbol": symbol,
            "open_time": from_ms(row[0]),
            "close_time": from_ms(row[6]),
            "open": Decimal(row[1]),
            "high": Decimal(row[2]),
            "low": Decimal(row[3]),
            "close": Decimal(row[4]),
            "volume": Decimal(row[5]),
            "quote_volume": Decimal(row[7]),
            "trade_count": int(row[8]),
        }
