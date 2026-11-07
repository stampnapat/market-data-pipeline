"""Daily spot gold (XAU/USD) from the Twelve Data REST API (free plan, API key required).

The key is read from the TWELVEDATA_API_KEY environment variable, never from code.
Observed on the live API (the docs are silent on these):
- end_date is exclusive; we still keep only bars inside our half-open window;
- the free plan allows 8 requests per minute; a rate-limit answer is retried after
  the minute resets, so a backfill can't fail just by being fast;
- weekend bars exist although the market is closed (flat placeholders). Raw keeps
  them as delivered; the dbt staging model drops them.
Prices arrive as strings and are parsed to Decimal.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime, timedelta
from decimal import Decimal

import pyarrow as pa
import requests

from ingestion.datasets import PRICE, DatasetSpec
from ingestion.sources.base import Record, Source
from ingestion.validation import check_daily_ohlc

log = logging.getLogger(__name__)

BASE_URL = "https://api.twelvedata.com/time_series"
RATE_LIMIT_CODE = 429
RATE_LIMIT_WAIT_SECONDS = 61  # free plan: 8 requests per rolling minute
RATE_LIMIT_RETRIES = 3
_sleep = time.sleep  # replaced in tests

GOLD_SPOT_SPEC = DatasetSpec(
    key_fields=("symbol", "price_date"),
    partition_field="price_date",
    arrow_schema=pa.schema(
        [
            ("symbol", pa.string()),  # e.g. XAUUSD (no slash: it becomes a path segment)
            ("price_date", pa.date32()),
            ("open", PRICE),
            ("high", PRICE),
            ("low", PRICE),
            ("close", PRICE),  # USD per troy ounce
        ]
    ),
    staging_table="staging.gold_spot_prices",
    staging_ddl="""
CREATE TABLE IF NOT EXISTS staging.gold_spot_prices (
    symbol        text           NOT NULL,
    price_date    date           NOT NULL,
    open          numeric(28, 8) NOT NULL,
    high          numeric(28, 8) NOT NULL,
    low           numeric(28, 8) NOT NULL,
    close         numeric(28, 8) NOT NULL,
    _ingested_at  timestamptz    NOT NULL,
    _source_file  text           NOT NULL,
    _loaded_at    timestamptz    NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, price_date)
);
""",
    check=check_daily_ohlc,
    allow_empty=True,  # no daily bars on weekends
)


class TwelveDataSource(Source):
    type_name = "twelvedata"
    spec = GOLD_SPOT_SPEC

    def fetch(self, start: datetime, end: datetime) -> list[Record]:
        api_key = os.environ.get("TWELVEDATA_API_KEY")
        if not api_key:
            raise RuntimeError("TWELVEDATA_API_KEY is not set (see .env.example)")

        first_day = start.date()
        last_day = (end - timedelta(days=1)).date()  # window is [start, end)
        records: list[Record] = []
        for symbol in self.params["symbols"]:  # e.g. "XAU/USD"
            payload = self._request(symbol, first_day, end.date(), api_key)
            for bar in payload.get("values", []):
                day = date.fromisoformat(bar["datetime"][:10])
                if not first_day <= day <= last_day:
                    continue
                records.append(
                    {
                        "symbol": symbol.replace("/", ""),
                        "price_date": day,
                        "open": Decimal(bar["open"]),
                        "high": Decimal(bar["high"]),
                        "low": Decimal(bar["low"]),
                        "close": Decimal(bar["close"]),
                    }
                )
            log.info("Fetched %d daily bars for %s", len(records), symbol)
        return records

    def _request(self, symbol: str, first_day: date, end_day: date, api_key: str) -> dict:
        """GET one symbol's bars; wait out the per-minute rate limit instead of failing."""
        for attempt in range(RATE_LIMIT_RETRIES + 1):
            response = requests.get(
                BASE_URL,
                params={
                    "symbol": symbol,
                    "interval": self.params.get("interval", "1day"),
                    "start_date": first_day.isoformat(),
                    "end_date": end_day.isoformat(),  # exclusive
                    "order": "asc",
                    "apikey": api_key,
                },
                timeout=15,
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("status") == "ok":
                return payload
            code, message = payload.get("code"), payload.get("message")
            if code == RATE_LIMIT_CODE and attempt < RATE_LIMIT_RETRIES:
                log.warning("Twelve Data rate limit hit; waiting %ss", RATE_LIMIT_WAIT_SECONDS)
                _sleep(RATE_LIMIT_WAIT_SECONDS)
                continue
            # Errors come back as HTTP 200 with {"status": "error", "code", "message"}
            raise ValueError(f"Twelve Data error for {symbol}: {code} {message}")
        raise AssertionError("unreachable")
