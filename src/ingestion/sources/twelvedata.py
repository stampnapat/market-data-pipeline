"""Daily spot gold (XAU/USD) from the Twelve Data REST API (free plan, API key required).

The key is read from the TWELVEDATA_API_KEY environment variable, never from code.
Prices arrive as strings and are parsed to Decimal. Twelve Data's docs don't say
whether end_date is inclusive, so we ask for a range that covers both readings and
keep only bars inside our half-open window.
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from decimal import Decimal

import pyarrow as pa
import requests

from ingestion.datasets import PRICE, DatasetSpec
from ingestion.sources.base import Record, Source
from ingestion.validation import check_daily_ohlc

log = logging.getLogger(__name__)

BASE_URL = "https://api.twelvedata.com/time_series"

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
            response = requests.get(
                BASE_URL,
                params={
                    "symbol": symbol,
                    "interval": self.params.get("interval", "1day"),
                    "start_date": first_day.isoformat(),
                    "end_date": end.date().isoformat(),  # inclusive or not, we filter below
                    "order": "asc",
                    "apikey": api_key,
                },
                timeout=15,
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("status") != "ok":
                # Errors come back as HTTP 200 with {"status": "error", "code", "message"}
                code, message = payload.get("code"), payload.get("message")
                raise ValueError(f"Twelve Data error for {symbol}: {code} {message}")

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
