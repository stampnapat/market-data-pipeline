"""Daily FX reference rates (European Central Bank) from the free Frankfurter API.

ECB publishes one rate per business day (~16:00 CET); there are no weekend or
TARGET-holiday rates. Frankfurter silently snaps a request for a non-business
day to the previous business day, so we keep only dates inside the asked window.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from decimal import Decimal

import pyarrow as pa
import requests

from ingestion.datasets import PRICE, DatasetSpec
from ingestion.sources.base import Record, Source
from ingestion.validation import check_fx_rate

log = logging.getLogger(__name__)

BASE_URL = "https://api.frankfurter.dev/v1"

FX_SPEC = DatasetSpec(
    key_fields=("symbol", "rate_date"),
    partition_field="rate_date",
    arrow_schema=pa.schema(
        [
            ("symbol", pa.string()),  # e.g. USDTHB
            ("base_currency", pa.string()),
            ("quote_currency", pa.string()),
            ("rate_date", pa.date32()),
            ("rate", PRICE),  # 1 base = rate quote
        ]
    ),
    staging_table="staging.fx_rates",
    staging_ddl="""
CREATE TABLE IF NOT EXISTS staging.fx_rates (
    symbol          text           NOT NULL,
    base_currency   text           NOT NULL,
    quote_currency  text           NOT NULL,
    rate_date       date           NOT NULL,
    rate            numeric(28, 8) NOT NULL,
    _ingested_at    timestamptz    NOT NULL,
    _source_file    text           NOT NULL,
    _loaded_at      timestamptz    NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, rate_date)
);
""",
    check=check_fx_rate,
    allow_empty=True,  # weekends and ECB holidays have no rates
)


class FrankfurterSource(Source):
    type_name = "frankfurter"
    spec = FX_SPEC

    def fetch(self, start: datetime, end: datetime) -> list[Record]:
        base = self.params["base"]
        quotes = self.params["symbols"]
        first_day = start.date()
        last_day = (end - timedelta(days=1)).date()  # window is [start, end)

        response = requests.get(
            f"{BASE_URL}/{first_day.isoformat()}..{last_day.isoformat()}",
            params={"base": base, "symbols": ",".join(quotes)},
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json(parse_float=Decimal)  # parse JSON numbers as exact Decimals

        records: list[Record] = []
        for day_str, rates in payload.get("rates", {}).items():
            day = date.fromisoformat(day_str)
            if not first_day <= day <= last_day:
                continue  # snapped to an earlier business day: not data for this window
            for quote, rate in rates.items():
                records.append(
                    {
                        "symbol": f"{base}{quote}",
                        "base_currency": base,
                        "quote_currency": quote,
                        "rate_date": day,
                        "rate": rate,
                    }
                )
        log.info("Fetched %d FX rates for %s..%s", len(records), first_day, last_day)
        return records
