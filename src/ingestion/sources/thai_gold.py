"""Thai gold prices (96.5%, THB per baht-weight) announced by the Gold Traders Association.

The association's own site renders prices client-side and offers no public API, so
we read a free third-party JSON mirror of its latest announcement. Limitations:
- latest announcement only: there is NO history, backfilled days stay empty;
- a run captures the day's last announcement (the DAG runs at 07:00 Bangkok, after
  the market has closed), not every intraday update;
- the mirror is a dependency we don't control; a format change fails the task loudly.
Swapping in a better source later = a new Source class with the same DatasetSpec.
"""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pyarrow as pa
import requests

from ingestion.datasets import PRICE, UTC_MS, DatasetSpec
from ingestion.sources.base import Record, Source
from ingestion.validation import check_thai_gold

log = logging.getLogger(__name__)

BANGKOK = ZoneInfo("Asia/Bangkok")
BUDDHIST_ERA_OFFSET = 543
USER_AGENT = "market-data-pipeline/0.1 (educational portfolio project)"
# API product key -> our symbol
PRODUCTS = {"gold_bar": "TH_GOLD_BAR_965", "gold": "TH_GOLD_ORNAMENT_965"}

THAI_GOLD_SPEC = DatasetSpec(
    key_fields=("symbol", "announced_at"),
    partition_field="announced_at",
    arrow_schema=pa.schema(
        [
            ("symbol", pa.string()),
            ("announced_at", UTC_MS),
            ("announcement_no", pa.int64()),  # n-th price update of that day
            ("buy_price", PRICE),  # dealers buy from you (THB per baht-weight)
            ("sell_price", PRICE),  # dealers sell to you
        ]
    ),
    staging_table="staging.thai_gold_prices",
    staging_ddl="""
CREATE TABLE IF NOT EXISTS staging.thai_gold_prices (
    symbol           text           NOT NULL,
    announced_at     timestamptz    NOT NULL,
    announcement_no  bigint         NOT NULL,
    buy_price        numeric(28, 8) NOT NULL,
    sell_price       numeric(28, 8) NOT NULL,
    _ingested_at     timestamptz    NOT NULL,
    _source_file     text           NOT NULL,
    _loaded_at       timestamptz    NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, announced_at)
);
""",
    check=check_thai_gold,
    allow_empty=True,  # no history, and no announcements on some days
)


def parse_announcement(update_date: str, update_time: str) -> tuple[datetime, int]:
    """'05/11/2569', 'เวลา 11:26 น. (ครั้งที่ 11)' -> (2026-11-05 04:26 UTC, 11)."""
    day, month, be_year = (int(x) for x in update_date.split("/"))
    clock = re.search(r"(\d{1,2}):(\d{2})", update_time)
    number = re.search(r"ครั้งที่\s*(\d+)", update_time)
    if clock is None or number is None:
        raise ValueError(f"Unrecognised announcement time format: {update_time!r}")
    local = datetime(
        be_year - BUDDHIST_ERA_OFFSET, month, day, int(clock[1]), int(clock[2]), tzinfo=BANGKOK
    )
    return local.astimezone(UTC), int(number[1])


def parse_baht(text: str) -> Decimal:
    """'65,250.00' -> Decimal('65250.00')."""
    return Decimal(text.replace(",", ""))


class ThaiGoldSource(Source):
    type_name = "thai_gold"
    spec = THAI_GOLD_SPEC

    def fetch(self, start: datetime, end: datetime) -> list[Record]:
        response = requests.get(self.params["url"], headers={"User-Agent": USER_AGENT}, timeout=10)
        response.raise_for_status()
        payload = response.json()
        if payload.get("status") != "success":
            raise ValueError(f"Thai gold API returned status {payload.get('status')!r}")

        data = payload["response"]
        announced_at, number = parse_announcement(data["update_date"], data["update_time"])
        if not start <= announced_at < end:
            log.info(
                "Latest announcement %s is outside %s..%s; this source has no history",
                announced_at,
                start,
                end,
            )
            return []

        records = [
            {
                "symbol": symbol,
                "announced_at": announced_at,
                "announcement_no": number,
                "buy_price": parse_baht(data["price"][product]["buy"]),
                "sell_price": parse_baht(data["price"][product]["sell"]),
            }
            for product, symbol in PRODUCTS.items()
        ]
        log.info("Captured announcement #%d at %s", number, announced_at)
        return records
