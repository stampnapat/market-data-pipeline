"""Ingestion-time data quality checks: split records into valid and quarantined."""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime

from ingestion.sources.base import Record

log = logging.getLogger(__name__)

PRICE_FIELDS = ("open", "high", "low", "close")
REQUIRED_FIELDS = ("symbol", "open_time", "close_time", *PRICE_FIELDS, "volume")
# Reasons that mean "not ready yet", not "broken": excluded from the failure ratio
NOT_A_DEFECT = {"candle_not_closed"}


class DataQualityError(Exception):
    """Raised when a batch is too broken to load at all."""


@dataclass
class ValidationResult:
    valid: list[Record] = field(default_factory=list)
    # Same records plus a "_reject_reason" key explaining why they were rejected
    quarantined: list[Record] = field(default_factory=list)


def check_candle(record: Record, now: datetime) -> str | None:
    """Return why a single candle is invalid, or None if it passes every rule."""
    if any(record.get(f) is None for f in REQUIRED_FIELDS):
        return "missing_value"
    if record["close_time"] >= now:
        return "candle_not_closed"  # still changing; the next run will pick it up
    if any(record[f] <= 0 for f in PRICE_FIELDS):
        return "non_positive_price"
    if not (
        record["low"] <= min(record["open"], record["close"])
        and max(record["open"], record["close"]) <= record["high"]
    ):
        return "inconsistent_ohlc"
    if record["volume"] < 0:
        return "negative_volume"
    return None


def validate_candles(
    records: list[Record], now: datetime, max_reject_ratio: float = 0.05
) -> ValidationResult:
    """Apply row rules, then resolve duplicates on (symbol, open_time).

    - exact duplicate rows   -> keep one copy
    - same key, other values -> quarantine all copies (we can't tell which is right)
    Raises DataQualityError if the batch is empty or too many rows are defective.
    """
    if not records:
        raise DataQualityError("Source returned no records")

    result = ValidationResult()
    rows_by_key: dict[tuple, list[Record]] = defaultdict(list)

    for record in records:
        reason = check_candle(record, now)
        if reason:
            result.quarantined.append({**record, "_reject_reason": reason})
        else:
            rows_by_key[(record["symbol"], record["open_time"])].append(record)

    for rows in rows_by_key.values():
        distinct = list({tuple(sorted(r.items())): r for r in rows}.values())
        if len(distinct) == 1:
            result.valid.append(distinct[0])
        else:
            result.quarantined.extend({**r, "_reject_reason": "conflicting_duplicate"} for r in distinct)

    defects = [r for r in result.quarantined if r["_reject_reason"] not in NOT_A_DEFECT]
    defect_ratio = len(defects) / len(records)
    log.info(
        "Validated %d records: %d valid, %d quarantined (%d defects = %.1f%%)",
        len(records), len(result.valid), len(result.quarantined), len(defects), defect_ratio * 100,
    )
    if defect_ratio > max_reject_ratio:
        raise DataQualityError(
            f"{defect_ratio:.1%} of records are defective (limit {max_reject_ratio:.0%}); "
            "refusing to load this batch"
        )
    return result
