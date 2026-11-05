from __future__ import annotations

from collections import Counter
from datetime import timedelta
from decimal import Decimal

import pytest

from ingestion.validation import DataQualityError, check_candle, validate_candles


def test_clean_batch_passes_untouched(candles, now):
    result = validate_candles(candles, now)
    assert len(result.valid) == len(candles)
    assert result.quarantined == []


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"close": None}, "missing_value"),
        ({"open": Decimal("0")}, "non_positive_price"),
        ({"low": Decimal("-1")}, "non_positive_price"),
        ({"high": Decimal("1")}, "inconsistent_ohlc"),  # high below low
        ({"low": Decimal("99999999")}, "inconsistent_ohlc"),  # low above open/close
        ({"volume": Decimal("-0.1")}, "negative_volume"),
    ],
)
def test_each_rule_reports_its_reason(candles, now, change, reason):
    assert check_candle({**candles[0], **change}, now) == reason


def test_open_candle_is_quarantined_but_not_a_defect(candles):
    # "now" is in the middle of the last candle -> it has not closed yet
    now = candles[-1]["open_time"] + timedelta(minutes=30)
    result = validate_candles(candles, now, max_reject_ratio=0.0)  # would raise if counted
    assert [r["_reject_reason"] for r in result.quarantined] == ["candle_not_closed"]
    assert len(result.valid) == len(candles) - 1


def test_exact_duplicate_is_kept_once(candles, now):
    result = validate_candles([*candles, candles[0]], now)
    assert len(result.valid) == len(candles)
    assert result.quarantined == []


def test_conflicting_duplicates_are_all_quarantined(candles, now):
    conflict = {**candles[0], "close": candles[0]["close"] + Decimal("0.01")}
    result = validate_candles([*candles, conflict], now, max_reject_ratio=1.0)
    assert Counter(r["_reject_reason"] for r in result.quarantined) == {"conflicting_duplicate": 2}
    assert len(result.valid) == len(candles) - 1


def test_empty_batch_fails_loudly(now):
    with pytest.raises(DataQualityError, match="no records"):
        validate_candles([], now)


def test_too_many_defects_fail_the_whole_batch(candles, now):
    broken = [{**c, "open": Decimal("-1")} for c in candles]
    with pytest.raises(DataQualityError, match="defective"):
        validate_candles(broken, now)
