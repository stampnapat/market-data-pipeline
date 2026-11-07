"""Twelve Data gold source. The fixture follows the documented /time_series shape;
re-record it from a live response once an API key is configured."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from ingestion.sources.twelvedata import GOLD_SPOT_SPEC, TwelveDataSource
from ingestion.validation import check_daily_ohlc, validate_records

FIXTURE = Path(__file__).parent / "fixtures" / "twelvedata_xauusd_daily.json"


class FakeResponse:
    def __init__(self, payload: dict) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self.payload


@pytest.fixture
def calls(monkeypatch) -> list[dict]:
    seen: list[dict] = []
    payload = json.loads(FIXTURE.read_text())

    def fake_get(url, params, timeout):
        seen.append(params)
        return FakeResponse(payload)

    monkeypatch.setenv("TWELVEDATA_API_KEY", "test-key")
    monkeypatch.setattr("ingestion.sources.twelvedata.requests.get", fake_get)
    return seen


@pytest.fixture
def source() -> TwelveDataSource:
    return TwelveDataSource("gold_spot", {"symbols": ["XAU/USD"], "interval": "1day"})


def utc(y: int, m: int, d: int) -> datetime:
    return datetime(y, m, d, tzinfo=UTC)


def test_bars_inside_window_only_with_exact_decimals(source, calls):
    records = source.fetch(utc(2026, 11, 2), utc(2026, 11, 3))  # just Monday 2 Nov
    assert records == [
        {
            "symbol": "XAUUSD",  # slash removed: symbol becomes a path segment
            "price_date": date(2026, 11, 2),
            "open": Decimal("3921.40000"),
            "high": Decimal("3940.00000"),
            "low": Decimal("3899.75000"),
            "close": Decimal("3909.40000"),
        }
    ]
    assert calls[0]["apikey"] == "test-key"
    assert calls[0]["symbol"] == "XAU/USD"


def test_missing_api_key_fails_clearly(source, monkeypatch):
    monkeypatch.delenv("TWELVEDATA_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="TWELVEDATA_API_KEY"):
        source.fetch(utc(2026, 11, 2), utc(2026, 11, 3))


def test_api_error_payload_fails_loudly(source, monkeypatch):
    monkeypatch.setenv("TWELVEDATA_API_KEY", "bad")
    monkeypatch.setattr(
        "ingestion.sources.twelvedata.requests.get",
        lambda url, params, timeout: FakeResponse(
            {"code": 401, "message": "Invalid API key", "status": "error"}
        ),
    )
    with pytest.raises(ValueError, match="Invalid API key"):
        source.fetch(utc(2026, 11, 2), utc(2026, 11, 3))


def test_weekend_window_is_empty_and_allowed(source, calls):
    records = source.fetch(utc(2026, 10, 31), utc(2026, 11, 2))  # Sat + Sun
    assert records == []
    assert validate_records(records, GOLD_SPOT_SPEC, utc(2026, 11, 5)).valid == []


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"close": None}, "missing_value"),
        ({"open": Decimal("0")}, "non_positive_price"),
        ({"high": Decimal("1")}, "inconsistent_ohlc"),
    ],
)
def test_daily_ohlc_rules(change, reason):
    bar = {
        "symbol": "XAUUSD",
        "price_date": date(2026, 11, 2),
        "open": Decimal("3921.4"),
        "high": Decimal("3940"),
        "low": Decimal("3899.75"),
        "close": Decimal("3909.4"),
    }
    assert check_daily_ohlc(bar, utc(2026, 11, 5)) is None
    assert check_daily_ohlc({**bar, **change}, utc(2026, 11, 5)) == reason
