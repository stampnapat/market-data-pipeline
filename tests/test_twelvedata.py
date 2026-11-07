"""Twelve Data gold source. The fixture is a recorded live /time_series response
(XAU/USD, 2026-10-29 to 2026-11-02, including the weekend bars the API returns)."""

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
    def __init__(self, payload: dict, status_code: int = 200) -> None:
        self.payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self.payload


@pytest.fixture
def calls(monkeypatch) -> list[dict]:
    seen: list[dict] = []
    payload = json.loads(FIXTURE.read_text())

    def fake_get(url, headers, params, timeout):
        seen.append({**params, **headers})
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
            "open": Decimal("3875.34685"),
            "high": Decimal("3969.9869"),
            "low": Decimal("3866.02108"),
            "close": Decimal("3944.31429"),
        }
    ]
    assert calls[0]["Authorization"] == "apikey test-key"  # header, never the URL
    assert "apikey" not in calls[0]
    assert calls[0]["symbol"] == "XAU/USD"


def test_missing_api_key_fails_clearly(source, monkeypatch):
    monkeypatch.delenv("TWELVEDATA_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="TWELVEDATA_API_KEY"):
        source.fetch(utc(2026, 11, 2), utc(2026, 11, 3))


def test_api_error_payload_fails_loudly(source, monkeypatch):
    monkeypatch.setenv("TWELVEDATA_API_KEY", "bad")
    monkeypatch.setattr(
        "ingestion.sources.twelvedata.requests.get",
        lambda url, headers, params, timeout: FakeResponse(
            {"code": 401, "message": "Invalid API key", "status": "error"}, status_code=401
        ),
    )
    with pytest.raises(ValueError, match="Invalid API key"):
        source.fetch(utc(2026, 11, 2), utc(2026, 11, 3))


def test_weekend_bars_are_kept_raw_as_delivered(source, calls):
    """The live API returns Sat/Sun bars although the market is closed. Ingestion keeps
    raw data as the source sent it; dbt staging drops weekend bars."""
    records = source.fetch(utc(2026, 10, 31), utc(2026, 11, 2))  # Sat + Sun
    assert [r["price_date"] for r in records] == [date(2026, 10, 31), date(2026, 11, 1)]


def test_no_data_answer_is_an_empty_window(source, monkeypatch):
    """Live API: a window without bars is HTTP 400 'No data is available...'."""
    monkeypatch.setenv("TWELVEDATA_API_KEY", "test-key")
    no_data = {
        "code": 400,
        "message": "No data is available on the specified dates. Try setting different "
        "start/end dates.",
        "status": "error",
    }
    monkeypatch.setattr(
        "ingestion.sources.twelvedata.requests.get",
        lambda url, headers, params, timeout: FakeResponse(no_data, status_code=400),
    )
    assert source.fetch(utc(2026, 8, 8), utc(2026, 8, 9)) == []


def test_empty_window_is_allowed():
    assert validate_records([], GOLD_SPOT_SPEC, utc(2026, 11, 5)).valid == []


def test_rate_limit_waits_and_retries(source, monkeypatch):
    monkeypatch.setenv("TWELVEDATA_API_KEY", "test-key")
    answers = [
        {
            "code": 429,
            "message": "You have run out of API credits for the current minute.",
            "status": "error",
        },
        json.loads(FIXTURE.read_text()),
    ]
    waits: list[int] = []
    monkeypatch.setattr(
        "ingestion.sources.twelvedata.requests.get",
        lambda url, headers, params, timeout: FakeResponse(answers.pop(0)),
    )
    monkeypatch.setattr("ingestion.sources.twelvedata._sleep", waits.append)
    records = source.fetch(utc(2026, 11, 2), utc(2026, 11, 3))
    assert len(records) == 1
    assert waits == [61]


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
