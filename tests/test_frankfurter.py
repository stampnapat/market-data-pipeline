from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from ingestion.sources.frankfurter import FX_SPEC, FrankfurterSource
from ingestion.validation import check_fx_rate, validate_records

FIXTURES = Path(__file__).parent / "fixtures"
NOW = datetime(2026, 11, 5, tzinfo=UTC)


class FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None

    def json(self, **kwargs):
        import json

        return json.loads(self.text, **kwargs)


@pytest.fixture
def fake_frankfurter(monkeypatch):
    """Serve a saved Frankfurter response; record requested URLs."""
    calls: list[str] = []

    def install(fixture_name: str) -> list[str]:
        text = (FIXTURES / fixture_name).read_text()

        def fake_get(url, params, timeout):
            calls.append(url)
            return FakeResponse(text)

        monkeypatch.setattr("ingestion.sources.frankfurter.requests.get", fake_get)
        return calls

    return install


@pytest.fixture
def source() -> FrankfurterSource:
    return FrankfurterSource("fx_rates", {"base": "USD", "symbols": ["THB"]})


def utc(y: int, m: int, d: int) -> datetime:
    return datetime(y, m, d, tzinfo=UTC)


def test_range_returns_one_record_per_business_day(source, fake_frankfurter):
    calls = fake_frankfurter("frankfurter_usd_thb_fri_to_mon.json")
    records = source.fetch(utc(2026, 10, 30), utc(2026, 11, 3))  # Fri..Mon, end exclusive

    assert calls == ["https://api.frankfurter.dev/v1/2026-10-30..2026-11-02"]
    assert [r["rate_date"] for r in records] == [date(2026, 10, 30), date(2026, 11, 2)]
    assert records[0] == {
        "symbol": "USDTHB",
        "base_currency": "USD",
        "quote_currency": "THB",
        "rate_date": date(2026, 10, 30),
        "rate": Decimal("33.73"),
    }


def test_rates_are_exact_decimals_not_floats(source, fake_frankfurter):
    fake_frankfurter("frankfurter_usd_thb_fri_to_mon.json")
    records = source.fetch(utc(2026, 10, 30), utc(2026, 11, 3))
    assert all(isinstance(r["rate"], Decimal) for r in records)


def test_snapped_weekend_rate_is_ignored(source, fake_frankfurter):
    """Asking for Sunday makes the API answer with Friday's rate; that is not Sunday's data."""
    fake_frankfurter("frankfurter_usd_thb_sunday_snapped.json")
    assert source.fetch(utc(2026, 11, 1), utc(2026, 11, 2)) == []


def test_empty_day_is_allowed_for_fx():
    result = validate_records([], FX_SPEC, NOW)
    assert result.valid == [] and result.quarantined == []


@pytest.mark.parametrize(
    ("change", "reason"),
    [({"rate": None}, "missing_value"), ({"rate": Decimal("0")}, "non_positive_rate")],
)
def test_fx_rules(change, reason):
    record = {"symbol": "USDTHB", "rate_date": date(2026, 11, 2), "rate": Decimal("33.76")}
    assert check_fx_rate(record, NOW) is None
    assert check_fx_rate({**record, **change}, NOW) == reason
