from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pendulum
import pytest

from ingestion.sources.thai_gold import (
    THAI_GOLD_SPEC,
    ThaiGoldSource,
    parse_announcement,
    parse_baht,
)
from ingestion.validation import check_thai_gold, validate_records

FIXTURE = Path(__file__).parent / "fixtures" / "thai_gold_latest.json"
# The fixture's announcement: 05/11/2569 11:26 Bangkok time, round 11
ANNOUNCED = datetime(2026, 11, 5, 4, 26, tzinfo=UTC)
DAY_START = datetime(2026, 11, 5, tzinfo=UTC)


class FakeResponse:
    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return json.loads(FIXTURE.read_text())


@pytest.fixture
def source(monkeypatch) -> ThaiGoldSource:
    calls: list[dict] = []

    def fake_get(url, headers, timeout):
        calls.append(headers)
        return FakeResponse()

    monkeypatch.setattr("ingestion.sources.thai_gold.requests.get", fake_get)
    src = ThaiGoldSource("thai_gold", {"url": "https://example.test/latest"})
    src.calls = calls  # type: ignore[attr-defined]
    return src


def test_buddhist_date_and_bangkok_time_become_utc():
    announced_at, number = parse_announcement("05/11/2569", "เวลา 11:26 น. (ครั้งที่ 11)")
    assert announced_at == ANNOUNCED  # 11:26 Bangkok = 04:26 UTC, 2569 BE = 2026 CE
    assert number == 11


def test_unknown_time_format_fails_loudly():
    with pytest.raises(ValueError, match="Unrecognised"):
        parse_announcement("05/11/2569", "updated just now")


def test_baht_text_parses_to_exact_decimal():
    assert parse_baht("65,250.00") == Decimal("65250.00")


def test_announcement_inside_window_gives_bar_and_ornament(source):
    records = source.fetch(DAY_START, DAY_START + timedelta(days=1))
    by_symbol = {r["symbol"]: r for r in records}
    assert set(by_symbol) == {"TH_GOLD_BAR_965", "TH_GOLD_ORNAMENT_965"}
    bar = by_symbol["TH_GOLD_BAR_965"]
    assert (bar["buy_price"], bar["sell_price"]) == (Decimal("65250.00"), Decimal("65450.00"))
    assert bar["announced_at"] == ANNOUNCED and bar["announcement_no"] == 11
    assert "educational" in source.calls[0]["User-Agent"]  # identifies itself politely


def test_past_window_is_empty_because_there_is_no_history(source):
    yesterday = DAY_START - timedelta(days=1)
    assert source.fetch(yesterday, DAY_START) == []


def test_accepts_pendulum_window_from_airflow(source):
    start = pendulum.datetime(2026, 11, 5, tz="UTC")
    assert len(source.fetch(start, start.add(days=1))) == 2


def test_empty_day_is_allowed():
    assert validate_records([], THAI_GOLD_SPEC, ANNOUNCED).valid == []


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"buy_price": None}, "missing_value"),
        ({"buy_price": Decimal("0")}, "non_positive_price"),
        ({"sell_price": Decimal("65000")}, "negative_spread"),
    ],
)
def test_thai_gold_rules(change, reason):
    quote = {
        "symbol": "TH_GOLD_BAR_965",
        "announced_at": ANNOUNCED,
        "buy_price": Decimal("65250"),
        "sell_price": Decimal("65450"),
    }
    assert check_thai_gold(quote, ANNOUNCED) is None
    assert check_thai_gold({**quote, **change}, ANNOUNCED) == reason
