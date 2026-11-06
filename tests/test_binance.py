from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pendulum
import pytest

from ingestion.sources.binance import BinanceSource, to_ms

START = datetime(2026, 11, 1, tzinfo=UTC)
END = START + timedelta(days=1)


def test_normalize_gives_named_fields_with_exact_types(candles):
    first = candles[0]
    assert first["symbol"] == "BTCUSDT"
    assert first["open_time"] == START
    assert first["open_time"].tzinfo is not None  # always timezone-aware UTC
    assert first["close"] == Decimal("77690.41")
    assert isinstance(first["close"], Decimal)  # never float for money
    assert first["trade_count"] == 37826


def test_request_window_is_half_open(binance_source, fake_binance):
    binance_source.fetch(START, END)
    params = fake_binance[0]
    assert params["startTime"] == to_ms(START)
    assert params["endTime"] == to_ms(END) - 1  # excludes the next day's 00:00 candle


def test_fetch_calls_api_once_per_symbol(fake_binance):
    source = BinanceSource("b", {"symbols": ["BTCUSDT", "ETHUSDT"], "interval": "1h"})
    records = source.fetch(START, END)
    assert [c["symbol"] for c in fake_binance] == ["BTCUSDT", "ETHUSDT"]
    assert len(records) == 6  # 3 fixture rows x 2 symbols


def test_window_larger_than_api_limit_is_rejected(binance_source, fake_binance):
    with pytest.raises(ValueError, match="exceeds"):
        binance_source.fetch(START, START + timedelta(hours=1001))
    assert fake_binance == []  # failed before calling the API


def test_accepts_pendulum_datetimes_from_airflow(binance_source, fake_binance):
    """Regression: Airflow passes pendulum datetimes; dividing their difference
    by a stdlib timedelta used to raise AttributeError (episode 10)."""
    start = pendulum.datetime(2026, 11, 1, tz="UTC")
    records = binance_source.fetch(start, start.add(days=1))
    assert len(records) == 3
