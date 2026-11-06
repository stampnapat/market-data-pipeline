"""Shared pytest fixtures. No test here talks to the real Binance API, MinIO or Postgres."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ingestion.sources.binance import BinanceSource

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def raw_klines() -> list[list]:
    """Three real-shaped 1h candles for BTCUSDT on 2026-11-01 (00:00, 01:00, 02:00 UTC)."""
    return json.loads((FIXTURES / "binance_klines_btcusdt.json").read_text())


class FakeResponse:
    def __init__(self, payload: list) -> None:
        self._payload = payload
        self.headers: dict[str, str] = {}

    def raise_for_status(self) -> None:
        return None

    def json(self) -> list:
        return self._payload


@pytest.fixture
def fake_binance(monkeypatch, raw_klines):
    """Replace requests.get inside the Binance source; record every call's params."""
    calls: list[dict] = []

    def fake_get(url, params, timeout):
        calls.append(params)
        return FakeResponse(raw_klines)

    monkeypatch.setattr("ingestion.sources.binance.requests.get", fake_get)
    return calls


@pytest.fixture
def binance_source() -> BinanceSource:
    return BinanceSource(name="binance_klines", params={"symbols": ["BTCUSDT"], "interval": "1h"})


@pytest.fixture
def candles(binance_source, fake_binance) -> list[dict]:
    """Normalized records, exactly what BinanceSource.fetch returns."""
    start = datetime(2026, 11, 1, tzinfo=UTC)
    return binance_source.fetch(start, datetime(2026, 11, 2, tzinfo=UTC))


@pytest.fixture
def now() -> datetime:
    """A fixed 'current time' well after the fixture candles closed."""
    return datetime(2026, 11, 5, tzinfo=UTC)
