from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.sources.binance import BinanceSource
from ingestion.sources.frankfurter import FrankfurterSource
from ingestion.sources.registry import load_sources

REPO_CONFIG = Path(__file__).parents[1] / "config" / "sources.yml"


def test_repo_config_builds_binance_source():
    sources = load_sources(REPO_CONFIG)
    binance = sources["binance_klines"]
    assert isinstance(binance, BinanceSource)
    assert binance.params["interval"] == "1h"
    assert set(binance.params["symbols"]) == {"BTCUSDT", "ETHUSDT"}


def test_repo_config_builds_fx_source():
    fx = load_sources(REPO_CONFIG)["fx_rates"]
    assert isinstance(fx, FrankfurterSource)
    assert fx.params == {"base": "USD", "symbols": ["THB"]}


def test_unknown_source_type_is_an_error(tmp_path):
    config = tmp_path / "sources.yml"
    config.write_text("sources:\n  - name: x\n    type: does_not_exist\n")
    with pytest.raises(KeyError):
        load_sources(config)
