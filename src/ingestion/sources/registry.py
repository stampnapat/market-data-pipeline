"""Build Source objects from config/sources.yml (config-driven, like a pipeline config table)."""

from __future__ import annotations

from pathlib import Path

import yaml

from ingestion.sources.base import Source
from ingestion.sources.binance import BinanceSource
from ingestion.sources.frankfurter import FrankfurterSource
from ingestion.sources.thai_gold import ThaiGoldSource
from ingestion.sources.twelvedata import TwelveDataSource

# type name in YAML -> class. Adding a new source type = one line here.
SOURCE_TYPES: dict[str, type[Source]] = {
    BinanceSource.type_name: BinanceSource,
    FrankfurterSource.type_name: FrankfurterSource,
    ThaiGoldSource.type_name: ThaiGoldSource,
    TwelveDataSource.type_name: TwelveDataSource,
}


def load_sources(config_path: str | Path) -> dict[str, Source]:
    """Return {source name: Source object} for every entry in the config file."""
    config = yaml.safe_load(Path(config_path).read_text())
    sources: dict[str, Source] = {}
    for entry in config["sources"]:
        source_cls = SOURCE_TYPES[entry["type"]]
        sources[entry["name"]] = source_cls.from_config(entry)
    return sources
