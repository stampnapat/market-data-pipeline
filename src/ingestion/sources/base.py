"""Common interface that every data source implements."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, ClassVar

from ingestion.datasets import DatasetSpec, Record

__all__ = ["Record", "Source"]


class Source(ABC):
    """A data source = one dataset we ingest (e.g. Binance klines).

    Subclasses set `type_name` and `spec` and implement `fetch`. What differs
    between pipelines lives in config/sources.yml and in the DatasetSpec.
    """

    type_name: ClassVar[str]
    spec: ClassVar[DatasetSpec]

    def __init__(self, name: str, params: dict[str, Any]) -> None:
        self.name = name
        self.params = params

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> Source:
        """Build a source from one entry of config/sources.yml."""
        return cls(name=config["name"], params=config.get("params", {}))

    @abstractmethod
    def fetch(self, start: datetime, end: datetime) -> list[Record]:
        """Return normalized records for the half-open window [start, end)."""
