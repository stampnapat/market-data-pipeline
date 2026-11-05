"""Airflow Assets shared between DAGs (data-aware scheduling).

Each ingestion DAG marks `ingested.<source>` as updated when its staging load
succeeds; the dbt DAG is scheduled on those assets instead of on a clock.
"""

from __future__ import annotations

import os
from pathlib import Path

import yaml
from airflow.sdk import Asset

CONFIG_PATH = os.environ.get("PIPELINE_CONFIG", "/opt/pipeline/config/sources.yml")


def source_entries() -> list[dict]:
    """Entries of config/sources.yml (cheap: YAML only, no heavy imports at parse time)."""
    return yaml.safe_load(Path(CONFIG_PATH).read_text())["sources"]


def ingested_asset(source_name: str) -> Asset:
    """'Fresh rows for <source> are in staging' as an Airflow Asset."""
    return Asset(f"ingested.{source_name}")
