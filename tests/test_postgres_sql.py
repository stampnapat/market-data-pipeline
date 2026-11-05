"""The loader builds its SQL from each DatasetSpec; check the generated upserts."""

from __future__ import annotations

from ingestion.sources.binance import CANDLE_SPEC
from ingestion.sources.frankfurter import FX_SPEC
from ingestion.storage.postgres import staging_columns, upsert_sql


def test_upsert_targets_the_natural_key():
    assert "ON CONFLICT (symbol, open_time) DO UPDATE" in upsert_sql(CANDLE_SPEC)
    assert "ON CONFLICT (symbol, rate_date) DO UPDATE" in upsert_sql(FX_SPEC)


def test_key_columns_are_never_overwritten():
    sql = upsert_sql(FX_SPEC)
    updates = sql.split("DO UPDATE SET", 1)[1]
    assert "symbol = EXCLUDED" not in updates
    assert "rate_date = EXCLUDED" not in updates
    assert "rate = EXCLUDED.rate" in updates
    assert updates.rstrip().endswith("_loaded_at = now();")


def test_control_columns_are_loaded_after_dataset_columns():
    assert staging_columns(FX_SPEC)[-2:] == ["_ingested_at", "_source_file"]
    assert staging_columns(CANDLE_SPEC)[:2] == ["symbol", "open_time"]
