"""Storage tests use a local folder as the 'bucket', so no MinIO is needed."""

from __future__ import annotations

from datetime import date

import pyarrow.parquet as pq
from pyarrow import fs

from ingestion.sources.binance import CANDLE_SPEC
from ingestion.storage.minio import (
    partition_path,
    quarantine_path,
    raw_schema,
    write_quarantine,
    write_raw,
)

DAY = date(2026, 11, 1)


def test_partition_paths_follow_hive_layout():
    assert (
        partition_path("raw", "binance_klines", "BTCUSDT", DAY)
        == "raw/binance_klines/symbol=BTCUSDT/date=2026-11-01/data.parquet"
    )
    assert (
        quarantine_path("raw", "binance_klines", DAY)
        == "raw/_quarantine/binance_klines/date=2026-11-01/rejects.parquet"
    )


def test_write_raw_creates_one_typed_file_per_symbol_and_day(candles, tmp_path):
    local = fs.LocalFileSystem()
    paths = write_raw(candles, CANDLE_SPEC, "binance_klines", local, str(tmp_path))

    assert paths == [partition_path(str(tmp_path), "binance_klines", "BTCUSDT", DAY)]
    table = pq.read_table(paths[0])
    assert table.num_rows == len(candles)
    assert table.schema.remove_metadata() == raw_schema(CANDLE_SPEC)


def test_rewriting_a_day_overwrites_instead_of_appending(candles, tmp_path):
    local = fs.LocalFileSystem()
    write_raw(candles, CANDLE_SPEC, "binance_klines", local, str(tmp_path))
    paths = write_raw(candles, CANDLE_SPEC, "binance_klines", local, str(tmp_path))

    assert pq.read_table(paths[0]).num_rows == len(candles)  # not doubled
    files = list(tmp_path.rglob("*.parquet"))
    assert len(files) == 1


def test_quarantine_file_is_removed_when_a_rerun_is_clean(candles, tmp_path):
    local = fs.LocalFileSystem()
    rejects = [{**candles[0], "_reject_reason": "inconsistent_ohlc"}]

    path = write_quarantine(rejects, CANDLE_SPEC, "binance_klines", DAY, local, str(tmp_path))
    assert pq.read_table(path).column("_reject_reason").to_pylist() == ["inconsistent_ohlc"]

    assert write_quarantine([], CANDLE_SPEC, "binance_klines", DAY, local, str(tmp_path)) is None
    assert local.get_file_info(path).type == fs.FileType.NotFound
