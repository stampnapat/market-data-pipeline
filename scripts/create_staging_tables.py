"""Create every source's (empty) staging table from its DatasetSpec DDL.

Used by CI so dbt can compile and build against a fresh Postgres without running
ingestion. Idempotent: the DDL is CREATE TABLE IF NOT EXISTS.
"""

from __future__ import annotations

import os
from pathlib import Path

from ingestion.settings import WarehouseSettings
from ingestion.sources.registry import load_sources
from ingestion.storage.postgres import connect

CONFIG = Path(os.environ.get("PIPELINE_CONFIG", Path(__file__).parents[1] / "config/sources.yml"))


def main() -> None:
    conn = connect(WarehouseSettings.from_env())
    try:
        with conn, conn.cursor() as cur:
            for name, source in load_sources(CONFIG).items():
                cur.execute(source.spec.staging_ddl)
                print(f"ok  {source.spec.staging_table}  ({name})")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
