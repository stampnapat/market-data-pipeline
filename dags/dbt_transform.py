"""Run dbt after ingestion: staging -> intermediate -> marts, with tests.

Data-aware schedule: runs whenever any ingestion DAG reports fresh rows
(asset `ingested.<source>`), not on a clock. Each run is incremental and
idempotent, so being triggered by several sources in a morning is harmless.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import reduce

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
from pipeline_assets import ingested_asset, source_entries

DBT = "/opt/dbt-venv/bin/dbt"  # separate virtualenv, see docker/airflow/Dockerfile
PROJECT = "--project-dir /opt/pipeline/dbt --profiles-dir /opt/pipeline/dbt"

# ingested.binance_klines | ingested.fx_rates | ingested.thai_gold  (OR: any one triggers)
ANY_SOURCE_UPDATED = reduce(
    lambda a, b: a | b, [ingested_asset(e["name"]) for e in source_entries()]
)


@dag(
    dag_id="dbt_transform",
    schedule=ANY_SOURCE_UPDATED,
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,  # never run two dbt builds against the warehouse at once
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=20),
    },
    tags=["transform", "dbt"],
)
def dbt_transform():
    # `dbt build` = seeds + models + tests in dependency order. A failing test
    # stops the models downstream of it, so bad data never reaches the marts.
    build = BashOperator(task_id="dbt_build", bash_command=f"{DBT} build {PROJECT}")

    # Separate task: a stale source turns this red without blocking the marts above.
    freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{DBT} source freshness {PROJECT}",
        retries=0,
    )

    build >> freshness


dbt_transform()
