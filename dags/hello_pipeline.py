from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task

log = logging.getLogger(__name__)


@dag(
    dag_id="hello_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 10, 30),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=10)},
    tags=["tutorial"],
)
def hello_pipeline():
    @task
    def say_hello(**context) -> str:
        run_id = context["run_id"]
        log.info("Hello from run %s, logical_date=%s", run_id, context.get("logical_date"))
        return run_id

    @task
    def say_done(hello_result: str) -> None:
        log.info("Previous task returned: %s", hello_result)

    say_done(say_hello())


hello_pipeline()
