"""Runtime settings read from environment variables (never hardcode credentials)."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class MinioSettings:
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str

    @classmethod
    def from_env(cls) -> MinioSettings:
        return cls(
            # localhost:9000 from your laptop; containers override with minio:9000
            endpoint=os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.environ["MINIO_ROOT_USER"],
            secret_key=os.environ["MINIO_ROOT_PASSWORD"],
            bucket=os.environ.get("MINIO_BUCKET", "raw"),
        )


@dataclass(frozen=True)
class WarehouseSettings:
    host: str
    port: int
    dbname: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> WarehouseSettings:
        return cls(
            # localhost from your laptop; containers override with postgres
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=int(os.environ.get("POSTGRES_PORT", "5432")),
            dbname=os.environ["WAREHOUSE_DB"],
            user=os.environ["LOADER_USER"],
            password=os.environ["LOADER_PASSWORD"],
        )
