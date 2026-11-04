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
