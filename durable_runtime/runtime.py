"""Durable runtime: registration, idempotent invocation, recovery (skeleton)."""
from __future__ import annotations

from .models import ExecutionRecord, RetryPolicy
from .storage import Storage


class DurableRuntime:
    """Coordinates function registry, storage, retries, and scheduling."""

    SCHEMA_VERSION = 1

    def __init__(self, storage: Storage, retry_policy: RetryPolicy | None = None,
                 max_concurrency: int = 4, max_queue: int = 16) -> None:
        self.storage = storage
        self.retry_policy = retry_policy or RetryPolicy()

    def register(self, name: str, fn) -> None:
        raise NotImplementedError

    def invoke(self, function_name: str, idempotency_key: str,
               payload: dict) -> ExecutionRecord:
        raise NotImplementedError

    def get(self, idempotency_key: str) -> ExecutionRecord | None:
        raise NotImplementedError

    def recover(self) -> list[ExecutionRecord]:
        raise NotImplementedError
