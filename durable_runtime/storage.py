"""SQLite-backed persistence for execution records (skeleton)."""
from __future__ import annotations

from .models import ExecutionRecord


class Storage:
    """Durable store for execution records; survives process restarts."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    def load(self, idempotency_key: str) -> ExecutionRecord | None:
        raise NotImplementedError

    def save(self, record: ExecutionRecord) -> None:
        raise NotImplementedError

    def list_incomplete(self) -> list[ExecutionRecord]:
        raise NotImplementedError
