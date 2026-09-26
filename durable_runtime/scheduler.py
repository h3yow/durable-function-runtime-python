"""Bounded-concurrency scheduler with bounded queue (skeleton)."""
from __future__ import annotations


class Scheduler:
    """Limits concurrent executions; excess calls queue or are rejected."""

    def __init__(self, max_concurrency: int, max_queue: int) -> None:
        self.max_concurrency = max_concurrency
        self.max_queue = max_queue
