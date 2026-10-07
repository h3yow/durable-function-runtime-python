"""Bounded-concurrency scheduler with a bounded waiting queue.

At most ``max_concurrency`` jobs run at once. At most ``max_queue``
additional accepted jobs may wait; anything beyond that is rejected
synchronously with a distinguishable reason instead of growing memory
without limit.
"""
from __future__ import annotations

import enum
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Callable


class RejectedReason(str, enum.Enum):
    QUEUE_FULL = "queue_full"            # capacity: no queue permit available
    QUOTA_EXCEEDED = "quota_exceeded"    # quota: acceptance budget used up


class RejectedError(Exception):
    """The call was not accepted because the bounded queue is full."""

    def __init__(self, reason: RejectedReason) -> None:
        super().__init__(reason.value)
        self.reason = reason


class Scheduler:
    def __init__(self, max_concurrency: int, max_queue: int) -> None:
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        if max_queue < 0:
            raise ValueError("max_queue must be >= 0")
        self.max_concurrency = max_concurrency
        self.max_queue = max_queue
        self._pool = ThreadPoolExecutor(max_workers=max_concurrency,
                                        thread_name_prefix="durable-worker")
        self._inflight = threading.Semaphore(max_concurrency + max_queue)

    def submit(self, fn: Callable, *args) -> object:
        """Accept a job if a queue permit is free, otherwise reject.

        The permit is held until the job finishes; the inner semaphore
        held for the job's body enforces the concurrency ceiling, so
        accepted-but-waiting jobs never exceed the bounded queue.
        """
        if not self._inflight.acquire(blocking=False):
            raise RejectedError(RejectedReason.QUEUE_FULL)

        def _wrapped() -> object:
            try:
                return fn(*args)
            finally:
                self._inflight.release()

        return self._pool.submit(_wrapped)

    def shutdown(self, wait: bool = True) -> None:
        self._pool.shutdown(wait=wait)
