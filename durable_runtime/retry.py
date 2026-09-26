"""Retry/backoff policy computation (skeleton)."""
from __future__ import annotations

from .models import RetryPolicy


def compute_delay(policy: RetryPolicy, attempt: int) -> float:
    """Return the backoff delay before the given attempt number (1-based)."""
    raise NotImplementedError
