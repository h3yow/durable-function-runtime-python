"""Retry/backoff policy computation."""
from __future__ import annotations

from .models import RetryPolicy


def compute_delay(policy: RetryPolicy, attempt: int) -> float:
    """Delay before the given attempt (1-based): 0 for the first attempt,
    then exponentially increasing capped delay."""
    if attempt <= 1:
        return 0.0
    raw = policy.base_delay_seconds * policy.backoff_multiplier ** (attempt - 2)
    return min(raw, policy.max_delay_seconds)


def is_retry_allowed(policy: RetryPolicy, attempt: int) -> bool:
    """True when another attempt may be made after failure at ``attempt``."""
    return attempt < policy.max_attempts
