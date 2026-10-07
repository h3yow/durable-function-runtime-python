"""Core type definitions: statuses, errors, and execution records."""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


class ExecutionStatus(str, enum.Enum):
    """Lifecycle states of a function execution."""
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED_RETRYABLE = "failed_retryable"   # transient state, will retry
    FAILED = "failed"                       # terminal: retries exhausted
    REJECTED = "rejected"                   # terminal: not accepted (transient:
                                            #   the same key may be resubmitted)
    TIMED_OUT = "timed_out"                 # terminal: time budget exhausted
    CANCELLED = "cancelled"                 # terminal: withdrawn by the caller


class ErrorKind(str, enum.Enum):
    """Classification of failures raised by user functions."""
    RETRYABLE = "retryable"
    NON_RETRYABLE = "non_retryable"


class RetryableError(Exception):
    """Raise from a function to request a retry with backoff."""


class NonRetryableError(Exception):
    """Raise from a function to fail terminally without retries."""


class StepTimedOutError(Exception):
    """A durable step exceeded its per-step time limit.

    Classified as retryable: the step is not checkpointed, so the next
    attempt re-executes it (already-checkpointed steps still replay).
    """

    def __init__(self, step_name: str, limit_seconds: float) -> None:
        super().__init__(
            f"step {step_name!r} exceeded its time limit of "
            f"{limit_seconds:.3f}s")
        self.step_name = step_name
        self.limit_seconds = limit_seconds


class RecoveryError(Exception):
    """Raised when a checkpoint cannot be resumed (missing/incompatible)."""


@dataclass
class RetryPolicy:
    """Backoff configuration for retryable failures."""
    max_attempts: int = 3
    base_delay_seconds: float = 0.1
    backoff_multiplier: float = 2.0
    max_delay_seconds: float = 5.0


@dataclass
class RetryRecord:
    """One retry decision in the execution history."""
    attempt: int
    error: str
    delay_seconds: float


@dataclass
class ExecutionRecord:
    """Persisted state of one invocation (keyed by idempotency key)."""
    idempotency_key: str
    function_name: str
    payload: dict
    status: ExecutionStatus = ExecutionStatus.QUEUED
    attempt: int = 0
    result: object = None
    error: str | None = None
    error_kind: ErrorKind | None = None
    checkpoint_step: int = 0
    checkpoint_data: dict = field(default_factory=dict)
    checkpoint_version: int = 0
    retry_history: list[RetryRecord] = field(default_factory=list)
    created_at: float = 0.0
    updated_at: float = 0.0
    deadline: float | None = None
