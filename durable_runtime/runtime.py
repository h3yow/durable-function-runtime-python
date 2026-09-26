"""Durable runtime: function registration, idempotent invocation, retry,
checkpointed recovery, concurrency control, and timeouts.

Execution model (see docs/README for the full description):

* Each invocation is keyed by an idempotency key.  A duplicated request
  with the same key never re-executes; it observes the same conclusion.
* User functions run against an :class:`ExecutionContext`.  Every
  ``ctx.step(...)`` result is checkpointed before moving on; after a
  restart completed steps replay from the checkpoint and are not rerun.
* Retryable failures retry with exponential backoff up to a ceiling, then
  enter the queryable terminal FAILED state.  Non-retryable errors fail
  immediately.  Deadlines lead to the TIMED_OUT terminal state.
"""
from __future__ import annotations

import enum
import logging
import threading
import time
from concurrent.futures import Future
from typing import Callable

from .models import (
    ErrorKind,
    ExecutionRecord,
    ExecutionStatus,
    NonRetryableError,
    RetryPolicy,
    RetryRecord,
)
from .retry import compute_delay, is_retry_allowed
from .scheduler import RejectedError, Scheduler
from .storage import Storage

logger = logging.getLogger("durable_runtime")


class ResumeRejectedReason(str, enum.Enum):
    """Distinguishable reasons a checkpoint cannot be resumed."""
    UNKNOWN_FUNCTION = "resume_unknown_function"
    VERSION_MISMATCH = "resume_version_mismatch"
    CHECKPOINT_MISSING = "resume_checkpoint_missing"


class ExecutionTimedOut(Exception):
    """Raised inside a function when the execution deadline has passed."""


UserFunction = Callable[["ExecutionContext", dict], object]


class ExecutionContext:
    """Passed to user functions; exposes durable steps and the payload."""

    def __init__(self, runtime: "DurableRuntime", record: ExecutionRecord) -> None:
        self._runtime = runtime
        self._record = record
        # Steps are addressed by call order.  Each attempt starts at 0 so
        # already-checkpointed steps replay their stored results and only
        # steps beyond the checkpoint actually execute.
        self._next_idx = 0

    @property
    def payload(self) -> dict:
        return self._record.payload

    @property
    def idempotency_key(self) -> str:
        return self._record.idempotency_key

    def step(self, name: str, fn: Callable[[], object]) -> object:
        """Run ``fn`` once durably.

        On replay the stored result is returned without calling ``fn``.
        Results are persisted (fsync via SQLite transaction) immediately,
        so a crash after this method returns cannot repeat the step.
        """
        idx = self._next_idx
        key = str(idx)
        self._next_idx += 1
        if key in self._record.checkpoint_data:
            logger.info("[%s] step#%d %s replay -> skipped",
                        self._record.idempotency_key, idx, name)
            return self._record.checkpoint_data[key]

        if self._runtime._deadline_exceeded(self._record):
            raise ExecutionTimedOut()
        logger.info("[%s] step#%d %s execute",
                    self._record.idempotency_key, idx, name)
        value = fn()
        self._record.checkpoint_data[key] = value
        self._record.checkpoint_step = idx
        self._runtime._persist(self._record)
        return value

    def cancelled(self) -> bool:
        return self._runtime._deadline_exceeded(self._record)


class DurableRuntime:
    """Coordinates the registry, storage, retries, and bounded scheduler."""

    def __init__(self, storage: Storage, retry_policy: RetryPolicy | None = None,
                 max_concurrency: int = 4, max_queue: int = 16,
                 execution_timeout: float | None = 30.0,
                 checkpoint_version: int = 1,
                 sleeper: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.storage = storage
        self.retry_policy = retry_policy or RetryPolicy()
        self.execution_timeout = execution_timeout
        self.checkpoint_version = checkpoint_version
        self._sleeper = sleeper
        self._clock = clock
        self._functions: dict[str, UserFunction] = {}
        # idempotency key -> Future of its single in-process execution
        self._inflight: dict[str, Future] = {}
        self._inflight_mu = threading.Lock()
        self._scheduler = Scheduler(max_concurrency, max_queue)

    def register(self, name: str, fn: UserFunction) -> None:
        if name in self._functions:
            raise ValueError(f"function already registered: {name}")
        self._functions[name] = fn

    def invoke(self, function_name: str, idempotency_key: str,
               payload: dict | None = None,
               wait_timeout: float | None = None) -> ExecutionRecord:
        """Invoke with idempotent deduplication; waits for the terminal state.

        Concurrent duplicate calls share the single execution's future and
        get the same record.  A duplicate after completion returns the
        persisted conclusion without rerunning anything.
        """
        if function_name not in self._functions:
            raise ValueError(f"unknown function: {function_name}")
        payload = payload or {}
        now = self._clock()

        with self._inflight_mu:
            existing_future = self._inflight.get(idempotency_key)
            if existing_future is None:
                record = self.storage.load(idempotency_key)
                if record is not None and record.status in _TERMINAL:
                    logger.info("[%s] duplicate completed call -> return stored %s",
                                idempotency_key, record.status.value)
                    return record
                if record is None:
                    record = ExecutionRecord(
                        idempotency_key=idempotency_key,
                        function_name=function_name,
                        payload=payload,
                        status=ExecutionStatus.QUEUED,
                        checkpoint_version=self.checkpoint_version,
                        created_at=now,
                        updated_at=now,
                        deadline=(now + self.execution_timeout
                                  if self.execution_timeout else None),
                    )
                    self._persist(record)
                    logger.info("[%s] accepted new invocation of %s",
                                idempotency_key, function_name)
                else:
                    logger.info("[%s] found non-terminal %s record -> resuming",
                                idempotency_key, record.status.value)
                    self._validate_resume(record)
                future = self._accept(record)
            else:
                future = existing_future
                logger.info("[%s] concurrent duplicate call -> sharing execution",
                            idempotency_key)

        try:
            future.result(timeout=wait_timeout)
        except TimeoutError:
            logger.info("[%s] wait timeout, current state queryable",
                        idempotency_key)
        return self.get(idempotency_key)  # type: ignore[return-value]

    def get(self, idempotency_key: str) -> ExecutionRecord | None:
        return self.storage.load(idempotency_key)

    def recover(self) -> list[ExecutionRecord]:
        """On startup: resume or explicitly reject interrupted executions.

        Returns the affected records.  Records whose checkpoint is missing
        or version-incompatible, or whose function is unregistered, are
        moved to the terminal FAILED state with a distinguishable reason
        (never silently rerun from scratch or skipped).
        """
        affected: list[ExecutionRecord] = []
        records = self.storage.list_incomplete()
        logger.info("recovery scan: %d interrupted execution(s)", len(records))
        for record in records:
            if self._resume_or_reject(record):
                affected.append(record)
        return affected

    def shutdown(self) -> None:
        self._scheduler.shutdown(wait=True)

    # ------------------------------------------------------------------ internals

    def _accept(self, record: ExecutionRecord) -> Future:
        try:
            future: Future = self._scheduler.submit(self._run_attempts, record)
        except RejectedError as exc:
            self._transition(
                record, ExecutionStatus.REJECTED,
                error=f"rejected:{exc.reason.value}",
                error_kind=ErrorKind.NON_RETRYABLE)
            raise
        self._inflight[record.idempotency_key] = future
        return future

    def _run_attempts(self, record: ExecutionRecord) -> None:
        key = record.idempotency_key
        try:
            while True:
                record.attempt += 1
                self._transition(record, ExecutionStatus.RUNNING)
                ctx = ExecutionContext(self, record)
                try:
                    result = self._functions[record.function_name](ctx, record.payload)
                except ExecutionTimedOut:
                    self._transition(record, ExecutionStatus.TIMED_OUT,
                                     error="execution deadline exceeded")
                    return
                except Exception as exc:  # noqa: BLE001 - classified below
                    if not self._handle_failure(record, exc):
                        return
                    continue
                else:
                    record.result = result
                    self._transition(record, ExecutionStatus.SUCCEEDED)
                    return
        finally:
            with self._inflight_mu:
                if self._inflight.get(key) is not None:
                    del self._inflight[key]

    def _handle_failure(self, record: ExecutionRecord, exc: Exception) -> bool:
        """Persist the failure; return True iff another attempt may run."""
        if self._deadline_exceeded(record):
            self._transition(record, ExecutionStatus.TIMED_OUT,
                             error="execution deadline exceeded")
            return False
        if isinstance(exc, NonRetryableError):
            self._transition(record, ExecutionStatus.FAILED,
                             error=str(exc), error_kind=ErrorKind.NON_RETRYABLE)
            return False
        if is_retry_allowed(self.retry_policy, record.attempt):
            delay = compute_delay(self.retry_policy, record.attempt + 1)
            record.retry_history.append(
                RetryRecord(attempt=record.attempt, error=str(exc),
                            delay_seconds=delay))
            self._transition(record, ExecutionStatus.FAILED_RETRYABLE,
                             error=str(exc), error_kind=ErrorKind.RETRYABLE)
            logger.info("[%s] retryable failure attempt=%d -> retry in %.3fs",
                        record.idempotency_key, record.attempt, delay)
            self._sleeper(delay)
            return True
        self._transition(record, ExecutionStatus.FAILED,
                         error=f"retries exhausted: {exc}",
                         error_kind=ErrorKind.RETRYABLE)
        return False

    def _resume_or_reject(self, record: ExecutionRecord) -> bool:
        """Validate a recovered record; submit it or mark a terminal reason."""
        try:
            self._validate_resume(record)
        except ResumeRejectedError as exc:
            self._transition(record, ExecutionStatus.FAILED,
                             error=f"resume_rejected:{exc.reason.value}: {exc}",
                             error_kind=ErrorKind.NON_RETRYABLE)
            logger.warning("[%s] recovery rejected: %s",
                           record.idempotency_key, exc.reason.value)
            return False
        with self._inflight_mu:
            if record.idempotency_key in self._inflight:
                return False  # already running in this process
            self._accept(record)
        return True

    def _validate_resume(self, record: ExecutionRecord) -> None:
        if record.function_name not in self._functions:
            raise ResumeRejectedError(
                ResumeRejectedReason.UNKNOWN_FUNCTION,
                f"function {record.function_name!r} is not registered")
        if record.checkpoint_version != self.checkpoint_version:
            raise ResumeRejectedError(
                ResumeRejectedReason.VERSION_MISMATCH,
                f"checkpoint v{record.checkpoint_version} != runtime "
                f"v{self.checkpoint_version}")
        # RUNNING means an attempt had started; its recorded step index must
        # have a corresponding durable checkpoint value.
        if (record.status == ExecutionStatus.RUNNING
                and record.checkpoint_step > 0
                and str(record.checkpoint_step) not in record.checkpoint_data):
            raise ResumeRejectedError(
                ResumeRejectedReason.CHECKPOINT_MISSING,
                f"step #{record.checkpoint_step} has no checkpoint value")

    def _deadline_exceeded(self, record: ExecutionRecord) -> bool:
        return record.deadline is not None and self._clock() > record.deadline

    def _persist(self, record: ExecutionRecord) -> None:
        record.updated_at = self._clock()
        self.storage.save(record)

    def _transition(self, record: ExecutionRecord,
                    status: ExecutionStatus, error: str | None = None,
                    error_kind: ErrorKind | None = None) -> None:
        old = record.status.value
        record.status = status
        record.updated_at = self._clock()
        if error is not None:
            record.error = error
        if error_kind is not None:
            record.error_kind = error_kind
        self.storage.save(record)
        logger.info("[%s] transition %s -> %s (attempt=%d)",
                    record.idempotency_key, old, status.value, record.attempt)


_TERMINAL = {
    ExecutionStatus.SUCCEEDED,
    ExecutionStatus.FAILED,
    ExecutionStatus.REJECTED,
    ExecutionStatus.TIMED_OUT,
}


class ResumeRejectedError(Exception):
    def __init__(self, reason: ResumeRejectedReason, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail
