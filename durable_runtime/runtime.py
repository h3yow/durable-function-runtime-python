"""Durable runtime: function registration, idempotent invocation, retry,
checkpointed recovery, concurrency control, timeouts, and cancellation.

Execution model (see docs/README for the full description):

* Each invocation is keyed by an idempotency key.  A duplicated request
  with the same key never re-executes; it observes the same conclusion.
* User functions run against an :class:`ExecutionContext`.  Every
  ``ctx.step(...)`` result is checkpointed before moving on; after a
  restart completed steps replay from the checkpoint and are not rerun.
* Retryable failures retry with exponential backoff up to a ceiling, then
  enter the queryable terminal FAILED state.  Non-retryable errors fail
  immediately.  Deadlines lead to the TIMED_OUT terminal state.  A caller
  may withdraw a queued/running invocation via ``cancel()`` which lands in
  the distinguishable CANCELLED terminal state.
* Every invocation has a time budget (per-call ``timeout`` or the global
  ``execution_timeout``).  Queueing, backoff waits, and execution all
  count against the single persisted deadline; once it is exhausted the
  execution converges to TIMED_OUT instead of retrying forever.
* Rejection (capacity or quota) is transient: the idempotency key is not
  burned and the same key may be resubmitted later.
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
from .scheduler import RejectedError, RejectedReason, Scheduler
from .storage import Storage

logger = logging.getLogger("durable_runtime")


class ResumeRejectedReason(str, enum.Enum):
    """Distinguishable reasons a checkpoint cannot be resumed."""
    UNKNOWN_FUNCTION = "resume_unknown_function"
    VERSION_MISMATCH = "resume_version_mismatch"
    CHECKPOINT_MISSING = "resume_checkpoint_missing"


class ExecutionTimedOut(Exception):
    """Raised inside a function when the execution deadline has passed."""


class ExecutionCancelled(Exception):
    """Raised inside a function/attempt when the caller cancelled it."""


class StepTimedOut(Exception):
    """A durable step exceeded its per-step time limit (retryable)."""


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

    def step(self, name: str, fn: Callable[[], object],
             timeout: float | None = None) -> object:
        """Run ``fn`` once durably.

        On replay the stored result is returned without calling ``fn``.
        Results are persisted (fsync via SQLite transaction) immediately,
        so a crash after this method returns cannot repeat the step.

        ``timeout`` bounds this step's wall-clock duration.  A step that
        exceeds its limit raises :class:`StepTimedOut`, which is classified
        as a retryable failure: the step's result is not checkpointed and
        the attempt is retried with backoff (still bounded by the overall
        execution budget).  Already-checkpointed steps are not rerun.
        """
        idx = self._next_idx
        key = str(idx)
        self._next_idx += 1
        if key in self._record.checkpoint_data:
            logger.info("[%s] step#%d %s replay -> skipped",
                        self._record.idempotency_key, idx, name)
            return self._record.checkpoint_data[key]

        self._runtime._raise_if_interrupted(self._record)
        logger.info("[%s] step#%d %s execute",
                    self._record.idempotency_key, idx, name)
        start = self._runtime._clock()
        value = fn()
        if timeout is not None and self._runtime._clock() - start > timeout:
            # The step's side effect already happened, but it breached its
            # own time limit: do not checkpoint it; retry the attempt.
            raise StepTimedOut(
                f"step {name!r} exceeded its {timeout}s limit")
        self._runtime._raise_if_interrupted(self._record)
        self._record.checkpoint_data[key] = value
        self._record.checkpoint_step = idx
        self._runtime._persist(self._record)
        return value

    def cancelled(self) -> bool:
        """True when the execution should stop: cancelled or out of budget."""
        return (self._runtime._cancel_requested(self._record)
                or self._runtime._deadline_exceeded(self._record))


class DurableRuntime:
    """Coordinates the registry, storage, retries, and bounded scheduler."""

    def __init__(self, storage: Storage, retry_policy: RetryPolicy | None = None,
                 max_concurrency: int = 4, max_queue: int = 16,
                 execution_timeout: float | None = 30.0,
                 invocation_quota: int | None = None,
                 checkpoint_version: int = 1,
                 sleeper: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.storage = storage
        self.retry_policy = retry_policy or RetryPolicy()
        self.execution_timeout = execution_timeout
        self.invocation_quota = invocation_quota
        self.checkpoint_version = checkpoint_version
        self._sleeper = sleeper
        self._clock = clock
        self._functions: dict[str, UserFunction] = {}
        # idempotency key -> Future of its single in-process execution
        self._inflight: dict[str, Future] = {}
        # idempotency key -> cooperative cancellation flag of that execution
        self._cancel_events: dict[str, threading.Event] = {}
        self._inflight_mu = threading.Lock()
        self._scheduler = Scheduler(max_concurrency, max_queue)

    def register(self, name: str, fn: UserFunction) -> None:
        if name in self._functions:
            raise ValueError(f"function already registered: {name}")
        self._functions[name] = fn

    def invoke(self, function_name: str, idempotency_key: str,
               payload: dict | None = None,
               wait_timeout: float | None = None,
               timeout: float | None = None) -> ExecutionRecord:
        """Invoke with idempotent deduplication; waits for the terminal state.

        ``timeout`` is this call's own time budget (queueing + backoff +
        execution); when omitted the runtime-wide ``execution_timeout``
        applies.  Concurrent duplicate calls share the single execution's
        future and get the same record.  A duplicate after completion
        returns the persisted conclusion without rerunning anything.  A key
        whose previous call was *rejected* (capacity/quota) is accepted
        again as a fresh invocation: rejection never burns the key.
        """
        if function_name not in self._functions:
            raise ValueError(f"unknown function: {function_name}")
        payload = payload or {}
        now = self._clock()
        budget = timeout if timeout is not None else self.execution_timeout

        with self._inflight_mu:
            existing_future = self._inflight.get(idempotency_key)
            if existing_future is None:
                record = self.storage.load(idempotency_key)
                if record is not None and record.status in _TERMINAL:
                    if record.status is not ExecutionStatus.REJECTED:
                        logger.info(
                            "[%s] duplicate completed call -> return stored %s",
                            idempotency_key, record.status.value)
                        return record
                    # Transient rejection: nothing ever ran for this key, so
                    # it may be resubmitted as a fresh invocation.
                    logger.info("[%s] prior rejection (%s) is transient -> "
                                "resubmitting as a fresh invocation",
                                idempotency_key, record.error)
                    record = self._fresh_record(
                        record, function_name, payload, now, budget)
                    self._check_quota(record)
                    self._persist(record)
                elif record is None:
                    record = ExecutionRecord(
                        idempotency_key=idempotency_key,
                        function_name=function_name,
                        payload=payload,
                        status=ExecutionStatus.QUEUED,
                        checkpoint_version=self.checkpoint_version,
                        created_at=now,
                        updated_at=now,
                        deadline=(now + budget if budget else None),
                    )
                    self._check_quota(record)
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

    def cancel(self, idempotency_key: str) -> ExecutionRecord | None:
        """Withdraw a queued or running invocation; idempotent.

        Returns the record (None when the key is unknown).  A live
        execution is moved to the distinguishable CANCELLED terminal state:
        the decision is persisted first (so a restart never resumes it),
        then the in-process execution is signalled to stop cooperatively at
        the next step/retry boundary.  Cancelling an already-terminal
        execution is a no-op that returns the same stored conclusion, so
        repeated cancels observe one consistent outcome.
        """
        record = self.storage.load(idempotency_key)
        if record is None:
            return None
        if record.status in _TERMINAL:
            logger.info("[%s] cancel: already terminal (%s) -> no-op",
                        idempotency_key, record.status.value)
            return record
        self._transition(record, ExecutionStatus.CANCELLED,
                         error="cancelled by caller")
        with self._inflight_mu:
            event = self._cancel_events.get(idempotency_key)
        if event is not None:
            event.set()
        logger.info("[%s] cancel: withdrawal signalled", idempotency_key)
        return record

    def recover(self, wait: bool = True) -> list[ExecutionRecord]:
        """On startup: resume or explicitly reject interrupted executions.

        Returns the affected (resumed) records.  Records whose checkpoint
        is missing or version-incompatible, or whose function is
        unregistered, are moved to the terminal FAILED state with a
        distinguishable reason (never silently rerun from scratch or
        skipped).  Terminal records (including CANCELLED) are never
        resumed.  With ``wait=True`` (default) blocks until the resumed
        executions reach a terminal state.
        """
        affected: list[ExecutionRecord] = []
        futures: list[Future] = []
        records = self.storage.list_incomplete()
        logger.info("recovery scan: %d interrupted execution(s)", len(records))
        for record in records:
            if self._resume_or_reject(record):
                affected.append(record)
                fut = self._inflight.get(record.idempotency_key)
                if fut is not None:
                    futures.append(fut)
        if wait:
            for fut in futures:
                fut.result()
        return affected

    def shutdown(self) -> None:
        self._scheduler.shutdown(wait=True)

    # ------------------------------------------------------------------ internals

    def _fresh_record(self, record: ExecutionRecord, function_name: str,
                      payload: dict, now: float,
                      budget: float | None) -> ExecutionRecord:
        """Reset a transiently-rejected record for a brand-new invocation."""
        record.function_name = function_name
        record.payload = payload
        record.status = ExecutionStatus.QUEUED
        record.attempt = 0
        record.result = None
        record.error = None
        record.error_kind = None
        record.checkpoint_step = 0
        record.checkpoint_data = {}
        record.checkpoint_version = self.checkpoint_version
        record.retry_history = []
        record.created_at = now
        record.updated_at = now
        record.deadline = now + budget if budget else None
        return record

    def _check_quota(self, record: ExecutionRecord) -> None:
        """Reject a not-yet-accepted invocation when the quota is spent."""
        if self.invocation_quota is None:
            return
        if self.storage.count_accepted() >= self.invocation_quota:
            self._transition(
                record, ExecutionStatus.REJECTED,
                error=f"rejected:{RejectedReason.QUOTA_EXHAUSTED.value}",
                error_kind=ErrorKind.NON_RETRYABLE)
            logger.info("[%s] rejected: invocation quota exhausted",
                        record.idempotency_key)
            raise RejectedError(RejectedReason.QUOTA_EXHAUSTED)

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
        self._cancel_events[record.idempotency_key] = threading.Event()
        return future

    def _run_attempts(self, record: ExecutionRecord) -> None:
        key = record.idempotency_key
        try:
            while True:
                # Reload-backed check: catches a cancellation that landed
                # while this execution was not inflight in this process.
                self._raise_if_interrupted(record, reload=True)
                record.attempt += 1
                self._transition(record, ExecutionStatus.RUNNING)
                ctx = ExecutionContext(self, record)
                try:
                    result = self._functions[record.function_name](ctx, record.payload)
                    self._raise_if_interrupted(record)
                except ExecutionCancelled:
                    raise
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
        except ExecutionCancelled:
            self._transition(record, ExecutionStatus.CANCELLED,
                             error="cancelled by caller")
        except ExecutionTimedOut:
            self._transition(record, ExecutionStatus.TIMED_OUT,
                             error="execution deadline exceeded")
        finally:
            with self._inflight_mu:
                self._inflight.pop(key, None)
                self._cancel_events.pop(key, None)

    def _handle_failure(self, record: ExecutionRecord, exc: Exception) -> bool:
        """Persist the failure; return True iff another attempt may run."""
        if self._cancel_requested(record):
            raise ExecutionCancelled()
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
            if record.deadline is not None:
                # The backoff wait counts against the budget: never sleep
                # past the deadline, so the execution converges to
                # TIMED_OUT promptly once the budget is spent.
                delay = min(delay, max(0.0, record.deadline - self._clock()))
            record.retry_history.append(
                RetryRecord(attempt=record.attempt, error=str(exc),
                            delay_seconds=delay))
            self._transition(record, ExecutionStatus.FAILED_RETRYABLE,
                             error=str(exc), error_kind=ErrorKind.RETRYABLE)
            logger.info("[%s] retryable failure attempt=%d -> retry in %.3fs",
                        record.idempotency_key, record.attempt, delay)
            if self._sleep_unless_cancelled(record.idempotency_key, delay):
                raise ExecutionCancelled()
            return True
        self._transition(record, ExecutionStatus.FAILED,
                         error=f"retries exhausted: {exc}",
                         error_kind=ErrorKind.RETRYABLE)
        return False

    def _sleep_unless_cancelled(self, key: str, delay: float) -> bool:
        """Sleep for ``delay``; return True if cancelled mid-wait."""
        event = self._cancel_events.get(key)
        if event is not None:
            return event.wait(delay)
        self._sleeper(delay)
        return False

    def _cancel_requested(self, record: ExecutionRecord) -> bool:
        event = self._cancel_events.get(record.idempotency_key)
        return event is not None and event.is_set()

    def _raise_if_interrupted(self, record: ExecutionRecord,
                              reload: bool = False) -> None:
        """Cooperative stop points: cancellation first, then the deadline."""
        if self._cancel_requested(record):
            raise ExecutionCancelled()
        if reload:
            stored = self.storage.load(record.idempotency_key)
            if stored is not None and stored.status in _TERMINAL:
                raise ExecutionCancelled()
        if self._deadline_exceeded(record):
            raise ExecutionTimedOut()

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
        if record.status in _TERMINAL and record.status is not status:
            # Terminal states are conclusions: never silently move out of
            # one (e.g. a worker racing a persisted cancellation).
            logger.info("[%s] skip transition %s -> %s: already terminal",
                        record.idempotency_key, record.status.value,
                        status.value)
            return
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
    ExecutionStatus.CANCELLED,
}


class ResumeRejectedError(Exception):
    def __init__(self, reason: ResumeRejectedReason, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail
