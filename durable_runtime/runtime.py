"""Durable runtime: function registration, idempotent invocation, retry,
checkpointed recovery, concurrency control, time budgets, and cancellation.

Execution model (see docs/README for the full description):

* Each invocation is keyed by an idempotency key.  A duplicated request
  with the same key never re-executes; it observes the same conclusion.
  A rejection is the exception: it never consumed capacity, so the same
  key may be submitted again once capacity/quota allows.
* User functions run against an :class:`ExecutionContext`.  Every
  ``ctx.step(...)`` result is checkpointed before moving on; after a
  restart completed steps replay from the checkpoint and are not rerun.
* Retryable failures retry with exponential backoff up to a ceiling, then
  enter the queryable terminal FAILED state.  Non-retryable errors fail
  immediately.  Per-call time budgets (queueing + backoff + execution)
  lead to the TIMED_OUT terminal state; caller-initiated cancellation
  lands in the CANCELLED terminal state and is never resumed.
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
    StepTimedOutError,
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
    """Raised inside the worker when the caller has withdrawn the call."""


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

        ``timeout`` is this step's own time limit in seconds.  A step that
        exceeds it is *not* checkpointed and raises a retryable
        :class:`StepTimedOutError`, so the execution retries with backoff
        (still bounded by the overall time budget); already-checkpointed
        steps replay and are not rerun.  The effective limit is the
        smaller of ``timeout`` and the remaining overall budget.
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
        limit = self._effective_step_limit(self._record, timeout)
        logger.info("[%s] step#%d %s execute (limit=%s)",
                    self._record.idempotency_key, idx, name,
                    f"{limit:.3f}s" if limit is not None else "none")
        if limit is None:
            value = fn()
        else:
            value = self._run_step_bounded(name, fn, limit, timeout)
        if self._runtime._deadline_exceeded(self._record):
            # The step's side effect already happened, but the execution as
            # a whole is out of time: do not commit further progress.
            raise ExecutionTimedOut()
        self._record.checkpoint_data[key] = value
        self._record.checkpoint_step = idx
        self._runtime._persist(self._record)
        return value

    def _effective_step_limit(self, record: ExecutionRecord,
                              timeout: float | None) -> float | None:
        """Effective step limit: min(step timeout, remaining budget)."""
        limit = timeout
        if record.deadline is not None:
            remaining = record.deadline - self._runtime._clock()
            limit = remaining if limit is None else min(limit, remaining)
        return limit

    def _run_step_bounded(self, name: str, fn: Callable[[], object],
                          limit: float,
                          step_timeout: float | None) -> object:
        """Run ``fn`` on a helper thread and bound the wait at ``limit``.

        Python cannot kill a thread, so an over-running step keeps its
        orphaned helper thread, but its result is discarded and the step
        is never checkpointed: the execution itself converges (retry or
        TIMED_OUT) within the budget.
        """
        box: dict[str, object] = {}

        def target() -> None:
            try:
                box["value"] = fn()
            except BaseException as exc:  # noqa: BLE001 - re-raised below
                box["error"] = exc

        worker = threading.Thread(target=target, daemon=True)
        worker.start()
        wait_until = self._runtime._clock() + limit
        while worker.is_alive():
            if self._runtime._is_cancelled(self._record.idempotency_key):
                raise ExecutionCancelled()
            remaining = wait_until - self._runtime._clock()
            if remaining <= 0:
                break
            worker.join(min(0.05, remaining))
        if worker.is_alive():
            if self._runtime._deadline_exceeded(self._record):
                raise ExecutionTimedOut()
            assert step_timeout is not None
            raise StepTimedOutError(name, step_timeout)
        if "error" in box:
            raise box["error"]  # type: ignore[misc]
        return box.get("value")

    def cancelled(self) -> bool:
        """True once the call is withdrawn or out of budget.

        Long-running functions may poll this between steps to stop early;
        the runtime also enforces it at every checkpoint/transition.
        """
        return (self._runtime._is_cancelled(self._record.idempotency_key)
                or self._runtime._deadline_exceeded(self._record))


class DurableRuntime:
    """Coordinates the registry, storage, retries, and bounded scheduler."""

    def __init__(self, storage: Storage, retry_policy: RetryPolicy | None = None,
                 max_concurrency: int = 4, max_queue: int = 16,
                 execution_timeout: float | None = 30.0,
                 checkpoint_version: int = 1,
                 quotas: dict[str, int] | None = None,
                 sleeper: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.storage = storage
        self.retry_policy = retry_policy or RetryPolicy()
        self.execution_timeout = execution_timeout
        self.checkpoint_version = checkpoint_version
        # function name -> max admitted invocations; exceeding calls are
        # rejected with reason "quota_exceeded" (distinct from "queue_full")
        self._quotas = dict(quotas or {})
        self._sleeper = sleeper
        self._clock = clock
        self._functions: dict[str, UserFunction] = {}
        # idempotency key -> Future of its single in-process execution
        self._inflight: dict[str, Future] = {}
        self._inflight_mu = threading.Lock()
        # idempotency key -> set once the caller cancels that execution
        self._cancel_events: dict[str, threading.Event] = {}
        # Serializes "check cancel flag + persist" against cancel()'s
        # "set flag + persist CANCELLED", so a worker can never overwrite a
        # persisted CANCELLED state with a stale in-flight transition.
        self._persist_mu = threading.RLock()
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

        Concurrent duplicate calls share the single execution's future and
        get the same record.  A duplicate after completion returns the
        persisted conclusion without rerunning anything.

        ``timeout`` is the per-call time budget in seconds: queueing,
        backoff waits and execution all count against it.  When omitted,
        the runtime-wide ``execution_timeout`` default applies.  Once the
        budget is spent the execution converges to the queryable terminal
        TIMED_OUT state instead of lingering in "running".
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
                    if record.status is not ExecutionStatus.REJECTED:
                        logger.info("[%s] duplicate completed call -> return stored %s",
                                    idempotency_key, record.status.value)
                        return record
                    # A rejection is transient: that attempt was never
                    # admitted and never consumed a queue slot, so the same
                    # idempotency key stays usable and is admitted afresh
                    # once capacity/quota allows.
                    logger.info("[%s] prior %s is transient -> admitting anew",
                                idempotency_key, record.error)
                    record = None
                if record is None:
                    record = self._new_record(function_name, idempotency_key,
                                              payload, timeout, now)
                    self._enforce_quota(record)
                    self._persist(record)
                    logger.info("[%s] accepted new invocation of %s (budget=%s)",
                                idempotency_key, function_name,
                                f"{record.deadline - now:.3f}s"
                                if record.deadline else "none")
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
        """Withdraw a queued or running invocation.

        Idempotent: repeated calls return the same conclusion.  A call that
        already reached a terminal state keeps that conclusion (cancelling
        a succeeded call does not rewrite history).  A successful cancel
        lands in the terminal CANCELLED state — distinguishable from
        FAILED/TIMED_OUT/REJECTED — remaining steps never run, and the
        record is terminal so a process restart never resumes it.
        Returns None when the key is unknown.
        """
        with self._persist_mu:
            record = self.storage.load(idempotency_key)
            if record is None:
                return None
            if record.status in _TERMINAL:
                logger.info("[%s] cancel ignored: already terminal %s",
                            idempotency_key, record.status.value)
                return record
            # Set the flag before persisting; the worker observes it at the
            # next checkpoint/transition boundary (or while waiting).
            self._cancel_events.setdefault(
                idempotency_key, threading.Event()).set()
            self._transition(record, ExecutionStatus.CANCELLED,
                             error="cancelled by caller")
            logger.info("[%s] cancel accepted -> cancelled", idempotency_key)
            return record

    def recover(self, wait: bool = True) -> list[ExecutionRecord]:
        """On startup: resume or explicitly reject interrupted executions.

        Returns the affected (resumed) records.  Records whose checkpoint
        is missing or version-incompatible, or whose function is
        unregistered, are moved to the terminal FAILED state with a
        distinguishable reason (never silently rerun from scratch or
        skipped).  With ``wait=True`` (default) blocks until the resumed
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

    def _new_record(self, function_name: str, idempotency_key: str,
                    payload: dict, timeout: float | None,
                    now: float) -> ExecutionRecord:
        budget = timeout if timeout is not None else self.execution_timeout
        return ExecutionRecord(
            idempotency_key=idempotency_key,
            function_name=function_name,
            payload=payload,
            status=ExecutionStatus.QUEUED,
            checkpoint_version=self.checkpoint_version,
            created_at=now,
            updated_at=now,
            deadline=(now + budget if budget else None),
        )

    def _enforce_quota(self, record: ExecutionRecord) -> None:
        """Reject when the function's admission quota is used up.

        Distinct from capacity (queue_full): quota counts admitted
        invocations per function.  The rejected record is persisted as a
        queryable terminal state but stays transient — resubmitting the
        same key retries admission.
        """
        quota = self._quotas.get(record.function_name)
        if quota is None:
            return
        admitted = self.storage.count_accepted(record.function_name)
        if admitted < quota:
            return
        self._transition(
            record, ExecutionStatus.REJECTED,
            error=f"rejected:{RejectedReason.QUOTA_EXCEEDED.value}",
            error_kind=ErrorKind.NON_RETRYABLE)
        logger.info("[%s] quota for %s exhausted (%d/%d) -> rejected",
                    record.idempotency_key, record.function_name,
                    admitted, quota)
        raise RejectedError(RejectedReason.QUOTA_EXCEEDED)

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
                    if self._is_cancelled(key):
                        raise ExecutionCancelled()
                    if self._deadline_exceeded(record):
                        raise ExecutionTimedOut()
                except ExecutionCancelled:
                    self._transition(record, ExecutionStatus.CANCELLED,
                                     error="cancelled by caller")
                    return
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
            # Cancellation landed between transitions (e.g. while queued or
            # sleeping between retries); converge to the terminal state.
            self._transition(record, ExecutionStatus.CANCELLED,
                             error="cancelled by caller")
        finally:
            with self._inflight_mu:
                if self._inflight.get(key) is not None:
                    del self._inflight[key]
            with self._persist_mu:
                self._cancel_events.pop(key, None)

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
            # The backoff wait itself is bounded by the remaining budget so
            # the execution converges to TIMED_OUT promptly once the budget
            # is spent, instead of sleeping past the deadline.
            if record.deadline is not None:
                remaining = record.deadline - self._clock()
                delay = max(0.0, min(delay, remaining))
            logger.info("[%s] retryable failure attempt=%d -> retry in %.3fs",
                        record.idempotency_key, record.attempt, delay)
            self._sleep_cancellable(delay, record)
            if self._is_cancelled(record.idempotency_key):
                raise ExecutionCancelled()
            if self._deadline_exceeded(record):
                self._transition(record, ExecutionStatus.TIMED_OUT,
                                 error="execution deadline exceeded")
                return False
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

    def _is_cancelled(self, idempotency_key: str) -> bool:
        event = self._cancel_events.get(idempotency_key)
        return event is not None and event.is_set()

    def _sleep_cancellable(self, delay: float, record: ExecutionRecord) -> None:
        """Sleep in short slices so cancellation interrupts the backoff wait."""
        end = self._clock() + delay
        while not self._is_cancelled(record.idempotency_key):
            remaining = end - self._clock()
            if remaining <= 0:
                return
            self._sleeper(min(0.05, remaining))

    def _persist(self, record: ExecutionRecord) -> None:
        with self._persist_mu:
            if self._is_cancelled(record.idempotency_key):
                raise ExecutionCancelled()
            record.updated_at = self._clock()
            self.storage.save(record)

    def _transition(self, record: ExecutionRecord,
                    status: ExecutionStatus, error: str | None = None,
                    error_kind: ErrorKind | None = None) -> None:
        with self._persist_mu:
            if (status != ExecutionStatus.CANCELLED
                    and self._is_cancelled(record.idempotency_key)):
                # A cancel landed first: refuse to overwrite the persisted
                # CANCELLED state with a stale in-flight transition.
                raise ExecutionCancelled()
            old = record.status.value
            record.status = status
            record.updated_at = self._clock()
            if error is not None:
                record.error = error
            if error_kind is not None:
                record.error_kind = error_kind
            self.storage.save(record)
            logger.info("[%s] transition %s -> %s (attempt=%d)",
                        record.idempotency_key, old, status.value,
                        record.attempt)


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
