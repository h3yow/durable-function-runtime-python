"""执行引擎：幂等调用、重试退避、并发与背压、断点恢复。

关键不变量
----------
* 同一幂等键在本进程内由一把键锁串行化：绝不会并发执行同一份执行。
* 每个步骤成功后立即原子持久化检查点；恢复时从 ``next_step`` 继续，
  已完成步骤不会被再次执行。
* 重试只发生在可重试错误上，退避时长写入 ``retries`` 记录；重试耗尽进入
  FAILED 终态；超过整体截止时间进入 TIMED_OUT 终态。终态可查询、不再执行。
* 容量受 ``max_concurrency`` 个执行槽与 ``max_queue`` 个排队槽限制；
  两者都不可用时以 ``concurrency_limit_rejected`` 有界拒绝；配额耗尽以
  ``quota_exceeded`` 拒绝。排队不会丢失或覆盖任何已接受调用的状态。
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dfr.backoff import RetryPolicy
from dfr.checkpoint import CheckpointManager
from dfr.errors import (
    CheckpointMissingError,
    ConcurrencyLimitRejectedError,
    NonRetryableError,
    QuotaExceededError,
    RetryableError,
    StepTimeoutError,
)
from dfr.models import (
    ExecutionRecord,
    RetryRecord,
    Status,
    StepResult,
    utcnow_iso,
)
from dfr.registry import FunctionRegistry, StepContext
from dfr.storage import JsonStore

logger = logging.getLogger("dfr.runtime")


@dataclass
class RuntimeConfig:
    """运行时可调参数（并发上限、有界队列、配额、超时）。"""

    max_concurrency: int = 4
    max_queue: int = 8
    max_calls: int | None = None  # 全局接受配额，None 表示不限
    default_step_timeout: float | None = None
    default_deadline: float | None = None


@dataclass
class RecoveryOutcome:
    """一次恢复尝试的结果，原因可区分。"""

    idempotency_key: str
    recovered: bool
    reason: str
    record: ExecutionRecord | None = None
    error_code: str | None = None


class InvocationResult:
    """invoke 的返回：执行记录 + 容量处置信息。"""

    def __init__(self, record: ExecutionRecord, *, accepted: bool,
                 replayed: bool = False, queued: bool = False,
                 reason: str | None = None) -> None:
        self.record = record
        self.accepted = accepted
        self.replayed = replayed
        self.queued = queued
        self.reason = reason

    def __repr__(self) -> str:  # pragma: no cover - 调试辅助
        return (f"InvocationResult(key={self.record.idempotency_key!r}, "
                f"status={self.record.status}, accepted={self.accepted}, "
                f"replayed={self.replayed}, queued={self.queued})")


class DurableRuntime:
    """对外核心门面：invoke / get / recover_one / recover_all / shutdown。"""

    def __init__(
        self,
        directory: str | Path,
        registry: FunctionRegistry,
        config: RuntimeConfig | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        self.directory = Path(directory)
        self.registry = registry
        self.config = config or RuntimeConfig()
        self.retry_policy = retry_policy or RetryPolicy()
        self.store = JsonStore(self.directory)
        self.checkpoints = CheckpointManager()

        # 每个幂等键一把锁，保证同键不被并发重复执行
        self._key_locks: dict[str, threading.Lock] = {}
        self._key_locks_guard = threading.Lock()

        # 两级容量信号量：执行槽 / 总容量（执行 + 排队）
        self._active_slots = threading.BoundedSemaphore(
            self.config.max_concurrency
        )
        self._capacity = threading.BoundedSemaphore(
            self.config.max_concurrency + self.config.max_queue
        )

        self._accepted_total = 0
        self._quota_lock = threading.Lock()
        self._executor = ThreadPoolExecutor(
            max_workers=self.config.max_concurrency,
            thread_name_prefix="dfr-exec",
        )
        self._stop = False

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------
    def _key_lock(self, key: str) -> threading.Lock:
        with self._key_locks_guard:
            lock = self._key_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._key_locks[key] = lock
            return lock

    def _persist(self, record: ExecutionRecord) -> None:
        self.store.write(record.idempotency_key, record.to_dict())

    @staticmethod
    def _log_record(record: ExecutionRecord, event: str, **extra: Any) -> None:
        suffix = " ".join(f"{k}={v}" for k, v in extra.items())
        logger.info(
            "invoke=%s event=%s status=%s attempt=%d next_step=%d %s",
            record.idempotency_key, event, record.status,
            record.attempts, record.next_step, suffix,
        )

    # ------------------------------------------------------------------
    # 容量与配额
    # ------------------------------------------------------------------
    def _try_acquire_capacity(self) -> str:
        """获取容量：返回 "active"（拿到执行槽）或 "queued"（仅排队槽）。"""
        if not self._capacity.acquire(blocking=False):
            raise ConcurrencyLimitRejectedError(
                f"已达容量上限: {self.config.max_concurrency} 个执行 + "
                f"{self.config.max_queue} 个排队均满"
            )
        if self._active_slots.acquire(blocking=False):
            return "active"
        return "queued"

    def _release_capacity(self, mode: str) -> None:
        if mode == "active":
            self._active_slots.release()
        self._capacity.release()

    def _consume_quota(self) -> None:
        if self.config.max_calls is None:
            return
        with self._quota_lock:
            if self._accepted_total >= self.config.max_calls:
                raise QuotaExceededError(
                    f"调用配额已耗尽: max_calls={self.config.max_calls}"
                )
            self._accepted_total += 1

    def _refund_quota(self) -> None:
        if self.config.max_calls is None:
            return
        with self._quota_lock:
            self._accepted_total -= 1

    # ------------------------------------------------------------------
    # 对外调用入口
    # ------------------------------------------------------------------
    def invoke(
        self,
        function_name: str,
        idempotency_key: str,
        payload: Any = None,
        *,
        timeout: float | None = None,
        deadline: float | None = None,
        wait: bool = True,
        suspend_before_step: int | None = None,
    ) -> InvocationResult:
        """提交一次幂等调用。

        wait=True  阻塞到终态（或挂起）后返回；
        wait=False 容量满时立即以可区分原因拒绝，否则后台执行并立即返回。
        """
        # 未注册函数属于调用方错误，在任何持久化之前拒绝
        self.registry.get(function_name)

        lock = self._key_lock(idempotency_key)
        lock.acquire()
        transferred = False
        try:
            existing = self._load_existing(idempotency_key)
            if existing is not None:
                self._log_record(existing, "duplicate_invocation")
                return InvocationResult(
                    existing, accepted=False, replayed=True,
                    reason="idempotent_replay",
                )

            self._consume_quota()
            try:
                mode = self._try_acquire_capacity()
            except BaseException:
                self._refund_quota()
                raise

            record = ExecutionRecord.new(
                idempotency_key, function_name, payload
            )
            step_timeout = (
                timeout if timeout is not None
                else self.config.default_step_timeout
            )
            eff_deadline = (
                deadline if deadline is not None
                else self.config.default_deadline
            )
            record.deadline_epoch = (
                time.monotonic() + eff_deadline if eff_deadline else None
            )
            self._persist(record)
            self._log_record(record, "accepted", capacity=mode)

            if mode == "active" and wait:
                # 同步路径：当前线程持键锁执行，并发同键调用只能走重放分支
                self._run_execution(record, step_timeout, suspend_before_step)
                self._release_capacity("active")
                return InvocationResult(record, accepted=True)

            # 异步路径（排队 或 wait=False）：键锁所有权移交后台线程
            transferred = True
            self._executor.submit(
                self._worker, record, step_timeout, mode,
                suspend_before_step, lock,
            )
            if mode == "queued":
                self._log_record(record, "queued")
                return InvocationResult(
                    record, accepted=True, queued=True, reason="queued"
                )
            return InvocationResult(
                record, accepted=True, queued=False, reason="accepted_async"
            )
        finally:
            if not transferred:
                lock.release()

    def _load_existing(self, key: str) -> ExecutionRecord | None:
        """读取已有检查点（损坏/版本不符会作为可区分错误抛出）。"""
        if not self.store.exists(key):
            return None
        data = self.store.read(key)
        self.checkpoints.validate(data)
        return ExecutionRecord.from_dict(data)

    # ------------------------------------------------------------------
    # 后台 worker（异步/排队路径）
    # ------------------------------------------------------------------
    def _worker(self, record: ExecutionRecord, step_timeout: float | None,
                mode: str, suspend_before_step: int | None,
                lock: threading.Lock) -> None:
        try:
            if mode == "queued":
                self._log_record(record, "queue_wait_start")
                self._active_slots.acquire()  # 有界等待，不会拒绝已接受调用
                mode = "active"
                self._log_record(record, "queue_wait_end")
            self._run_execution(record, step_timeout, suspend_before_step)
        except Exception:  # pragma: no cover - 兜底，避免线程静默死亡
            logger.exception("invoke=%s 后台执行异常", record.idempotency_key)
        finally:
            self._release_capacity(mode)
            lock.release()

    # ------------------------------------------------------------------
    # 执行核心
    # ------------------------------------------------------------------
    def _run_execution(self, record: ExecutionRecord,
                       step_timeout: float | None,
                       suspend_before_step: int | None) -> None:
        """在键锁保护下，从 next_step 继续执行直到终态或挂起。"""
        steps = self.registry.get(record.function_name).steps()

        if record.status == Status.PENDING.value:
            record.transition(Status.RUNNING, "execution_started")
            self._persist(record)
        elif record.status == Status.SUSPENDED.value:
            record.transition(Status.RUNNING, "execution_resumed")
            self._persist(record)
        self._log_record(record, "execution_enter")

        while record.next_step < len(steps):
            # 整体截止时间检查（重启恢复后依旧有效）
            if (record.deadline_epoch is not None
                    and time.monotonic() > record.deadline_epoch):
                self._finalize_timeout(
                    record, "超过整体截止时间 (deadline)"
                )
                return

            index = record.next_step
            if suspend_before_step is not None and index == suspend_before_step:
                record.transition(
                    Status.SUSPENDED, f"manual_suspend_before_step_{index}"
                )
                self._persist(record)
                self._log_record(record, "suspended")
                return

            step = steps[index]
            record.attempts += 1
            ctx = StepContext(
                payload=record.payload,
                previous=record.steps[-1].value if record.steps else None,
            )

            try:
                value = self._run_step_with_timeout(step, ctx, step_timeout)
            except Exception as exc:  # noqa: BLE001 - 需要分类全部异常
                # 执行尝试期间可能已越过整体截止时间：优先判定超时终态
                if (record.deadline_epoch is not None
                        and time.monotonic() > record.deadline_epoch):
                    self._finalize_timeout(
                        record, "执行步骤期间超过整体截止时间"
                    )
                    return
                retryable = isinstance(exc, RetryableError)
                error_code = getattr(exc, "code", exc.__class__.__name__)
                should_retry = self.retry_policy.should_retry(
                    record.attempts, retryable=retryable
                )
                backoff = (
                    self.retry_policy.delay_for(record.attempts)
                    if should_retry else 0.0
                )
                record.retries.append(RetryRecord(
                    attempt=record.attempts,
                    step_index=index,
                    step_name=getattr(step, "__name__", f"step_{index}"),
                    error_code=error_code,
                    error_message=str(exc),
                    backoff_seconds=backoff,
                    timestamp=utcnow_iso(),
                ))
                self._log_record(
                    record, "step_failed", error=error_code,
                    retry=should_retry, backoff=backoff,
                )
                self._persist(record)

                if not retryable:
                    record.error_code = error_code
                    record.error_message = str(exc)
                    record.transition(
                        Status.FAILED, f"non_retryable:{error_code}"
                    )
                    self._persist(record)
                    self._log_record(record, "terminal_failed")
                    return
                if not should_retry:
                    record.error_code = error_code
                    record.error_message = str(exc)
                    record.transition(
                        Status.FAILED,
                        f"retries_exhausted_after_{record.attempts}_attempts",
                    )
                    self._persist(record)
                    self._log_record(record, "terminal_failed_retries")
                    return

                if (record.deadline_epoch is not None
                        and time.monotonic() + backoff
                        > record.deadline_epoch):
                    self._finalize_timeout(
                        record, "重试等待将超过整体截止时间"
                    )
                    return
                time.sleep(backoff)
                continue  # next_step 未推进：该步骤将被重试

            # 步骤成功返回：若执行期间已越过整体截止时间，不采纳结果
            if (record.deadline_epoch is not None
                    and time.monotonic() > record.deadline_epoch):
                self._finalize_timeout(
                    record, "步骤完成时已超过整体截止时间"
                )
                return

            # 步骤成功：推进检查点（已完成步骤绝不重复）
            record.steps.append(
                StepResult(index=index,
                           name=getattr(step, "__name__", f"step_{index}"),
                           value=value)
            )
            record.next_step += 1
            self._persist(record)
            self._log_record(record, "step_checkpoint", value=value)

        record.result = record.steps[-1].value if record.steps else None
        record.error_code = None
        record.error_message = None
        record.transition(Status.SUCCEEDED, "all_steps_completed")
        self._persist(record)
        self._log_record(record, "terminal_succeeded", result=record.result)

    @staticmethod
    def _run_step_with_timeout(step, ctx: StepContext,
                               timeout: float | None) -> Any:
        """在独立线程中执行单步；超时抛出 StepTimeoutError（可重试）。"""
        if timeout is None:
            return step(ctx)

        box: dict[str, Any] = {}

        def _target() -> None:
            try:
                box["value"] = step(ctx)
            except BaseException as exc:  # noqa: BLE001
                box["error"] = exc

        worker = threading.Thread(target=_target, daemon=True)
        worker.start()
        worker.join(timeout)
        if worker.is_alive():
            raise StepTimeoutError(f"步骤执行超过 {timeout}s")
        if "error" in box:
            raise box["error"]
        return box.get("value")

    def _finalize_timeout(self, record: ExecutionRecord, reason: str) -> None:
        record.error_code = "execution_timed_out"
        record.error_message = reason
        record.transition(Status.TIMED_OUT, reason)
        self._persist(record)
        self._log_record(record, "terminal_timed_out")

    # ------------------------------------------------------------------
    # 查询与恢复
    # ------------------------------------------------------------------
    def get(self, idempotency_key: str) -> ExecutionRecord | None:
        """按幂等键查询执行记录；终态记录原样返回，不重新执行。"""
        return self._load_existing(idempotency_key)

    def recover_all(self) -> list[RecoveryOutcome]:
        """进程重启后恢复全部未完成执行。

        检查点损坏/版本不符会以对应 error_code 记录在结果中并拒绝该条恢复，
        不影响其他执行；恢复按序在当前线程完成（确定性、可测试）。
        """
        outcomes: list[RecoveryOutcome] = []
        for key, item in self.store.iter_keyed_records():
            if isinstance(item, Exception):
                logger.error("invoke=%s 恢复被拒绝 reason=%s",
                             key, item.code)
                outcomes.append(RecoveryOutcome(
                    key, False, f"rejected:{item.code}",
                    error_code=item.code,
                ))
                continue
            data = item
            try:
                self.checkpoints.validate(data)
            except Exception as exc:  # noqa: BLE001
                code = getattr(exc, "code", "checkpoint_corrupt")
                logger.error("invoke=%s 恢复被拒绝 reason=%s", key, code)
                outcomes.append(RecoveryOutcome(
                    key, False, f"rejected:{code}", error_code=code,
                ))
                continue

            record = ExecutionRecord.from_dict(data)
            if record.status in (s.value for s in
                                 (Status.SUCCEEDED, Status.FAILED,
                                  Status.TIMED_OUT)):
                outcomes.append(RecoveryOutcome(
                    key, False, f"already_{record.status.lower()}",
                    record=record,
                ))
                continue

            self._consume_quota()
            try:
                lock = self._key_lock(key)
                acquired = lock.acquire(blocking=False)
                if not acquired:
                    outcomes.append(RecoveryOutcome(
                        key, False, "rejected:in_progress",
                        record=record, error_code="execution_in_progress",
                    ))
                    self._refund_quota()
                    continue
                try:
                    mode = self._try_acquire_capacity()
                    try:
                        logger.info(
                            "invoke=%s event=recover_start status=%s next_step=%d",
                            key, record.status, record.next_step,
                        )
                        self._run_execution(
                            record, self.config.default_step_timeout, None
                        )
                        outcomes.append(RecoveryOutcome(
                            key, True, f"recovered:{record.status.lower()}",
                            record=record,
                        ))
                    finally:
                        self._release_capacity(mode)
                finally:
                    lock.release()
            except (ConcurrencyLimitRejectedError, QuotaExceededError) as exc:
                self._refund_quota()
                outcomes.append(RecoveryOutcome(
                    key, False, f"rejected:{exc.code}",
                    record=record, error_code=exc.code,
                ))
        return outcomes

    def recover_one(self, idempotency_key: str) -> RecoveryOutcome:
        """显式恢复单个执行；检查点缺失时以 checkpoint_missing 拒绝。"""
        if not self.store.exists(idempotency_key):
            raise CheckpointMissingError(
                f"找不到执行 {idempotency_key} 的检查点，拒绝恢复（不会"
                "从头重跑）"
            )
        outcomes = [
            o for o in self.recover_all()
            if o.idempotency_key == idempotency_key
        ]
        return outcomes[0]

    # ------------------------------------------------------------------
    def shutdown(self) -> None:
        """停止接收新任务并等待已接受的后台执行结束（不丢失）。"""
        self._stop = True
        self._executor.shutdown(wait=True)
