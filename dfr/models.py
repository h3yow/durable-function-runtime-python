"""执行状态枚举与持久化记录的数据模型。"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any

#: 持久化记录格式版本；不兼容时恢复必须拒绝。
SCHEMA_VERSION = 1


def utcnow_iso() -> str:
    """UTC ISO-8601 时间戳（秒级，本地持久化用途）。"""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Status(str, Enum):
    """执行的生命周期状态。

    PENDING  -> RUNNING
    RUNNING  -> SUCCEEDED / FAILED / TIMED_OUT / SUSPENDED
    SUSPENDED-> RUNNING (恢复)
    SUCCEEDED / FAILED / TIMED_OUT 均为终态。
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUSPENDED = "SUSPENDED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"


TERMINAL_STATUSES = frozenset(
    {Status.SUCCEEDED, Status.FAILED, Status.TIMED_OUT}
)


@dataclass
class RetryRecord:
    """单次重试/失败尝试的可解释记录。"""

    attempt: int
    step_index: int
    step_name: str
    error_code: str
    error_message: str
    backoff_seconds: float
    timestamp: str


@dataclass
class Transition:
    """状态迁移记录（用于可解释日志与查询）。"""

    from_status: str
    to_status: str
    reason: str
    timestamp: str


@dataclass
class StepResult:
    """已完成步骤的检查点载荷。"""

    index: int
    name: str
    value: Any = None


@dataclass
class ExecutionRecord:
    """一次幂等调用的完整持久化记录。"""

    idempotency_key: str
    function_name: str
    payload: Any = None
    status: str = Status.PENDING.value
    next_step: int = 0
    steps: list[StepResult] = field(default_factory=list)
    retries: list[RetryRecord] = field(default_factory=list)
    transitions: list[Transition] = field(default_factory=list)
    result: Any = None
    error_code: str | None = None
    error_message: str | None = None
    attempts: int = 0
    created_at: str = ""
    updated_at: str = ""
    deadline_epoch: float | None = None
    schema_version: int = SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def touch(self) -> None:
        self.updated_at = utcnow_iso()

    def transition(self, to_status: Status, reason: str) -> None:
        """记录一次状态迁移并更新状态与时间戳。"""
        self.transitions.append(
            Transition(
                from_status=self.status,
                to_status=to_status.value,
                reason=reason,
                timestamp=utcnow_iso(),
            )
        )
        self.status = to_status.value
        self.touch()

    @classmethod
    def new(cls, idempotency_key: str, function_name: str,
            payload: Any = None) -> ExecutionRecord:
        now = utcnow_iso()
        record = cls(
            idempotency_key=idempotency_key,
            function_name=function_name,
            payload=payload,
            created_at=now,
            updated_at=now,
        )
        # 初始状态即为 PENDING，直接登记接收事件，避免自迁移
        record.transitions.append(
            Transition(
                from_status=Status.PENDING.value,
                to_status=Status.PENDING.value,
                reason="accepted",
                timestamp=now,
            )
        )
        return record

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ExecutionRecord:
        steps = [StepResult(**s) for s in data.get("steps", [])]
        retries = [RetryRecord(**r) for r in data.get("retries", [])]
        transitions = [Transition(**t) for t in data.get("transitions", [])]
        known = {
            "idempotency_key", "function_name", "payload", "status",
            "next_step", "steps", "retries", "transitions", "result",
            "error_code", "error_message", "attempts",
            "created_at", "updated_at", "deadline_epoch", "schema_version",
        }
        unknown = set(data) - known
        if unknown:
            raise TypeError(f"execution record 含未知字段: {sorted(unknown)}")
        return cls(
            idempotency_key=data["idempotency_key"],
            function_name=data["function_name"],
            payload=data.get("payload"),
            status=data.get("status", Status.PENDING.value),
            next_step=data.get("next_step", 0),
            steps=steps,
            retries=retries,
            transitions=transitions,
            result=data.get("result"),
            error_code=data.get("error_code"),
            error_message=data.get("error_message"),
            attempts=data.get("attempts", 0),
            created_at=data.get("created_at", ""),
            updated_at=data.get("updated_at", ""),
            deadline_epoch=data.get("deadline_epoch"),
            schema_version=data.get("schema_version", SCHEMA_VERSION),
        )
