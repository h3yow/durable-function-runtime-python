"""检查点构造、校验与恢复拒绝逻辑。

恢复必须满足三类前置检查，失败原因彼此可区分：

* 检查点文件不存在          -> ``checkpoint_missing``
* schema_version 与当前不兼容 -> ``checkpoint_version_incompatible``
* JSON 可解析但内容不一致/损坏 -> ``checkpoint_corrupt``

校验不通过时一律抛异常拒绝恢复，绝不静默从头重跑或跳过步骤。
"""

from __future__ import annotations

from typing import Any

from dfr.errors import (
    CheckpointCorruptError,
    CheckpointVersionError,
)
from dfr.models import SCHEMA_VERSION, TERMINAL_STATUSES, Status

_REQUIRED_FIELDS = (
    "idempotency_key", "function_name", "status", "next_step",
    "steps", "schema_version",
)


class CheckpointManager:
    """负责把执行记录视为检查点进行版本/完整性校验。"""

    def __init__(self, expected_version: int = SCHEMA_VERSION) -> None:
        self.expected_version = expected_version

    def validate(self, data: dict[str, Any]) -> None:
        """校验检查点；不合规时抛出带可区分 code 的异常。"""
        if not isinstance(data, dict):
            raise CheckpointCorruptError("检查点不是 JSON 对象")

        for field_name in _REQUIRED_FIELDS:
            if field_name not in data:
                raise CheckpointCorruptError(
                    f"检查点缺少必需字段: {field_name}"
                )

        version = data["schema_version"]
        if not isinstance(version, int) or isinstance(version, bool):
            raise CheckpointCorruptError("schema_version 不是整数")
        if version != self.expected_version:
            raise CheckpointVersionError(
                f"检查点版本 v{version} 与当前运行时 v{self.expected_version} "
                "不兼容，拒绝恢复"
            )

        try:
            status = Status(data["status"])
        except ValueError:
            raise CheckpointCorruptError(
                f"检查点状态非法: {data['status']!r}"
            ) from None

        steps = data["steps"]
        next_step = data["next_step"]
        if not isinstance(steps, list) or not isinstance(next_step, int):
            raise CheckpointCorruptError("steps/next_step 类型非法")
        if next_step != len(steps):
            raise CheckpointCorruptError(
                f"检查点不一致: next_step={next_step} 但已完成步骤数="
                f"{len(steps)}，可能丢步骤或重复计数，拒绝恢复"
            )
        for i, step in enumerate(steps):
            if not isinstance(step, dict) or step.get("index") != i:
                raise CheckpointCorruptError(
                    f"第 {i} 个步骤检查点缺失或序号错乱"
                )

        if status in TERMINAL_STATUSES:
            return  # 终态记录无需继续执行，仅校验结构

        if status not in (Status.PENDING, Status.RUNNING, Status.SUSPENDED):
            raise CheckpointCorruptError(f"不可恢复的中间状态: {status}")

    def resume_step(self, data: dict[str, Any]) -> int:
        """返回恢复后应从第几步继续（已完成步骤不得重复执行）。"""
        self.validate(data)
        return int(data["next_step"])
