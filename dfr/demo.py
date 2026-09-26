"""内置演示函数，便于本地手动验证幂等、重试与恢复。

步骤通过读取可变状态来模拟“临时故障后成功”：
* ``flaky_step``：前 ``fail_times`` 次调用抛可重试错误，之后成功。
* ``always_fail_step``：始终抛可重试错误（用于观察重试耗尽）。
* ``fatal_step``：抛不可重试错误。
* ``slow_step``：睡眠固定时长（用于超时/排队观察）。

副作用（外部系统的写入）通过进程内/跨文件的计数器模拟，
``side_effect_step`` 每次执行都会写一行到 side_effects.log，
重复执行会留下痕迹——测试据此断言“步骤未被重复执行”。
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

from dfr.errors import NonRetryableError, RetryableError
from dfr.registry import FunctionRegistry, StepContext

_STATE_LOCK = threading.Lock()
_FLAKY: dict[str, int] = {}


def reset_demo_state() -> None:
    """测试辅助：清空内存中的故障计数。"""
    with _STATE_LOCK:
        _FLAKY.clear()


def make_flaky_step(fail_times: int = 2, marker: str = "flaky"):
    """生成一个前 fail_times 次失败（可重试）之后成功的步骤。"""

    def step(ctx: StepContext):
        with _STATE_LOCK:
            count = _FLAKY.get(marker, 0) + 1
            _FLAKY[marker] = count
        if count <= fail_times:
            raise RetryableError(
                f"{marker}: 临时故障 ({count}/{fail_times})"
            )
        return f"{marker}:ok@{count}"

    step.__name__ = f"flaky_{marker}"
    return step


def make_side_effect_step(log_path: str | Path, label: str = "effect"):
    """生成一个带外部副作用的步骤：向日志追加一行并返回当前计数。"""

    log_path = Path(log_path)

    def step(ctx: StepContext):
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(log_path, "a", encoding="utf-8") as fh:
            line = f"{label}\n"
            fh.write(line)
        return label

    step.__name__ = f"side_effect_{label}"
    return step


def echo_step(ctx: StepContext):
    """直接返回调用入参。"""
    return ctx.payload


def append_step(ctx: StepContext):
    """在上一步结果（或入参字符串）后追加标记。"""
    base = ctx.previous if ctx.previous is not None else ctx.payload
    return f"{base}->done"


def always_retryable_fail_step(ctx: StepContext):
    raise RetryableError("永久可重试故障，用于观察重试耗尽")


def non_retryable_fail_step(ctx: StepContext):
    raise NonRetryableError("不可重试的确定性失败")


def make_slow_step(seconds: float = 0.5):
    def step(ctx: StepContext):
        time.sleep(seconds)
        return f"slept_{seconds}s"

    step.__name__ = f"slow_{seconds}"
    return step


def build_demo_registry(log_dir: str | Path = "data") -> FunctionRegistry:
    """注册一组演示函数。"""
    log_dir = Path(log_dir)
    registry = FunctionRegistry()
    registry.define("echo", [echo_step])
    registry.define("echo_then_append", [echo_step, append_step])
    registry.define("flaky_then_append", [
        make_flaky_step(2, "demo_flaky"), append_step,
    ])
    registry.define("side_effect", [
        make_side_effect_step(log_dir / "side_effects.log", "effect1"),
    ])
    registry.define("always_failing", [always_retryable_fail_step])
    registry.define("fatal", [non_retryable_fail_step])
    registry.define("slow", [make_slow_step(0.5)])
    return registry
