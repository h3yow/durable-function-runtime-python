"""测试夹具：临时数据目录、可观测步骤、独立运行时。

测试默认把 dfr.runtime 的日志输出到 caplog/终端，日志中包含：
调用标识 (invoke=<key>)、状态迁移 (event=.../transitions)、重试记录。
"""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from dfr.backoff import RetryPolicy
from dfr.demo import reset_demo_state
from dfr.errors import NonRetryableError, RetryableError
from dfr.registry import FunctionRegistry, StepContext
from dfr.runtime import DurableRuntime, RuntimeConfig


@pytest.fixture(autouse=True)
def _reset_demo():
    reset_demo_state()
    yield
    reset_demo_state()


@pytest.fixture()
def tmp_data_dir(tmp_path) -> Path:
    data = tmp_path / "data"
    data.mkdir()
    return data


@pytest.fixture()
def registry() -> FunctionRegistry:
    reg = FunctionRegistry()
    return reg


@pytest.fixture()
def runtime_factory(tmp_data_dir, registry):
    """生成绑定临时目录的运行时工厂；每个 runtime 在用例结束时关闭。"""
    runtimes: list[DurableRuntime] = []

    def _make(config: RuntimeConfig | None = None,
              retry: RetryPolicy | None = None) -> DurableRuntime:
        rt = DurableRuntime(tmp_data_dir, registry,
                            config or RuntimeConfig(),
                            retry or RetryPolicy())
        runtimes.append(rt)
        return rt

    yield _make

    for rt in runtimes:
        rt.shutdown()


@pytest.fixture()
def observing_steps():
    """构造可观测步骤：记录进入次数、可挂起事件、可注入故障。

    返回 (make_steps, state)：
      state.calls[step_name] 为步骤真实进入次数（副作用计数）；
      state.events 可用于阻塞/放行步骤。
    """

    class State:
        def __init__(self) -> None:
            self.calls: dict[str, int] = {}
            self.lock = threading.Lock()
            self.gates: dict[str, threading.Event] = {}
            self.fail_remaining: dict[str, int] = {}
            self.retryable: dict[str, bool] = {}

        def gate(self, name: str) -> threading.Event:
            ev = self.gates.get(name)
            if ev is None:
                ev = threading.Event()
                ev.set()  # 默认放行
                self.gates[name] = ev
            return ev

    state = State()

    def make_step(name: str, *, retryable: bool = True,
                  fail_times: int = 0):
        state.fail_remaining[name] = fail_times
        state.retryable[name] = retryable

        def step(ctx: StepContext):
            state.gate(name).wait(5)
            with state.lock:
                state.calls[name] = state.calls.get(name, 0) + 1
                n = state.calls[name]
            remaining = state.fail_remaining[name]
            if remaining > 0:
                state.fail_remaining[name] = remaining - 1
                if state.retryable[name]:
                    raise RetryableError(f"{name} 临时故障 #{n}")
                raise NonRetryableError(f"{name} 永久故障 #{n}")
            value = ctx.previous if ctx.previous is not None else ctx.payload
            return f"{name}({value})"

        step.__name__ = name
        return step

    def make_steps(*names: str, fail: dict[str, int] | None = None,
                   non_retryable: set[str] | None = None):
        fail = fail or {}
        non_retryable = non_retryable or set()
        return [
            make_step(n, retryable=n not in non_retryable,
                      fail_times=fail.get(n, 0))
            for n in names
        ]

    return make_steps, state
