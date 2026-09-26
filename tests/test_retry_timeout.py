"""可重试/不可重试错误区分、指数退避、重试耗尽终态、步骤超时与整体超时。"""

from __future__ import annotations

import threading
import time

from dfr.backoff import RetryPolicy
from dfr.models import Status
from dfr.runtime import DurableRuntime, RuntimeConfig


def test_retryable_error_retries_with_backoff_then_succeeds(
        runtime_factory, registry, observing_steps):
    make_steps, state = observing_steps
    registry.define("flaky", make_steps("s1", "s2", fail={"s1": 2}))
    policy = RetryPolicy(max_attempts=4, base_delay=0.01,
                         multiplier=2.0, max_delay=0.05)
    rt = runtime_factory(RuntimeConfig(), policy)

    start = time.monotonic()
    r = rt.invoke("flaky", "retry-1", wait=True)
    elapsed = time.monotonic() - start

    rec = r.record
    assert rec.status == Status.SUCCEEDED.value
    assert state.calls["s1"] == 3 and state.calls["s2"] == 1
    # 前两次失败、第三次成功：两条重试记录，退避 0.01 / 0.02
    assert [x.attempt for x in rec.retries] == [1, 2]
    assert [x.error_code for x in rec.retries] == [
        "retryable_error", "retryable_error"]
    assert rec.retries[0].backoff_seconds == 0.01
    assert rec.retries[1].backoff_seconds == 0.02
    assert elapsed >= 0.03  # 退避确实被执行
    print("\n[重试] retry-1 重试记录:")
    for x in rec.retries:
        print(f"  attempt={x.attempt} step={x.step_name} "
              f"code={x.error_code} backoff={x.backoff_seconds}s")


def test_retries_exhausted_enters_failed_terminal(
        runtime_factory, registry, observing_steps):
    make_steps, state = observing_steps
    registry.define("doomed", make_steps("s1", fail={"s1": 99}))
    rt = runtime_factory(
        RuntimeConfig(), RetryPolicy(max_attempts=3, base_delay=0.001))

    r = rt.invoke("doomed", "retry-2", wait=True)
    rec = r.record
    assert rec.status == Status.FAILED.value
    assert rec.error_code == "retryable_error"
    assert state.calls["s1"] == 3
    assert len(rec.retries) == 3
    last_transition = rec.transitions[-1]
    assert last_transition.to_status == Status.FAILED.value
    assert "retries_exhausted" in last_transition.reason
    print("[重试耗尽] retry-2 终态:", rec.status,
          "尝试次数:", rec.attempts,
          "迁移:", last_transition.reason)

    # 终态可查询且不会再执行
    again = rt.invoke("doomed", "retry-2", wait=True)
    assert again.replayed is True
    assert state.calls["s1"] == 3


def test_non_retryable_error_fails_immediately(
        runtime_factory, registry, observing_steps):
    make_steps, state = observing_steps
    registry.define("fatal", make_steps(
        "s1", non_retryable={"s1"}, fail={"s1": 1}))
    rt = runtime_factory(
        RuntimeConfig(), RetryPolicy(max_attempts=5, base_delay=0.01))
    r = rt.invoke("fatal", "retry-3", wait=True)
    rec = r.record
    assert rec.status == Status.FAILED.value
    assert rec.error_code == "non_retryable_error"
    assert rec.attempts == 1
    assert rec.retries[0].backoff_seconds == 0.0
    assert state.calls["s1"] == 1
    print("[不可重试] retry-3 直接终态:", rec.error_code)


def test_step_timeout_is_retryable_and_exhausts(
        runtime_factory, registry):
    enter = threading.Event()
    release = threading.Event()

    def hanging(ctx):
        enter.set()
        release.wait(2)

    registry.define("hang", [hanging])
    rt = runtime_factory(
        RuntimeConfig(default_step_timeout=0.05),
        RetryPolicy(max_attempts=2, base_delay=0.001),
    )
    r = rt.invoke("hang", "retry-4", wait=True, timeout=0.05)
    release.set()
    rec = r.record
    assert rec.status == Status.FAILED.value
    assert [x.error_code for x in rec.retries] == [
        "step_timeout", "step_timeout"]
    print("[步骤超时] retry-4 错误码序列:",
          [x.error_code for x in rec.retries])


def test_overall_deadline_enters_timed_out_terminal(
        runtime_factory, registry):
    def slow(ctx):
        time.sleep(0.3)

    registry.define("slow", [slow])
    rt = runtime_factory(RuntimeConfig())
    r = rt.invoke("slow", "retry-5", wait=True, deadline=0.05)
    assert r.record.status == Status.TIMED_OUT.value
    assert r.record.error_code == "execution_timed_out"
    print("[整体超时] retry-5:", r.record.status,
          r.record.error_message)
