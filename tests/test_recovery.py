"""进程重启恢复：已完成步骤不重复；挂起/超时执行有明确终态；
检查点缺失、版本不兼容、内容损坏必须以可区分原因拒绝恢复。

“进程重启”通过对同一数据目录构造第二个 DurableRuntime 实例模拟
（内存状态全部丢失，仅依赖磁盘检查点）。
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

from dfr.backoff import RetryPolicy
from dfr.errors import (
    CheckpointMissingError,
    CheckpointVersionError,
    CheckpointCorruptError,
)
from dfr.models import ExecutionRecord, Status
from dfr.runtime import DurableRuntime, RuntimeConfig
from dfr.storage import JsonStore


def test_resume_after_restart_skips_completed_steps(
        runtime_factory, registry, observing_steps, tmp_data_dir):
    make_steps, state = observing_steps
    registry.define("pipe", make_steps("s1", "s2", "s3"))
    rt1 = runtime_factory(RuntimeConfig())

    r = rt1.invoke("pipe", "resume-1", payload="X", wait=True,
                   suspend_before_step=2)
    assert r.record.status == Status.SUSPENDED.value
    assert r.record.next_step == 2
    assert state.calls == {"s1": 1, "s2": 1}
    rt1.shutdown()

    # 全新进程：只从磁盘恢复
    rt2 = DurableRuntime(tmp_data_dir, registry, RuntimeConfig())
    outcomes = rt2.recover_all()
    by_key = {o.idempotency_key: o for o in outcomes}
    assert by_key["resume-1"].recovered is True

    rec = rt2.get("resume-1")
    assert rec.status == Status.SUCCEEDED.value
    assert rec.result == "s3(s2(s1(X)))"
    print("\n[恢复] resume-1 恢复后状态迁移:",
          [(t.from_status, t.to_status, t.reason)
           for t in rec.transitions])
    # 已完成的 s1/s2 在新进程中不得再次执行
    assert state.calls == {"s1": 1, "s2": 1, "s3": 1}


def test_resume_after_crash_in_step_executes_it_once(
        runtime_factory, registry, observing_steps, tmp_data_dir):
    """崩溃发生在某一步“执行中”（检查点尚未写下）：

    磁盘停在 next_step=k，恢复后第 k 步最多再执行一次，前 k 步不重复。
    """
    make_steps, state = observing_steps
    registry.define("pipe", make_steps("s1", "s2"))

    # 手工构造“第一个进程在第 2 步执行中崩溃”的磁盘现场
    store = JsonStore(tmp_data_dir)
    partial = ExecutionRecord.new("crash-1", "pipe", payload="C")
    partial.transition(Status.RUNNING, "execution_started")
    partial.steps.append(__import__(
        "dfr.models", fromlist=["StepResult"]).StepResult(
        index=0, name="s1", value="s1(C)"))
    partial.next_step = 1
    store.write("crash-1", partial.to_dict())

    rt = DurableRuntime(tmp_data_dir, registry, RuntimeConfig())
    outcomes = rt.recover_all()
    assert outcomes[0].recovered is True
    rec = rt.get("crash-1")
    assert rec.status == Status.SUCCEEDED.value
    assert state.calls == {"s2": 1}  # s1 未重复，s2 恰好一次
    print("[崩溃恢复] 步骤执行计数:", state.calls)


def test_recover_terminal_execution_is_noop(
        runtime_factory, registry, observing_steps, tmp_data_dir):
    make_steps, state = observing_steps
    registry.define("p", make_steps("s1"))
    rt1 = runtime_factory(RuntimeConfig())
    rt1.invoke("p", "done-1", wait=True)
    rt1.shutdown()

    rt2 = DurableRuntime(tmp_data_dir, registry, RuntimeConfig())
    outcomes = rt2.recover_all()
    assert outcomes[0].recovered is False
    assert outcomes[0].reason == "already_succeeded"
    assert state.calls == {"s1": 1}


def test_recover_rejects_missing_checkpoint(runtime_factory):
    rt = runtime_factory(RuntimeConfig())
    try:
        rt.recover_one("ghost")
        raise AssertionError("应当拒绝恢复缺失检查点")
    except CheckpointMissingError as exc:
        assert exc.code == "checkpoint_missing"
        print("\n[恢复拒绝] 缺失原因码:", exc.code)


def test_recover_rejects_incompatible_version(
        runtime_factory, registry, tmp_data_dir):
    store = JsonStore(tmp_data_dir)
    bad = ExecutionRecord.new("ver-1", "anything").to_dict()
    bad["schema_version"] = 999
    store.write("ver-1", bad)

    rt = DurableRuntime(tmp_data_dir, registry, RuntimeConfig())
    outcomes = rt.recover_all()
    o = outcomes[0]
    assert o.recovered is False
    assert o.error_code == "checkpoint_version_incompatible"
    # 记录仍可原样查询（未被覆盖/重跑）
    assert store.read("ver-1")["schema_version"] == 999
    print("[恢复拒绝] 版本原因码:", o.error_code)


def test_recover_rejects_corrupt_checkpoint(
        runtime_factory, registry, tmp_data_dir):
    store = JsonStore(tmp_data_dir)

    # next_step 与 steps 长度不一致 => 拒绝，防止跳过/重复步骤
    bad = ExecutionRecord.new("corrupt-1", "anything").to_dict()
    bad["status"] = Status.RUNNING.value
    bad["next_step"] = 1  # 但 steps 为空
    store.write("corrupt-1", bad)

    # JSON 半文件 => corrupt
    (store.path_for("corrupt-2")).write_text("{broken json", encoding="utf-8")

    rt = DurableRuntime(tmp_data_dir, registry, RuntimeConfig())
    outcomes = {(o.idempotency_key, o.error_code) for o in rt.recover_all()}
    assert ("corrupt-1", "checkpoint_corrupt") in outcomes
    assert ("corrupt-2", "checkpoint_corrupt") in outcomes
    print("[恢复拒绝] 损坏原因:", outcomes)


def test_suspended_execution_past_deadline_becomes_timed_out(
        runtime_factory, registry, observing_steps, tmp_data_dir):
    make_steps, state = observing_steps
    registry.define("p", make_steps("s1", "s2"))
    rt1 = runtime_factory(RuntimeConfig())
    r = rt1.invoke("p", "dl-1", payload="Z", wait=True,
                   suspend_before_step=1, deadline=10.0)
    assert r.record.status == Status.SUSPENDED.value
    # 模拟截止时间已过（比真实重启等待更快、确定性更高）
    data = JsonStore(tmp_data_dir).read("dl-1")
    data["deadline_epoch"] = time.monotonic() - 1
    JsonStore(tmp_data_dir).write("dl-1", data)
    rt1.shutdown()

    rt2 = DurableRuntime(tmp_data_dir, registry, RuntimeConfig())
    rt2.recover_all()
    rec = rt2.get("dl-1")
    assert rec.status == Status.TIMED_OUT.value
    assert rec.error_code == "execution_timed_out"
    assert state.calls == {"s1": 1}  # 未再执行任何步骤
    print("[超时终态] dl-1:", rec.status, rec.error_message)
