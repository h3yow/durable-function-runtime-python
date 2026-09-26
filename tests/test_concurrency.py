"""并发与背压：同键串行、超并发有界拒绝/排队、配额拒绝、状态不错乱。"""

from __future__ import annotations

import threading
import time

from dfr.errors import ConcurrencyLimitRejectedError, QuotaExceededError
from dfr.models import Status
from dfr.runtime import RuntimeConfig


def test_same_key_concurrent_calls_execute_once(
        runtime_factory, registry, observing_steps):
    make_steps, state = observing_steps
    state.gate("s1")  # 确保首请求停在第一步
    registry.define("p", make_steps("s1", "s2"))
    rt = runtime_factory(RuntimeConfig(max_concurrency=4, max_queue=4))

    started = threading.Barrier(6)
    results = []

    def hit():
        started.wait()
        results.append(rt.invoke("p", "race-1", payload="R", wait=True))

    threads = [threading.Thread(target=hit) for _ in range(6)]
    for t in threads:
        t.start()
    time.sleep(0.15)       # 让首个请求进入 s1
    state.gate("s1").set()  # 放行
    for t in threads:
        t.join()

    accepted = [r for r in results if not r.replayed]
    replayed = [r for r in results if r.replayed]
    assert len(accepted) == 1, [r.record.status for r in accepted]
    assert len(replayed) == 5
    assert all(r.record.status == Status.SUCCEEDED.value for r in results)
    assert all(r.record.result == "s2(s1(R))" for r in results)
    assert state.calls == {"s1": 1, "s2": 1}
    print("\n[并发同键] race-1 真实执行=1 重放=5，副作用计数:", state.calls)


def test_capacity_overflow_is_bounded_rejected_with_distinct_code(
        runtime_factory, registry, observing_steps):
    make_steps, state = observing_steps
    state.gate("s1").clear()  # 全部阻塞
    registry.define("p", make_steps("s1"))
    rt = runtime_factory(RuntimeConfig(max_concurrency=1, max_queue=1))

    # 1 个执行 + 1 个排队
    t1 = threading.Thread(
        target=lambda: rt.invoke("p", "c1", wait=True))
    t2 = threading.Thread(
        target=lambda: rt.invoke("p", "c2", wait=False))
    t1.start(); time.sleep(0.1); t2.start(); time.sleep(0.15)

    # 第 3 个必须被有界拒绝，原因码可区分
    with __import__("pytest").raises(ConcurrencyLimitRejectedError) as ei:
        rt.invoke("p", "c3", wait=False)
    assert ei.value.code == "concurrency_limit_rejected"
    print("[背压拒绝] 原因码:", ei.value.code)

    state.gate("s1").set()
    t1.join(); t2.join()

    # 已接受的 c1/c2 不丢失，最终都成功
    for _ in range(100):
        if rt.get("c2").status == Status.SUCCEEDED.value:
            break
        time.sleep(0.01)
    assert rt.get("c1").status == Status.SUCCEEDED.value
    assert rt.get("c2").status == Status.SUCCEEDED.value
    # c3 被拒绝：没有任何记录被创建
    assert rt.get("c3") is None


def test_queued_calls_complete_without_state_corruption(
        runtime_factory, registry, observing_steps):
    make_steps, state = observing_steps
    registry.define("p", make_steps("s1"))
    rt = runtime_factory(RuntimeConfig(max_concurrency=1, max_queue=3))

    queued: list[str] = []
    order_lock = threading.Lock()

    def hit(key):
        r = rt.invoke("p", key, payload=key, wait=False)
        if r.reason == "queued":
            with order_lock:
                queued.append(key)

    # 先用一个慢任务占住唯一执行槽
    state.gate("s1").clear()
    holder = threading.Thread(
        target=lambda: rt.invoke("p", "hold", wait=False))
    holder.start()
    time.sleep(0.1)

    keys = [f"q{i}" for i in range(3)]
    threads = [threading.Thread(target=hit, args=(k,)) for k in keys]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # 三个调用全部有界排队（无一丢失、无一被错误拒绝）
    assert sorted(queued) == sorted(keys)

    state.gate("s1").set()
    for _ in range(200):
        statuses = [rt.get(k).status for k in keys]
        if all(s == Status.SUCCEEDED.value for s in statuses):
            break
        time.sleep(0.01)
    for k in keys:
        rec = rt.get(k)
        assert rec.status == Status.SUCCEEDED.value
        assert rec.result == f"s1({k})", rec.result  # 载荷未串号
    print("[排队] 全部排队调用:", sorted(queued), "各执行结果互不串号")


def test_quota_rejection_is_distinct_from_capacity_rejection(
        runtime_factory, registry, observing_steps):
    make_steps, state = observing_steps
    state.gate("s1").clear()
    registry.define("p", make_steps("s1"))
    rt = runtime_factory(RuntimeConfig(
        max_concurrency=4, max_queue=4, max_calls=2))

    rt.invoke("p", "quota-1", wait=False)
    rt.invoke("p", "quota-2", wait=False)
    try:
        rt.invoke("p", "quota-3", wait=False)
        raise AssertionError("配额耗尽必须被拒绝")
    except QuotaExceededError as exc:
        assert exc.code == "quota_exceeded"
        print("[配额拒绝] 原因码:", exc.code)

    # 重放不消耗新配额：已存在键仍可查询/重放
    replay = rt.invoke("p", "quota-1", wait=False)
    assert replay.replayed is True
    state.gate("s1").set()
