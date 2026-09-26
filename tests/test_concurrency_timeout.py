"""Concurrency ceilings, bounded-queue backpressure, and timeouts."""
import threading
import time

import pytest

from durable_runtime.models import ExecutionStatus
from durable_runtime.scheduler import RejectedError


def test_concurrency_ceiling_is_respected(make_runtime):
    rt = make_runtime(max_concurrency=2, max_queue=8)
    current, peak = 0, 0
    lock = threading.Lock()
    gate = threading.Event()

    def work(ctx, payload):
        nonlocal peak
        with lock:
            nonlocal_current[0] += 1
            peak = max(peak, nonlocal_current[0])
        gate.wait(5)
        with lock:
            nonlocal_current[0] -= 1
        return "ok"

    nonlocal_current = [0]
    rt.register("work", work)
    results = []

    def call(i):
        results.append(rt.invoke("work", f"key-c{i}").status)

    threads = [threading.Thread(target=call, args=(i,)) for i in range(6)]
    for t in threads:
        t.start()
    time.sleep(0.2)
    gate.set()
    for t in threads:
        t.join()

    assert peak == 2, f"peak concurrency {peak} exceeded ceiling 2"
    assert results == [ExecutionStatus.SUCCEEDED] * 6


def test_queue_overflow_rejects_with_distinguishable_reason(make_runtime):
    rt = make_runtime(max_concurrency=1, max_queue=1)
    gate = threading.Event()

    def slow(ctx, payload):
        gate.wait(5)
        return "done"

    rt.register("slow", slow)
    accepted_results = []

    def call(key):
        accepted_results.append(rt.invoke("slow", key).status)

    t1 = threading.Thread(target=call, args=("key-a",))  # occupies worker
    t2 = threading.Thread(target=call, args=("key-b",))  # occupies queue
    t1.start()
    t2.start()
    time.sleep(0.3)  # let both get accepted

    with pytest.raises(RejectedError) as excinfo:
        rt.invoke("slow", "key-c")
    assert excinfo.value.reason.value == "queue_full"

    # rejection is persisted as a queryable terminal state
    rejected = rt.get("key-c")
    assert rejected.status == ExecutionStatus.REJECTED
    assert rejected.error == "rejected:queue_full"

    # accepted calls are not lost or corrupted by the rejection
    gate.set()
    t1.join()
    t2.join()
    assert sorted(accepted_results) == [ExecutionStatus.SUCCEEDED] * 2
    assert rt.get("key-a").status == ExecutionStatus.SUCCEEDED
    assert rt.get("key-b").status == ExecutionStatus.SUCCEEDED


def test_execution_deadline_leads_to_timed_out_terminal(make_runtime, caplog):
    rt = make_runtime(execution_timeout=0.3)

    def sleepy(ctx, payload):
        ctx.step("nap", lambda: time.sleep(1.0))
        return "unreachable"

    rt.register("sleepy", sleepy)
    rec = rt.invoke("sleepy", "key-timeout")

    assert rec.status == ExecutionStatus.TIMED_OUT
    assert "deadline" in rec.error
    # terminal and stable across duplicate calls
    assert rt.invoke("sleepy", "key-timeout").status == ExecutionStatus.TIMED_OUT
    assert "key-timeout" in caplog.text


def test_timed_out_execution_is_not_resumed_after_restart(make_runtime):
    rt1 = make_runtime(execution_timeout=0.3)

    def sleepy(ctx, payload):
        ctx.step("nap", lambda: time.sleep(1.0))
        return "unreachable"

    rt1.register("sleepy", sleepy)
    rec = rt1.invoke("sleepy", "key-timeout2")
    assert rec.status == ExecutionStatus.TIMED_OUT

    rt2 = make_runtime()
    rt2.register("sleepy", sleepy)
    assert rt2.recover() == [], "terminal executions must not be resumed"
