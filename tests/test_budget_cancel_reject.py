"""Per-call time budgets, step timeouts, caller cancellation, and
transient capacity/quota rejections."""
import threading
import time

import pytest

from durable_runtime.models import ExecutionStatus, RetryPolicy, RetryableError
from durable_runtime.scheduler import RejectedError


# ----------------------------------------------------------------- time budget

def test_per_call_budget_overrides_global_default(make_runtime):
    rt = make_runtime(execution_timeout=30.0)  # generous global default

    def sleepy(ctx, payload):
        ctx.step("nap", lambda: time.sleep(1.0))
        return "unreachable"

    rt.register("sleepy", sleepy)
    rec = rt.invoke("sleepy", "key-budget", timeout=0.3)

    assert rec.status == ExecutionStatus.TIMED_OUT
    assert "deadline" in rec.error


def test_budget_exhaustion_converges_instead_of_retrying_forever(make_runtime):
    # retries are (nearly) unlimited: the budget, not the attempt count,
    # must terminate this execution in a queryable terminal state
    rt = make_runtime(
        retry_policy=RetryPolicy(max_attempts=100, base_delay_seconds=0.01))

    def slow_step(ctx, payload):
        ctx.step("too-slow", lambda: time.sleep(0.2), timeout=0.05)
        return "unreachable"

    rt.register("slow_step", slow_step)
    start = time.monotonic()
    rec = rt.invoke("slow_step", "key-budget2", timeout=0.5)
    elapsed = time.monotonic() - start

    assert rec.status == ExecutionStatus.TIMED_OUT
    assert elapsed < 5.0, "must converge promptly once the budget is spent"
    assert rec.retry_history, "step timeouts retried before the budget ran out"
    assert all("exceeded" in r.error for r in rec.retry_history)


def test_step_timeout_retries_and_completed_steps_are_not_rerun(make_runtime):
    rt = make_runtime()
    runs = {"fast": 0, "slow": 0}

    def two_step(ctx, payload):
        v1 = ctx.step("fast", lambda: runs.__setitem__("fast", runs["fast"] + 1) or 1)

        def slow():
            runs["slow"] += 1
            if runs["slow"] == 1:
                time.sleep(0.2)  # breaches the 0.05s step limit once
            return 10

        v2 = ctx.step("slow", slow, timeout=0.05)
        return v1 + v2

    rt.register("two_step", two_step)
    rec = rt.invoke("two_step", "key-step-timeout")

    assert rec.status == ExecutionStatus.SUCCEEDED
    assert rec.result == 11
    assert rec.attempt == 2, "first attempt retried after the step timeout"
    assert runs["fast"] == 1, "checkpointed step must not re-run on retry"
    assert runs["slow"] == 2, "only the timed-out step is re-executed"
    assert "exceeded" in rec.retry_history[0].error


# ----------------------------------------------------------------- cancellation

def test_cancel_queued_invocation(make_runtime):
    rt = make_runtime(max_concurrency=1, max_queue=4)
    gate = threading.Event()
    runs = {"a": 0, "b": 0}

    def work(ctx, payload):
        runs["a"] += 1
        gate.wait(5)
        return "done"

    def queued_work(ctx, payload):
        runs["b"] += 1
        return "should-never-run"

    rt.register("work", work)
    rt.register("queued_work", queued_work)
    results = {}

    t1 = threading.Thread(
        target=lambda: results.__setitem__("a", rt.invoke("work", "key-a").status))
    t2 = threading.Thread(
        target=lambda: results.__setitem__("b", rt.invoke("queued_work", "key-b").status))
    t1.start()
    t2.start()
    time.sleep(0.3)  # key-a running, key-b accepted and queued

    cancelled = rt.cancel("key-b")
    assert cancelled.status == ExecutionStatus.CANCELLED
    # repeated cancel is idempotent and reaches the same conclusion
    again = rt.cancel("key-b")
    assert again.status == ExecutionStatus.CANCELLED

    gate.set()
    t1.join()
    t2.join()
    assert results["a"] == ExecutionStatus.SUCCEEDED
    assert results["b"] == ExecutionStatus.CANCELLED
    assert runs["b"] == 0, "cancelled queued invocation must never start"
    # distinguishable from the other terminal states
    final = rt.get("key-b")
    assert final.status not in (ExecutionStatus.FAILED,
                                ExecutionStatus.TIMED_OUT,
                                ExecutionStatus.REJECTED)


def test_cancel_running_invocation_stops_remaining_steps(make_runtime):
    rt = make_runtime()
    entered = threading.Event()
    release = threading.Event()
    runs = {"s1": 0, "s2": 0}

    def two_step(ctx, payload):
        ctx.step("s1", lambda: runs.__setitem__("s1", 1) or 1)
        entered.set()
        release.wait(5)
        ctx.step("s2", lambda: runs.__setitem__("s2", 1) or 2)
        return "unreachable"

    rt.register("two_step", two_step)
    result = {}
    t = threading.Thread(
        target=lambda: result.__setitem__("rec", rt.invoke("two_step", "key-run")))
    t.start()
    assert entered.wait(5), "execution should be running its first steps"

    cancelled = rt.cancel("key-run")
    assert cancelled.status == ExecutionStatus.CANCELLED
    release.set()
    t.join()

    assert result["rec"].status == ExecutionStatus.CANCELLED
    assert runs["s2"] == 0, "remaining steps must not advance after cancel"
    # cancelling again (or a duplicate invoke) returns the same conclusion
    assert rt.cancel("key-run").status == ExecutionStatus.CANCELLED
    assert rt.invoke("two_step", "key-run").status == ExecutionStatus.CANCELLED


def test_cancel_interrupts_retry_backoff(make_runtime):
    rt = make_runtime(
        retry_policy=RetryPolicy(max_attempts=5, base_delay_seconds=30.0))
    failed_once = threading.Event()

    def flaky(ctx, payload):
        failed_once.set()
        raise RetryableError("down")

    rt.register("flaky", flaky)
    result = {}
    t = threading.Thread(
        target=lambda: result.__setitem__("rec", rt.invoke("flaky", "key-backoff")))
    t.start()
    assert failed_once.wait(5)
    time.sleep(0.2)  # now sleeping out a 30s backoff

    start = time.monotonic()
    rt.cancel("key-backoff")
    t.join()
    elapsed = time.monotonic() - start

    assert result["rec"].status == ExecutionStatus.CANCELLED
    assert elapsed < 5.0, "cancellation must interrupt the backoff wait"


def test_cancelled_invocation_is_not_resumed_after_restart(make_runtime):
    rt1 = make_runtime(max_concurrency=1, max_queue=4)
    gate = threading.Event()

    def work(ctx, payload):
        gate.wait(5)
        return "done"

    rt1.register("work", work)
    t1 = threading.Thread(target=lambda: rt1.invoke("work", "key-x"))
    t2 = threading.Thread(target=lambda: rt1.invoke("work", "key-cancel"))
    t1.start()
    t2.start()
    time.sleep(0.3)
    assert rt1.cancel("key-cancel").status == ExecutionStatus.CANCELLED
    gate.set()
    t1.join()
    t2.join()

    rt2 = make_runtime()  # fresh "process" over the same DB
    rt2.register("work", work)
    assert rt2.recover() == [], "cancelled executions must not be resumed"
    assert rt2.get("key-cancel").status == ExecutionStatus.CANCELLED


def test_cancel_unknown_key_returns_none(make_runtime):
    rt = make_runtime()
    assert rt.cancel("no-such-key") is None


# ----------------------------------------------------------------- rejection

def test_capacity_rejection_is_transient_and_key_can_be_resubmitted(make_runtime):
    rt = make_runtime(max_concurrency=1, max_queue=1)
    gate = threading.Event()

    def slow(ctx, payload):
        gate.wait(5)
        return "done"

    rt.register("slow", slow)
    t1 = threading.Thread(target=lambda: rt.invoke("slow", "key-a"))
    t2 = threading.Thread(target=lambda: rt.invoke("slow", "key-b"))
    t1.start()
    t2.start()
    time.sleep(0.3)  # worker + queue both occupied

    with pytest.raises(RejectedError) as excinfo:
        rt.invoke("slow", "key-c")
    assert excinfo.value.reason.value == "queue_full"
    # the rejection is queryable but is only "not accepted this time"
    assert rt.get("key-c").status == ExecutionStatus.REJECTED
    assert rt.get("key-c").error == "rejected:queue_full"

    # capacity recovers -> the very same key is accepted and concludes
    gate.set()
    t1.join()
    t2.join()
    rec = rt.invoke("slow", "key-c")
    assert rec.status == ExecutionStatus.SUCCEEDED
    assert rec.result == "done"
    assert rec.attempt == 1, "resubmission is a fresh invocation"


def test_rejected_call_does_not_block_accepted_calls(make_runtime):
    rt = make_runtime(max_concurrency=1, max_queue=1)
    gate = threading.Event()

    def slow(ctx, payload):
        gate.wait(5)
        return "done"

    rt.register("slow", slow)
    outcomes = {}

    def call(key):
        outcomes[key] = rt.invoke("slow", key).status

    t1 = threading.Thread(target=call, args=("key-a",))
    t2 = threading.Thread(target=call, args=("key-b",))
    t1.start()
    t2.start()
    time.sleep(0.3)
    with pytest.raises(RejectedError):
        rt.invoke("slow", "key-c")
    gate.set()
    t1.join()
    t2.join()
    # the rejected call neither occupied a slot nor disturbed accepted ones
    assert outcomes == {"key-a": ExecutionStatus.SUCCEEDED,
                        "key-b": ExecutionStatus.SUCCEEDED}


def test_quota_exhaustion_is_distinguishable_from_capacity(make_runtime):
    rt = make_runtime(invocation_quota=1)
    rt.register("f", lambda ctx, p: "ok")

    assert rt.invoke("f", "key-1").status == ExecutionStatus.SUCCEEDED

    with pytest.raises(RejectedError) as excinfo:
        rt.invoke("f", "key-2")
    assert excinfo.value.reason.value == "quota_exhausted"
    assert excinfo.value.reason.value != "queue_full"

    rec = rt.get("key-2")
    assert rec.status == ExecutionStatus.REJECTED
    assert rec.error == "rejected:quota_exhausted"
    # the rejected call did not consume quota or capacity
    assert rt.storage.count_accepted() == 1
