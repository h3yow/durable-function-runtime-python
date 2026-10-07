"""Per-call time budgets & step timeouts, caller-initiated cancellation,
and transient rejections (capacity vs. quota)."""
import threading
import time

import pytest

from durable_runtime.models import (
    ExecutionStatus,
    NonRetryableError,
    RetryPolicy,
    RetryableError,
)
from durable_runtime.scheduler import RejectedError


# ------------------------------------------------------------------ budget

def test_per_call_budget_overrides_global_default(make_runtime, caplog):
    rt = make_runtime(execution_timeout=10.0)  # generous global default

    def sleepy(ctx, payload):
        ctx.step("nap", lambda: time.sleep(1.0))
        return "unreachable"

    rt.register("sleepy", sleepy)
    start = time.monotonic()
    rec = rt.invoke("sleepy", "key-budget", timeout=0.3)
    elapsed = time.monotonic() - start

    assert rec.status == ExecutionStatus.TIMED_OUT
    assert "deadline" in rec.error
    # converged promptly: did not wait for the 1s step or the 10s default
    assert elapsed < 5.0
    assert "key-budget" in caplog.text


def test_step_timeout_is_retryable_and_converges(make_runtime, caplog):
    rt = make_runtime()
    runs = {"s0": 0, "s1": 0}

    def fn(ctx, payload):
        ctx.step("s0", lambda: runs.__setitem__("s0", runs["s0"] + 1) or "v0")

        def flaky_step():
            runs["s1"] += 1
            if runs["s1"] == 1:
                time.sleep(1.0)  # exceeds the step limit on attempt 1
            return "v1"

        v1 = ctx.step("s1", flaky_step, timeout=0.2)
        return v1

    rt.register("fn", fn)
    rec = rt.invoke("fn", "key-step-timeout")

    assert rec.status == ExecutionStatus.SUCCEEDED
    assert rec.result == "v1"
    assert rec.attempt == 2, "step timeout must trigger a backoff retry"
    assert len(rec.retry_history) == 1
    assert "exceeded its time limit" in rec.retry_history[0].error
    assert runs["s0"] == 1, "checkpointed step must not rerun on retry"
    assert runs["s1"] == 2, "timed-out step is re-executed on the retry"
    assert "key-step-timeout" in caplog.text and "retry in" in caplog.text


def test_step_timeout_still_bounded_by_overall_budget(make_runtime):
    rt = make_runtime()

    def fn(ctx, payload):
        ctx.step("slow", lambda: time.sleep(2.0), timeout=5.0)
        return "unreachable"

    rt.register("fn", fn)
    start = time.monotonic()
    # per-call budget 0.4s < step limit 5s: the budget wins
    rec = rt.invoke("fn", "key-budget-wins", timeout=0.4)
    elapsed = time.monotonic() - start

    assert rec.status == ExecutionStatus.TIMED_OUT
    assert elapsed < 5.0, "must converge on the budget, not the step limit"


def test_retry_backoff_wait_counts_against_budget(make_runtime):
    rt = make_runtime(retry_policy=RetryPolicy(
        max_attempts=10, base_delay_seconds=0.05, backoff_multiplier=1.0))

    def always(ctx, payload):
        raise RetryableError("still down")

    rt.register("always", always)
    rec = rt.invoke("always", "key-budget-retry", timeout=0.12)

    # budget ran out during a backoff wait: TIMED_OUT, not retries-exhausted
    assert rec.status == ExecutionStatus.TIMED_OUT
    assert rec.attempt < 10
    assert len(rec.retry_history) >= 1


# ------------------------------------------------------------------ cancel

def test_cancel_queued_execution(make_runtime, caplog):
    rt = make_runtime(max_concurrency=1, max_queue=1)
    gate = threading.Event()
    runs = {"a": 0, "b": 0}

    def work(ctx, payload):
        runs[payload["who"]] += 1
        gate.wait(5)
        return "done"

    rt.register("work", work)
    results = {}

    def call(key, who):
        results[key] = rt.invoke("work", key, {"who": who}).status

    ta = threading.Thread(target=call, args=("key-a", "a"))
    tb = threading.Thread(target=call, args=("key-b", "b"))
    ta.start()
    time.sleep(0.2)          # key-a now occupies the single worker
    tb.start()
    time.sleep(0.2)          # key-b accepted, waiting in the queue

    rec = rt.cancel("key-b")
    assert rec.status == ExecutionStatus.CANCELLED

    gate.set()
    ta.join(5)
    tb.join(5)

    assert results["key-a"] == ExecutionStatus.SUCCEEDED
    assert results["key-b"] == ExecutionStatus.CANCELLED
    assert rt.get("key-b").status == ExecutionStatus.CANCELLED
    assert runs == {"a": 1, "b": 0}, "cancelled queued call must never run"
    assert "cancel accepted" in caplog.text


def test_cancel_running_execution_stops_remaining_steps(make_runtime):
    rt = make_runtime(max_concurrency=1)
    started = threading.Event()
    release = threading.Event()
    runs = {"s1": 0, "s3": 0}

    def fn(ctx, payload):
        ctx.step("s1", lambda: runs.__setitem__("s1", runs["s1"] + 1))

        def blocking():
            started.set()
            release.wait(5)
            return 2

        ctx.step("s2", blocking)
        ctx.step("s3", lambda: runs.__setitem__("s3", runs["s3"] + 1))
        return "done"

    rt.register("fn", fn)
    holder = {}

    def call():
        holder["rec"] = rt.invoke("fn", "key-cancel-run")

    t = threading.Thread(target=call)
    t.start()
    assert started.wait(5), "execution should be inside step s2"

    rec = rt.cancel("key-cancel-run")
    assert rec.status == ExecutionStatus.CANCELLED

    t.join(5)
    assert not t.is_alive(), "cancelled execution must converge promptly"
    final = rt.get("key-cancel-run")
    assert final.status == ExecutionStatus.CANCELLED
    assert runs["s1"] == 1
    assert runs["s3"] == 0, "steps after the cancellation point never run"
    release.set()  # let the orphaned step thread finish


def test_cancel_is_idempotent_and_does_not_rewrite_terminal(make_runtime):
    rt = make_runtime()
    rt.register("echo", lambda ctx, p: "ok")

    done = rt.invoke("echo", "key-done")
    assert done.status == ExecutionStatus.SUCCEEDED
    # cancelling an already-terminal call keeps the original conclusion
    again = rt.cancel("key-done")
    assert again.status == ExecutionStatus.SUCCEEDED

    gate = threading.Event()

    def slow(ctx, payload):
        ctx.step("nap", lambda: gate.wait(5))
        return "done"

    rt.register("slow", slow)
    t = threading.Thread(target=lambda: rt.invoke("slow", "key-cx"))
    t.start()
    time.sleep(0.2)
    first = rt.cancel("key-cx")
    second = rt.cancel("key-cx")
    t.join(5)
    gate.set()
    assert first.status == second.status == ExecutionStatus.CANCELLED
    assert first.error == second.error == "cancelled by caller"


def test_cancelled_execution_is_not_resumed_after_restart(make_runtime):
    rt1 = make_runtime(max_concurrency=1)
    gate = threading.Event()

    def slow(ctx, payload):
        ctx.step("nap", lambda: gate.wait(5))
        return "done"

    rt1.register("slow", slow)
    t = threading.Thread(target=lambda: rt1.invoke("slow", "key-cancel-restart"))
    t.start()
    time.sleep(0.2)
    assert rt1.cancel("key-cancel-restart").status == ExecutionStatus.CANCELLED
    t.join(5)
    gate.set()

    # fresh process over the same DB: terminal CANCELLED must stay untouched
    rt2 = make_runtime()
    rt2.register("slow", slow)
    assert rt2.recover() == []
    assert rt2.get("key-cancel-restart").status == ExecutionStatus.CANCELLED


# ------------------------------------------------------------------ reject

def test_rejected_key_is_accepted_after_capacity_recovers(make_runtime, caplog):
    rt = make_runtime(max_concurrency=1, max_queue=0)
    gate = threading.Event()

    def slow(ctx, payload):
        gate.wait(5)
        return "done"

    rt.register("slow", slow)
    holder = {}

    def call():
        holder["a"] = rt.invoke("slow", "key-a").status

    t = threading.Thread(target=call)
    t.start()
    time.sleep(0.2)  # key-a holds the only permit

    with pytest.raises(RejectedError) as excinfo:
        rt.invoke("slow", "key-b")
    assert excinfo.value.reason.value == "queue_full"
    assert rt.get("key-b").status == ExecutionStatus.REJECTED

    gate.set()
    t.join(5)
    assert holder["a"] == ExecutionStatus.SUCCEEDED

    # capacity recovered: the SAME idempotency key is admitted afresh
    rec = rt.invoke("slow", "key-b")
    assert rec.status == ExecutionStatus.SUCCEEDED
    assert "transient" in caplog.text


def test_rejection_does_not_consume_queue_slot(make_runtime):
    rt = make_runtime(max_concurrency=1, max_queue=1)
    gate = threading.Event()
    rt.register("slow", lambda ctx, p: gate.wait(5) or "done")

    results = {}

    def call(key):
        try:
            results[key] = rt.invoke("slow", key).status
        except RejectedError:
            results[key] = "rejected"

    threads = [threading.Thread(target=call, args=(f"key-{i}",))
               for i in range(2)]
    for t in threads:
        t.start()
    time.sleep(0.3)  # both permits (1 running + 1 queued) now held

    for i in range(2, 5):  # overflow: rejected without touching the queue
        with pytest.raises(RejectedError):
            rt.invoke("slow", f"key-{i}")

    gate.set()
    for t in threads:
        t.join(5)
    # the two admitted calls were unaffected by the rejections
    assert results == {"key-0": ExecutionStatus.SUCCEEDED,
                       "key-1": ExecutionStatus.SUCCEEDED}


def test_quota_exceeded_is_distinguishable_from_queue_full(make_runtime):
    rt = make_runtime(quotas={"limited": 1})
    rt.register("limited", lambda ctx, p: "ok")
    rt.register("other", lambda ctx, p: "ok")

    assert rt.invoke("limited", "key-q1").status == ExecutionStatus.SUCCEEDED

    with pytest.raises(RejectedError) as excinfo:
        rt.invoke("limited", "key-q2")
    assert excinfo.value.reason.value == "quota_exceeded"
    rejected = rt.get("key-q2")
    assert rejected.status == ExecutionStatus.REJECTED
    assert rejected.error == "rejected:quota_exceeded"

    # quota is per-function: other functions are unaffected
    assert rt.invoke("other", "key-q3").status == ExecutionStatus.SUCCEEDED

    # the two rejection reasons are distinguishable
    assert excinfo.value.reason.value != "queue_full"


def test_terminal_states_are_distinguishable(make_runtime):
    rt = make_runtime(quotas={"quota-fn": 1})
    rt.register("ok", lambda ctx, p: "ok")
    rt.register("bad", lambda ctx, p: (_ for _ in ()).throw(
        NonRetryableError("nope")))
    rt.register("nap", lambda ctx, p: ctx.step("s", lambda: time.sleep(1)))
    rt.register("quota-fn", lambda ctx, p: "ok")

    rt.invoke("quota-fn", "k-fill")  # consume the single quota slot

    failed = rt.invoke("bad", "k-fail")
    timed_out = rt.invoke("nap", "k-timeout", timeout=0.2)
    with pytest.raises(RejectedError):
        rt.invoke("quota-fn", "k-reject")
    rejected = rt.get("k-reject")

    gate = threading.Event()
    rt.register("slow", lambda ctx, p: ctx.step("s", lambda: gate.wait(5)))
    t = threading.Thread(target=lambda: rt.invoke("slow", "k-cancel"))
    t.start()
    time.sleep(0.2)
    cancelled = rt.cancel("k-cancel")
    t.join(5)
    gate.set()

    statuses = {failed.status, timed_out.status, rejected.status,
                cancelled.status}
    assert statuses == {ExecutionStatus.FAILED, ExecutionStatus.TIMED_OUT,
                        ExecutionStatus.REJECTED, ExecutionStatus.CANCELLED}
    assert len(statuses) == 4, "terminal conclusions must be distinguishable"
