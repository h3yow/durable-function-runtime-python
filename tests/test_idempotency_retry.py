"""Duplicate invocation, retry classification, and terminal failure states."""
import threading

from durable_runtime.models import (
    ErrorKind,
    ExecutionStatus,
    NonRetryableError,
    RetryableError,
)


def test_duplicate_call_returns_same_conclusion(make_runtime, caplog):
    rt = make_runtime()
    runs = {"n": 0}

    def work(ctx, payload):
        runs["n"] += 1
        return {"value": 42}

    rt.register("work", work)
    first = rt.invoke("work", "key-dup")
    second = rt.invoke("work", "key-dup")

    assert first.status == ExecutionStatus.SUCCEEDED
    assert second.status == ExecutionStatus.SUCCEEDED
    assert second.result == first.result == {"value": 42}
    assert runs["n"] == 1, "duplicate call must not re-execute"
    assert "duplicate completed call" in caplog.text


def test_concurrent_duplicate_calls_share_one_execution(make_runtime):
    rt = make_runtime()
    gate = threading.Event()
    runs = {"n": 0}

    def slow(ctx, payload):
        runs["n"] += 1
        gate.wait(5)
        return "done"

    rt.register("slow", slow)
    results, errors = [], []

    def call():
        try:
            results.append(rt.invoke("slow", "key-race").result)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=call) for _ in range(8)]
    for t in threads:
        t.start()
    gate.set()
    for t in threads:
        t.join()

    assert not errors
    assert results == ["done"] * 8
    assert runs["n"] == 1, "concurrent duplicates must execute exactly once"


def test_retryable_error_retries_with_backoff_then_succeeds(make_runtime, caplog):
    rt = make_runtime()
    attempts = {"n": 0}

    def flaky(ctx, payload):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise RetryableError(f"boom {attempts['n']}")
        return "ok"

    rt.register("flaky", flaky)
    rec = rt.invoke("flaky", "key-retry")

    assert rec.status == ExecutionStatus.SUCCEEDED
    assert rec.attempt == 3
    assert [r.attempt for r in rec.retry_history] == [1, 2]
    # exponential backoff: 0.01 then 0.02
    assert rec.retry_history[0].delay_seconds < rec.retry_history[1].delay_seconds
    assert "key-retry" in caplog.text and "retry in" in caplog.text


def test_retryable_exhaustion_enters_queryable_failed_state(make_runtime):
    rt = make_runtime()

    def always(ctx, payload):
        raise RetryableError("permanent transient")

    rt.register("always", always)
    rec = rt.invoke("always", "key-exhaust")

    assert rec.status == ExecutionStatus.FAILED
    assert rec.error_kind == ErrorKind.RETRYABLE
    assert rec.attempt == 3
    assert len(rec.retry_history) == 2
    # terminal state remains queryable and stable
    again = rt.invoke("always", "key-exhaust")
    assert again.status == ExecutionStatus.FAILED
    assert again.attempt == 3


def test_non_retryable_error_fails_immediately(make_runtime):
    rt = make_runtime()
    attempts = {"n": 0}

    def bad(ctx, payload):
        attempts["n"] += 1
        raise NonRetryableError("bad input")

    rt.register("bad", bad)
    rec = rt.invoke("bad", "key-nonretry")

    assert rec.status == ExecutionStatus.FAILED
    assert rec.error_kind == ErrorKind.NON_RETRYABLE
    assert rec.attempt == 1 and not rec.retry_history
