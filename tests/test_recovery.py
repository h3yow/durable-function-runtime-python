"""Process-restart recovery: resume from checkpoints, reject incompatible
checkpoints with distinguishable reasons, never silently rerun or skip."""
from durable_runtime.models import ExecutionStatus, RetryableError
from durable_runtime.runtime import ResumeRejectedReason


def test_restart_resumes_from_checkpoint_without_rerunning_steps(make_runtime):
    # process 1: crashes (killed) after step1 completed and checkpointed
    rt1 = make_runtime()
    runs = {"step1": 0, "step2": 0}

    def two_step(ctx, payload):
        v1 = ctx.step("step1", lambda: runs.__setitem__("step1", runs["step1"] + 1) or 10)
        v2 = ctx.step("step2", lambda: runs.__setitem__("step2", runs["step2"] + 1) or 20)
        return v1 + v2

    rt1.register("two_step", two_step)
    rec = rt1.invoke("two_step", "key-restart")
    assert rec.status == ExecutionStatus.SUCCEEDED and rec.result == 30
    assert runs == {"step1": 1, "step2": 1}

    # corrupt back to mid-flight to emulate a crash right after step1:
    # keep checkpoint of step1, mark RUNNING again (as a kill would leave it)
    rec.status = ExecutionStatus.RUNNING
    rec.checkpoint_data.pop("1")  # step2 never checkpointed
    rec.checkpoint_step = 0
    rt1.storage.save(rec)

    # process 2: fresh runtime over the same DB recovers the execution
    rt2 = make_runtime()
    rt2.register("two_step", two_step)
    affected = rt2.recover()
    assert [r.idempotency_key for r in affected] == ["key-restart"]
    final = rt2.get("key-restart")
    assert final.status == ExecutionStatus.SUCCEEDED and final.result == 30
    assert runs["step1"] == 1, "completed step must not re-execute"
    assert runs["step2"] == 2, "only the missing step runs after recovery"


def test_recovery_rejects_version_mismatch(make_runtime, caplog):
    rt1 = make_runtime(checkpoint_version=1)
    rt1.register("f", lambda ctx, p: 1)
    rec = rt1.invoke("f", "key-ver")
    rec.status = ExecutionStatus.RUNNING  # emulate crash
    rt1.storage.save(rec)

    rt2 = make_runtime(checkpoint_version=2)  # incompatible version
    rt2.register("f", lambda ctx, p: 1)
    affected = rt2.recover()
    assert affected == []
    final = rt2.get("key-ver")
    assert final.status == ExecutionStatus.FAILED
    assert ResumeRejectedReason.VERSION_MISMATCH.value in final.error
    assert "resume_version_mismatch" in caplog.text


def test_recovery_rejects_missing_checkpoint(make_runtime):
    rt1 = make_runtime()
    rt1.register("f", lambda ctx, p: 1)
    rec = rt1.invoke("f", "key-missing")
    # emulate corrupted store: RUNNING at step 2 but its value is gone
    rec.status = ExecutionStatus.RUNNING
    rec.checkpoint_step = 2
    rec.checkpoint_data = {"0": 1}
    rt1.storage.save(rec)

    rt2 = make_runtime()
    rt2.register("f", lambda ctx, p: 1)
    assert rt2.recover() == []
    final = rt2.get("key-missing")
    assert final.status == ExecutionStatus.FAILED
    assert ResumeRejectedReason.CHECKPOINT_MISSING.value in final.error


def test_recovery_rejects_unknown_function(make_runtime):
    rt1 = make_runtime()
    rt1.register("f", lambda ctx, p: 1)
    rec = rt1.invoke("f", "key-unknown")
    rec.status = ExecutionStatus.RUNNING
    rt1.storage.save(rec)

    rt2 = make_runtime()  # nothing registered
    assert rt2.recover() == []
    final = rt2.get("key-unknown")
    assert final.status == ExecutionStatus.FAILED
    assert ResumeRejectedReason.UNKNOWN_FUNCTION.value in final.error


def test_interrupted_retryable_execution_resumes_after_restart(make_runtime):
    rt1 = make_runtime()
    rec_holder = {}

    def flaky(ctx, payload):
        raise RetryableError("still down")

    rt1.register("flaky", flaky)
    rec = rt1.invoke("flaky", "key-retry-crash")
    assert rec.status == ExecutionStatus.FAILED  # retries exhausted in proc 1

    # emulate crash while still retrying: rewind to a retryable state
    rec.status = ExecutionStatus.FAILED_RETRYABLE
    rec.attempt = 1
    rt1.storage.save(rec)

    rt2 = make_runtime()
    rt2.register("flaky", lambda ctx, p: "recovered")
    affected = rt2.recover()
    assert [r.idempotency_key for r in affected] == ["key-retry-crash"]
    final = rt2.get("key-retry-crash")
    assert final.status == ExecutionStatus.SUCCEEDED
    assert final.result == "recovered"
