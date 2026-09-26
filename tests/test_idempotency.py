"""重复调用不得产生重复副作用，且必须返回与首次一致的结论。

覆盖：
* 成功执行后，同键重复调用返回同一结果且步骤不重复执行；
* 失败终态后，同键重复调用返回同一失败结论（不重新执行）；
* 不同入参使用同一幂等键时，以首次请求为准（键即身份）；
* 不同幂等键互不影响。
"""

from __future__ import annotations

import logging

from dfr.backoff import RetryPolicy
from dfr.models import Status
from dfr.runtime import RuntimeConfig

logger = logging.getLogger("dfr.test.idempotency")


def test_duplicate_call_has_no_extra_side_effects(
        runtime_factory, registry, observing_steps, caplog):
    make_steps, state = observing_steps
    registry.define("pipeline", make_steps("s1", "s2", "s3"))
    rt = runtime_factory(RuntimeConfig())

    caplog.set_level(logging.INFO, logger="dfr")
    r1 = rt.invoke("pipeline", "idem-1", payload="P", wait=True)
    assert r1.record.status == Status.SUCCEEDED.value
    assert r1.record.result == "s3(s2(s1(P)))"

    for _ in range(3):
        r = rt.invoke("pipeline", "idem-1", payload="P", wait=True)
        assert r.replayed is True
        assert r.reason == "idempotent_replay"
        assert r.record.to_dict() == r1.record.to_dict()

    print("\n[幂等] 调用标识=idem-1 状态迁移:",
          [(t.from_status, t.to_status) for t in r1.record.transitions])
    print("[幂等] 副作用计数:", state.calls)
    assert state.calls == {"s1": 1, "s2": 1, "s3": 1}


def test_duplicate_call_after_failure_returns_same_conclusion(
        runtime_factory, registry, observing_steps):
    make_steps, state = observing_steps
    registry.define("bad", make_steps(
        "s1", fail={"s1": 9}, non_retryable={"s1"}))
    rt = runtime_factory(
        RuntimeConfig(),
        RetryPolicy(max_attempts=2, base_delay=0.001),
    )
    r1 = rt.invoke("bad", "idem-fail", wait=True)
    assert r1.record.status == Status.FAILED.value
    assert r1.record.error_code == "non_retryable_error"
    first_dict = r1.record.to_dict()

    r2 = rt.invoke("bad", "idem-fail", wait=True)
    assert r2.replayed is True
    assert r2.record.status == Status.FAILED.value
    assert r2.record.error_code == "non_retryable_error"
    assert r2.record.to_dict() == first_dict
    # 失败后重放不得重试/再执行
    assert state.calls == {"s1": 1}
    print("\n[幂等失败] 终态:", r2.record.status,
          "原因码:", r2.record.error_code,
          "迁移:", [(t.to_status, t.reason) for t in r2.record.transitions])


def test_same_key_ignores_different_payload(
        runtime_factory, registry, observing_steps):
    make_steps, state = observing_steps
    registry.define("p", make_steps("s1"))
    rt = runtime_factory(RuntimeConfig())

    r1 = rt.invoke("p", "same-key", payload="first", wait=True)
    r2 = rt.invoke("p", "same-key", payload="second", wait=True)
    assert r2.replayed is True
    assert r1.record.payload == "first"
    assert r2.record.result == r1.record.result == "s1(first)"
    assert state.calls == {"s1": 1}


def test_distinct_keys_are_independent(
        runtime_factory, registry, observing_steps):
    make_steps, state = observing_steps
    registry.define("p", make_steps("s1"))
    rt = runtime_factory(RuntimeConfig())
    a = rt.invoke("p", "key-A", payload="a", wait=True)
    b = rt.invoke("p", "key-B", payload="b", wait=True)
    assert a.record.result == "s1(a)" and b.record.result == "s1(b)"
    assert state.calls == {"s1": 2}
