"""HTTP 层集成：幂等调用、终态查询、可区分拒绝码、恢复端点。"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from dfr.api import create_app
from dfr.demo import build_demo_registry
from dfr.runtime import DurableRuntime, RuntimeConfig


@pytest.fixture()
def client(tmp_path):
    data_dir = tmp_path / "data"
    registry = build_demo_registry(data_dir)
    rt = DurableRuntime(data_dir, registry, RuntimeConfig(
        max_concurrency=1, max_queue=1, max_calls=3))
    app = create_app(rt, auto_recover=False)
    with TestClient(app) as c:
        c.rt = rt  # type: ignore[attr-defined]
        c.data_dir = data_dir  # type: ignore[attr-defined]
        yield c
    rt.shutdown()


def test_invoke_duplicate_and_query(client):
    body = {"function": "echo_then_append", "idempotency_key": "http-1",
            "payload": "hello"}
    r1 = client.post("/invoke", json=body)
    assert r1.status_code == 200
    assert r1.json()["execution"]["result"] == "hello->done"

    r2 = client.post("/invoke", json=body)
    assert r2.json()["replayed"] is True
    assert r2.json()["execution"]["result"] == "hello->done"

    q = client.get("/executions/http-1")
    assert q.json()["execution"]["status"] == "SUCCEEDED"
    assert client.get("/executions/missing").status_code == 404


def test_function_not_found_is_distinct_error(client):
    r = client.post("/invoke", json={
        "function": "nope", "idempotency_key": "x"})
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "function_not_found"


def test_capacity_and_quota_rejection_codes(client):
    # 占住唯一执行槽与唯一排队槽
    t1 = client.post("/invoke", json={
        "function": "slow", "idempotency_key": "s1", "wait": False})
    assert t1.json()["accepted"] is True
    t2 = client.post("/invoke", json={
        "function": "slow", "idempotency_key": "s2", "wait": False})
    assert t2.json()["queued"] is True

    r = client.post("/invoke", json={
        "function": "slow", "idempotency_key": "s3", "wait": False})
    assert r.status_code == 429
    assert r.json()["detail"]["code"] == "concurrency_limit_rejected"


def test_recover_endpoint_reports_rejection_reason(client, tmp_path):
    # 写入版本不兼容检查点，调用恢复端点应返回可区分原因
    from dfr.models import ExecutionRecord
    from dfr.storage import JsonStore

    bad = ExecutionRecord.new("bad-ver", "echo").to_dict()
    bad["schema_version"] = 42
    JsonStore(client.data_dir).write("bad-ver", bad)

    r = client.post("/recover")
    assert r.status_code == 200
    outcome = next(o for o in r.json()["outcomes"]
                   if o["idempotency_key"] == "bad-ver")
    assert outcome["recovered"] is False
    assert outcome["error_code"] == "checkpoint_version_incompatible"

    r2 = client.post("/recover/no-such-key")
    assert r2.status_code == 409
    assert r2.json()["detail"]["code"] == "checkpoint_missing"
