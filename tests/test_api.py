"""End-to-end HTTP API tests via FastAPI TestClient."""
import threading
import time

import pytest
from fastapi.testclient import TestClient

from durable_runtime.api import create_app
from durable_runtime.models import RetryableError


@pytest.fixture()
def client(tmp_path):
    app = create_app(db_path=str(tmp_path / "api.db"), execution_timeout=10.0)
    rt = app.state.runtime
    rt.register("echo", lambda ctx, p: {"echo": p})
    attempts = {"n": 0}

    def flaky(ctx, p):
        attempts["n"] += 1
        if attempts["n"] < 2:
            raise RetryableError("try again")
        return "ok"

    rt.register("flaky", flaky)
    with TestClient(app) as c:
        yield c


def test_invoke_and_query(client):
    r = client.post("/invoke", json={
        "function": "echo", "idempotency_key": "http-1",
        "payload": {"x": 1}})
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "succeeded"
    assert body["result"] == {"echo": {"x": 1}}

    q = client.get("/executions/http-1")
    assert q.status_code == 200 and q.json()["result"] == {"echo": {"x": 1}}


def test_duplicate_invoke_is_idempotent(client):
    a = client.post("/invoke", json={
        "function": "flaky", "idempotency_key": "http-2"}).json()
    b = client.post("/invoke", json={
        "function": "flaky", "idempotency_key": "http-2"}).json()
    assert a["status"] == b["status"] == "succeeded"
    assert a["attempt"] == b["attempt"] == 2
    assert len(b["retry_history"]) == 1


def test_unknown_function_and_key(client):
    r = client.post("/invoke", json={
        "function": "nope", "idempotency_key": "http-3"})
    assert r.status_code == 404
    assert client.get("/executions/nope").status_code == 404


def test_admin_recover_endpoint(client):
    r = client.post("/admin/recover")
    assert r.status_code == 200 and r.json() == {"resumed": []}


def test_per_call_timeout_budget_over_http(client):
    rt = client.app.state.runtime
    rt.register("sleepy", lambda ctx, p: ctx.step("nap", lambda: time.sleep(1.0)))
    r = client.post("/invoke", json={
        "function": "sleepy", "idempotency_key": "http-budget",
        "timeout": 0.3})
    assert r.status_code == 200
    assert r.json()["status"] == "timed_out"


def test_cancel_endpoint(client):
    rt = client.app.state.runtime
    entered = threading.Event()
    release = threading.Event()

    def slow(ctx, p):
        entered.set()
        release.wait(5)
        return "done"

    rt.register("slow", slow)
    outcome = {}

    def call():
        outcome["resp"] = client.post("/invoke", json={
            "function": "slow", "idempotency_key": "http-cancel"})

    t = threading.Thread(target=call)
    t.start()
    assert entered.wait(5)

    r = client.post("/executions/http-cancel/cancel")
    assert r.status_code == 200 and r.json()["status"] == "cancelled"
    # repeated cancel is idempotent
    r2 = client.post("/executions/http-cancel/cancel")
    assert r2.status_code == 200 and r2.json()["status"] == "cancelled"

    release.set()
    t.join()
    assert outcome["resp"].json()["status"] == "cancelled"
    assert client.get("/executions/http-cancel").json()["status"] == "cancelled"
    # unknown key -> 404
    assert client.post("/executions/nope/cancel").status_code == 404


def test_quota_rejection_over_http(tmp_path):
    app = create_app(db_path=str(tmp_path / "quota.db"), invocation_quota=1)
    app.state.runtime.register("echo", lambda ctx, p: "ok")
    with TestClient(app) as c:
        assert c.post("/invoke", json={
            "function": "echo", "idempotency_key": "q-1"}).status_code == 200
        r = c.post("/invoke", json={
            "function": "echo", "idempotency_key": "q-2"})
        assert r.status_code == 429
        assert r.json()["detail"] == "rejected:quota_exhausted"
        # distinguishable, queryable terminal state
        assert c.get("/executions/q-2").json()["status"] == "rejected"
