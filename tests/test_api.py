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
    rt.register("nap", lambda ctx, p: ctx.step(
        "sleep", lambda: time.sleep(float(p.get("seconds", 1)))))
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


def test_per_call_budget_via_http(client):
    r = client.post("/invoke", json={
        "function": "nap", "idempotency_key": "http-budget",
        "payload": {"seconds": 2}, "timeout": 0.2})
    assert r.status_code == 200
    assert r.json()["status"] == "timed_out"


def test_cancel_endpoint(client):
    done = {}

    def call():
        done["resp"] = client.post("/invoke", json={
            "function": "nap", "idempotency_key": "http-cancel",
            "payload": {"seconds": 5}})

    t = threading.Thread(target=call)
    t.start()
    time.sleep(0.3)  # let the invocation start running

    r = client.post("/executions/http-cancel/cancel")
    assert r.status_code == 200
    assert r.json()["status"] == "cancelled"

    t.join(5)
    assert done["resp"].json()["status"] == "cancelled"

    # repeated cancel is idempotent; unknown key is a 404
    assert client.post("/executions/http-cancel/cancel").json()["status"] == "cancelled"
    assert client.post("/executions/nope/cancel").status_code == 404
