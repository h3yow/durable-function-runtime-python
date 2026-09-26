"""Entry point: run the durable function runtime HTTP server locally.

Registers a few demo functions so the execution/recovery model can be
exercised end-to-end with curl (see docs/README.md).
"""
import logging
import os
import time

import uvicorn

from durable_runtime.api import create_app
from durable_runtime.models import NonRetryableError, RetryableError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s")

app = create_app(db_path=os.environ.get("RUNTIME_DB", "runtime.db"))
runtime = app.state.runtime


def add(ctx, payload):
    """Two-step durable computation; each step is checkpointed."""
    a = ctx.step("parse-a", lambda: int(payload["a"]))
    b = ctx.step("parse-b", lambda: int(payload["b"]))
    return a + b


def flaky(ctx, payload):
    """Fails retryably until `succeed_after` attempts, then succeeds."""
    n = ctx.step("attempt-counter", lambda: 0)  # durable across restarts
    attempt = ctx._record.attempt  # demo only: observe current attempt
    if attempt < int(payload.get("succeed_after", 2)):
        raise RetryableError(f"transient failure at attempt {attempt}")
    return {"ok": True, "attempts": attempt}


def validate(ctx, payload):
    """Non-retryable failure demo."""
    if "name" not in payload:
        raise NonRetryableError("payload.name is required")
    return {"hello": payload["name"]}


def slow(ctx, payload):
    """Long-running step; exceeds the deadline -> TIMED_OUT."""
    ctx.step("sleep", lambda: time.sleep(float(payload.get("seconds", 60))))
    return "done"


runtime.register("add", add)
runtime.register("flaky", flaky)
runtime.register("validate", validate)
runtime.register("slow", slow)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
