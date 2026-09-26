"""FastAPI surface for the durable runtime.

Endpoints:
  POST /functions            register a demo/builtin function is not
                             possible over HTTP; functions are registered
                             in code.  This API exposes invocation & query.
  POST /invoke               body: {function, idempotency_key, payload}
  GET  /executions/{key}     query the current/terminal state
  POST /admin/recover        rescan interrupted executions (also runs at startup)
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .models import ExecutionRecord, ExecutionStatus, RetryPolicy
from .runtime import DurableRuntime
from .scheduler import RejectedError
from .storage import Storage

logger = logging.getLogger("durable_runtime")


class InvokeRequest(BaseModel):
    function: str
    idempotency_key: str
    payload: dict = {}
    wait_timeout: float | None = 30.0


def _record_view(rec: ExecutionRecord) -> dict:
    return {
        "idempotency_key": rec.idempotency_key,
        "function": rec.function_name,
        "status": rec.status.value,
        "attempt": rec.attempt,
        "result": rec.result,
        "error": rec.error,
        "error_kind": rec.error_kind.value if rec.error_kind else None,
        "checkpoint_step": rec.checkpoint_step,
        "retry_history": [vars(r) for r in rec.retry_history],
    }


def create_app(db_path: str = "runtime.db",
               retry_policy: RetryPolicy | None = None,
               max_concurrency: int = 4,
               max_queue: int = 16,
               execution_timeout: float | None = 30.0,
               checkpoint_version: int = 1) -> FastAPI:
    runtime = DurableRuntime(
        Storage(db_path), retry_policy=retry_policy,
        max_concurrency=max_concurrency, max_queue=max_queue,
        execution_timeout=execution_timeout,
        checkpoint_version=checkpoint_version)

    @asynccontextmanager
    async def _lifespan(app: FastAPI):
        affected = runtime.recover()
        logger.info("startup recovery: %d execution(s) resumed", len(affected))
        yield
        runtime.shutdown()

    app = FastAPI(title="durable-function-runtime", lifespan=_lifespan)
    app.state.runtime = runtime

    @app.post("/invoke", status_code=200)
    def invoke(req: InvokeRequest) -> dict:
        try:
            rec = runtime.invoke(req.function, req.idempotency_key,
                                 req.payload, wait_timeout=req.wait_timeout)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except RejectedError as exc:
            # 429 + distinguishable reason; the record is queryable as REJECTED
            raise HTTPException(status_code=429,
                                detail=f"rejected:{exc.reason.value}")
        return _record_view(rec)

    @app.get("/executions/{key}")
    def get_execution(key: str) -> dict:
        rec = runtime.get(key)
        if rec is None:
            raise HTTPException(status_code=404, detail="unknown idempotency key")
        return _record_view(rec)

    @app.post("/admin/recover")
    def recover() -> dict:
        affected = runtime.recover()
        return {"resumed": [r.idempotency_key for r in affected]}

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    return app
