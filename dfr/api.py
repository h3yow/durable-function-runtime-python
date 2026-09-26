"""FastAPI HTTP 接口。

路由
----
GET  /health                        健康检查与运行配置
GET  /functions                     已注册函数列表
POST /invoke                        幂等调用
GET  /executions/{key}              查询执行（含状态迁移、重试记录）
POST /recover                       重启后恢复全部未完成执行
POST /recover/{key}                 恢复单个执行（缺失原因可区分）
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from dfr.demo import build_demo_registry
from dfr.errors import (
    CheckpointCorruptError,
    CheckpointMissingError,
    CheckpointVersionError,
    ConcurrencyLimitRejectedError,
    DFRError,
    QuotaExceededError,
)
from dfr.registry import FunctionRegistry
from dfr.runtime import DurableRuntime, RuntimeConfig

#: HTTP 状态码与平台错误码的映射（拒绝/失败原因可区分）
_STATUS_BY_CODE = {
    "function_not_found": 404,
    "concurrency_limit_rejected": 429,
    "queue_full": 429,
    "quota_exceeded": 429,
    "execution_in_progress": 409,
    "checkpoint_missing": 409,
    "checkpoint_version_incompatible": 422,
    "checkpoint_corrupt": 422,
}


class InvokeRequest(BaseModel):
    function: str
    idempotency_key: str
    payload: Any = None
    timeout: float | None = None
    deadline: float | None = None
    wait: bool = True
    suspend_before_step: int | None = None


def create_app(
    runtime: DurableRuntime,
    *,
    auto_recover: bool = False,
) -> FastAPI:
    """以给定运行时构建 ASGI 应用。"""

    app = FastAPI(
        title="本地持久化函数执行平台",
        version="0.1.0",
    )
    app.state.runtime = runtime

    if auto_recover:
        runtime.recover_all()

    def _raise_for(error: DFRError) -> None:
        raise HTTPException(
            status_code=_STATUS_BY_CODE.get(error.code, 400),
            detail={"code": error.code, "message": str(error)},
        )

    @app.get("/health")
    def health() -> dict[str, Any]:
        cfg = runtime.config
        return {
            "status": "ok",
            "data_dir": str(runtime.directory),
            "max_concurrency": cfg.max_concurrency,
            "max_queue": cfg.max_queue,
            "max_calls": cfg.max_calls,
        }

    @app.get("/functions")
    def list_functions() -> dict[str, Any]:
        return {"functions": runtime.registry.names()}

    @app.post("/invoke")
    def invoke(req: InvokeRequest) -> dict[str, Any]:
        try:
            result = runtime.invoke(
                req.function,
                req.idempotency_key,
                req.payload,
                timeout=req.timeout,
                deadline=req.deadline,
                wait=req.wait,
                suspend_before_step=req.suspend_before_step,
            )
        except (ConcurrencyLimitRejectedError, QuotaExceededError) as exc:
            _raise_for(exc)
            raise  # for type checker
        except DFRError as exc:  # 函数未注册等
            _raise_for(exc)
            raise
        return {
            "accepted": result.accepted,
            "replayed": result.replayed,
            "queued": result.queued,
            "reason": result.reason,
            "execution": result.record.to_dict(),
        }

    @app.get("/executions/{idempotency_key}")
    def get_execution(idempotency_key: str) -> dict[str, Any]:
        try:
            record = runtime.get(idempotency_key)
        except (CheckpointMissingError, CheckpointVersionError,
                CheckpointCorruptError) as exc:
            _raise_for(exc)
            raise
        if record is None:
            raise HTTPException(
                status_code=404,
                detail={
                    "code": "execution_not_found",
                    "message": f"找不到执行 {idempotency_key}",
                },
            )
        return {"execution": record.to_dict()}

    @app.post("/recover")
    def recover_all() -> dict[str, Any]:
        outcomes = runtime.recover_all()
        return {
            "outcomes": [
                {
                    "idempotency_key": o.idempotency_key,
                    "recovered": o.recovered,
                    "reason": o.reason,
                    "error_code": o.error_code,
                    "status": o.record.status if o.record else None,
                }
                for o in outcomes
            ]
        }

    @app.post("/recover/{idempotency_key}")
    def recover_one(idempotency_key: str) -> dict[str, Any]:
        try:
            outcome = runtime.recover_one(idempotency_key)
        except CheckpointMissingError as exc:
            _raise_for(exc)
            raise
        return {
            "idempotency_key": outcome.idempotency_key,
            "recovered": outcome.recovered,
            "reason": outcome.reason,
            "error_code": outcome.error_code,
            "execution": outcome.record.to_dict() if outcome.record else None,
        }

    return app


def build_default_app(
    data_dir: str = "data",
    config: RuntimeConfig | None = None,
    *,
    auto_recover: bool = True,
) -> FastAPI:
    """main.py 使用的默认应用：演示函数 + 启动自动恢复。"""
    registry = build_demo_registry(data_dir)
    runtime = DurableRuntime(data_dir, registry, config or RuntimeConfig())
    return create_app(runtime, auto_recover=auto_recover)
