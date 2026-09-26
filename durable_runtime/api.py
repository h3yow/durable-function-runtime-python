"""FastAPI surface for the durable runtime (skeleton)."""
from __future__ import annotations

from fastapi import FastAPI


def create_app(db_path: str = "runtime.db") -> FastAPI:
    app = FastAPI(title="durable-function-runtime")
    return app
