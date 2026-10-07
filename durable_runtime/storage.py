"""SQLite-backed persistence for execution records.

Every state transition and checkpoint is written through before the
runtime proceeds, so a process restart can reload the exact state.
"""
from __future__ import annotations

import json
import sqlite3
import threading

from .models import ErrorKind, ExecutionRecord, ExecutionStatus, RetryRecord

_SCHEMA = """
CREATE TABLE IF NOT EXISTS executions (
    idempotency_key   TEXT PRIMARY KEY,
    function_name     TEXT NOT NULL,
    payload           TEXT NOT NULL,
    status            TEXT NOT NULL,
    attempt           INTEGER NOT NULL,
    result            TEXT,
    error             TEXT,
    error_kind        TEXT,
    checkpoint_step   INTEGER NOT NULL,
    checkpoint_data   TEXT NOT NULL,
    checkpoint_version INTEGER NOT NULL,
    retry_history     TEXT NOT NULL,
    created_at        REAL NOT NULL,
    updated_at        REAL NOT NULL,
    deadline          REAL
);
"""


class Storage:
    """Durable store for execution records; survives process restarts."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._lock = threading.Lock()
        # check_same_thread=False: guarded by self._lock; enables use from
        # worker threads while keeping a single connection (single-writer).
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        with self._conn:
            self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def load(self, idempotency_key: str) -> ExecutionRecord | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM executions WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
        return self._to_record(row) if row else None

    def save(self, record: ExecutionRecord) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO executions VALUES (
                    :idempotency_key, :function_name, :payload, :status,
                    :attempt, :result, :error, :error_kind, :checkpoint_step,
                    :checkpoint_data, :checkpoint_version, :retry_history,
                    :created_at, :updated_at, :deadline
                )
                """,
                self._to_row(record),
            )

    def list_incomplete(self) -> list[ExecutionRecord]:
        """Records that were interrupted mid-flight (queued/running/retrying)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM executions WHERE status IN (?, ?, ?)",
                (
                    ExecutionStatus.QUEUED.value,
                    ExecutionStatus.RUNNING.value,
                    ExecutionStatus.FAILED_RETRYABLE.value,
                ),
            ).fetchall()
        return [self._to_record(r) for r in rows]

    def count_accepted(self, function_name: str) -> int:
        """Invocations of ``function_name`` that were admitted (not rejected).

        Rejected records never consumed capacity, so they do not count
        against the per-function quota.
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) FROM executions "
                "WHERE function_name = ? AND status != ?",
                (function_name, ExecutionStatus.REJECTED.value),
            ).fetchone()
        return int(row[0])

    @staticmethod
    def _to_row(rec: ExecutionRecord) -> dict:
        return {
            "idempotency_key": rec.idempotency_key,
            "function_name": rec.function_name,
            "payload": json.dumps(rec.payload),
            "status": rec.status.value,
            "attempt": rec.attempt,
            "result": json.dumps(rec.result),
            "error": rec.error,
            "error_kind": rec.error_kind.value if rec.error_kind else None,
            "checkpoint_step": rec.checkpoint_step,
            "checkpoint_data": json.dumps(rec.checkpoint_data),
            "checkpoint_version": rec.checkpoint_version,
            "retry_history": json.dumps([vars(r) for r in rec.retry_history]),
            "created_at": rec.created_at,
            "updated_at": rec.updated_at,
            "deadline": rec.deadline,
        }

    @staticmethod
    def _to_record(row: tuple) -> ExecutionRecord:
        keys = [
            "idempotency_key", "function_name", "payload", "status", "attempt",
            "result", "error", "error_kind", "checkpoint_step",
            "checkpoint_data", "checkpoint_version", "retry_history",
            "created_at", "updated_at", "deadline",
        ]
        d = dict(zip(keys, row))
        return ExecutionRecord(
            idempotency_key=d["idempotency_key"],
            function_name=d["function_name"],
            payload=json.loads(d["payload"]),
            status=ExecutionStatus(d["status"]),
            attempt=d["attempt"],
            result=json.loads(d["result"]),
            error=d["error"],
            error_kind=ErrorKind(d["error_kind"]) if d["error_kind"] else None,
            checkpoint_step=d["checkpoint_step"],
            checkpoint_data=json.loads(d["checkpoint_data"]),
            checkpoint_version=d["checkpoint_version"],
            retry_history=[RetryRecord(**r) for r in json.loads(d["retry_history"])],
            created_at=d["created_at"],
            updated_at=d["updated_at"],
            deadline=d["deadline"],
        )
