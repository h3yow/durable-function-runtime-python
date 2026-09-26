import logging

import pytest

from durable_runtime.models import RetryPolicy
from durable_runtime.runtime import DurableRuntime
from durable_runtime.storage import Storage


@pytest.fixture()
def db_path(tmp_path):
    return str(tmp_path / "runtime.db")


@pytest.fixture()
def make_runtime(db_path):
    """Factory: each call simulates a fresh process over the same DB."""
    runtimes = []

    def _make(**kwargs):
        kwargs.setdefault("retry_policy",
                          RetryPolicy(max_attempts=3, base_delay_seconds=0.01,
                                      backoff_multiplier=2.0))
        kwargs.setdefault("execution_timeout", 10.0)
        rt = DurableRuntime(Storage(db_path), **kwargs)
        runtimes.append(rt)
        return rt

    yield _make
    for rt in runtimes:
        rt.shutdown()


@pytest.fixture(autouse=True)
def _log_to_stdout(caplog):
    """Surface runtime logs (invocation ids, transitions, retries) in output."""
    caplog.set_level(logging.INFO, logger="durable_runtime")
