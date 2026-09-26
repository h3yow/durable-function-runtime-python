"""一键本地验证：幂等重放、退避重试、失败终态、并发排队/拒绝、重启恢复。

用法: uv run python scripts/demo.py
数据写入临时目录，脚本结束后打印全部结论；恢复场景用“新建 runtime 指向同一
目录”模拟进程重启。
"""

from __future__ import annotations

import tempfile
import threading
import time
from pathlib import Path

from dfr.backoff import RetryPolicy
from dfr.demo import build_demo_registry
from dfr.errors import ConcurrencyLimitRejectedError
from dfr.runtime import DurableRuntime, RuntimeConfig


def hr(title: str) -> None:
    print(f"\n{'=' * 64}\n{title}\n{'=' * 64}")


def main() -> None:
    data_dir = Path(tempfile.mkdtemp(prefix="dfr-demo-"))
    print(f"数据目录: {data_dir}")
    registry = build_demo_registry(data_dir)
    policy = RetryPolicy(max_attempts=4, base_delay=0.05, multiplier=2)
    config = RuntimeConfig(max_concurrency=2, max_queue=2)
    rt = DurableRuntime(data_dir, registry, config, policy)

    hr("1) 可重试错误退避后成功 + 重复调用幂等")
    r1 = rt.invoke("flaky_then_append", "order-1", payload="A", wait=True)
    print("首次:", r1.record.status, "=>", r1.record.result,
          " 重试次数:", len(r1.record.retries))
    r2 = rt.invoke("flaky_then_append", "order-1", payload="A", wait=True)
    print("重复:", r2.record.status, "=>", r2.record.result,
          " replayed:", r2.replayed, " 重试次数:", len(r2.record.retries))

    hr("2) 重试耗尽进入失败终态（可查询）")
    r3 = rt.invoke("always_failing", "order-2", wait=True)
    print("终态:", r3.record.status, " error_code:", r3.record.error_code,
          " 尝试:", r3.record.attempts)
    q = rt.get("order-2")
    print("查询终态:", q.status, "最后迁移:", q.transitions[-1].reason)

    hr("3) 不可重试错误立即失败")
    r4 = rt.invoke("fatal", "order-3", wait=True)
    print("终态:", r4.record.status, " error_code:", r4.record.error_code,
          " 尝试:", r4.record.attempts)

    hr("4) 并发背压：排队与有界拒绝（原因码可区分）")
    gate = threading.Event()

    def slow_blocker(ctx):
        gate.wait(5)
        return "ok"

    registry.define("blocker", [slow_blocker])
    outcomes: list[str] = []

    def hit(key):
        try:
            r = rt.invoke("blocker", key, wait=False)
            outcomes.append(f"{key}:{r.reason}")
        except ConcurrencyLimitRejectedError as exc:
            outcomes.append(f"{key}:REJECTED({exc.code})")

    threads = [threading.Thread(target=hit, args=(f"c{i}",))
               for i in range(5)]
    for t in threads:
        t.start()
    time.sleep(0.3)
    gate.set()
    for t in threads:
        t.join()
    for line in sorted(outcomes):
        print(" ", line)
    # 等待后台任务真正结束、释放执行槽，避免影响后续恢复演示
    deadline = time.time() + 5
    while time.time() < deadline:
        if all(rt.get(f"c{i}").status == "SUCCEEDED"
               for i in (0, 1, 2, 3)):
            break
        time.sleep(0.02)

    hr("5) 进程重启恢复（新 runtime 指向同一目录）")
    r5 = rt.invoke("echo_then_append", "order-4", payload="R", wait=True,
                   suspend_before_step=1)
    print("挂起:", r5.record.status, "next_step:", r5.record.next_step)
    rt.shutdown()
    rt2 = DurableRuntime(data_dir, registry, config, policy)
    for o in rt2.recover_all():
        print(" 恢复:", o.idempotency_key, "recovered:", o.recovered,
              "reason:", o.reason)
    rec = rt2.get("order-4")
    print("恢复后:", rec.status, "=>", rec.result,
          " 迁移:", [(t.from_status, t.to_status) for t in rec.transitions])
    rt2.shutdown()

    print(f"\n演示数据保留在: {data_dir}（可手动删除该临时目录）")


if __name__ == "__main__":
    main()
