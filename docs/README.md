# Durable Function Runtime（本地可运行的函数执行平台）

以本地 SQLite 持久化模拟服务端状态，提供函数的注册、幂等调用、退避重试、
检查点恢复、并发背压与超时终态。进程重启与重复请求下，结果确定且可解释。

## 架构

```
main.py                 入口：注册演示函数并启动 HTTP 服务
durable_runtime/
  models.py             状态机、错误分类、重试策略、执行记录
  storage.py            SQLite 持久化（WAL，单连接+锁，事务即落盘）
  retry.py              指数退避计算
  scheduler.py          有界并发 + 有界队列（超限拒绝）
  runtime.py            核心：幂等调用、检查点步骤、恢复校验
  api.py                FastAPI 接口（/invoke、/executions/{key}、/admin/recover）
tests/                  18 项测试（幂等/重试/恢复/并发/超时/HTTP）
```

## 执行与恢复模型

- 每次调用由**幂等键**（`idempotency_key`）唯一标识，对应一条持久化的
  `ExecutionRecord`，每次状态迁移与检查点都先落盘再推进。
- 用户函数签名为 `fn(ctx, payload)`，通过 `ctx.step(name, fn)` 执行**持久步骤**：
  步骤结果立即写入检查点；重试或重启后重放时，已完成的步骤直接返回存储值，
  **不会重复执行**，只有缺失的步骤会真正运行。
- 状态机：`queued → running → succeeded`，失败时经
  `failed_retryable → running` 循环，终态为
  `succeeded / failed / rejected / timed_out`。
- 进程重启后调用 `recover()`（HTTP 服务启动时自动执行）：
  - 扫描所有非终态记录并从中断处恢复；
  - 检查点不可恢复时**明确拒绝**并落入可查询的 `failed` 终态，原因可区分：
    - `resume_unknown_function`：函数未注册；
    - `resume_version_mismatch`：检查点版本与运行时不兼容；
    - `resume_checkpoint_missing`：步骤索引缺少对应检查点值。
  - 绝不静默从头重跑，也不跳过步骤。

## 幂等与重试语义

- 同一幂等键的重复调用：
  - 执行**完成后**到达 → 直接返回首次调用的持久化结论，不重跑；
  - 执行**进行中**并发到达 → 共享同一次执行（进程内 future 去重），
    所有调用方得到同一条记录；
  - 因崩溃遗留的非终态记录 → 触发断点恢复而非重新执行。
- 错误分类：
  - 抛出 `RetryableError`（或未预期异常）→ 可重试，按指数退避
    （`base_delay * multiplier^(n-1)`，封顶 `max_delay`）重试，
    超过 `max_attempts` 后进入终态 `failed`（`error_kind=retryable`）；
  - 抛出 `NonRetryableError` → 立即终态 `failed`（`error_kind=non_retryable`）；
  - 超过 `execution_timeout` 截止时间 → 终态 `timed_out`，不会被恢复重跑。
- 每次重试决策都记录在 `retry_history`（attempt、error、delay），随记录持久化。

## 并发与背压

- `max_concurrency`：同时运行的执行数上限；
- `max_queue`：已接受但排队等待的执行数上限；
- 超出 `max_concurrency + max_queue` 的调用被**同步拒绝**：
  记录落为终态 `rejected`，`error=rejected:queue_full`（HTTP 返回 429），
  与排队等待（保持 `queued`）可区分；
- 拒绝不影响已接受的调用：排队记录已持久化，worker 释放后照常执行。

配置方式（`DurableRuntime` / `create_app` 参数）：

| 参数 | 默认 | 含义 |
| --- | --- | --- |
| `max_concurrency` | 4 | 并发执行上限 |
| `max_queue` | 16 | 有界排队长度 |
| `execution_timeout` | 30s | 单次执行截止时间 |
| `retry_policy` | 3 次 / 0.1s 起 / ×2 / 封顶 5s | 重试策略 |
| `checkpoint_version` | 1 | 检查点兼容版本 |

## 本地验证

```bash
# 运行测试（日志会打印调用标识、状态迁移与重试记录）
uv run pytest -q

# 启动服务（默认 runtime.db，可用 RUNTIME_DB 覆盖）
uv run python main.py

# 调用（幂等键重复提交返回同一结论）
curl -s -X POST localhost:8000/invoke -H 'content-type: application/json' \
  -d '{"function":"add","idempotency_key":"k1","payload":{"a":1,"b":2}}'

# 查询终态 / 重试历史
curl -s localhost:8000/executions/k1

# 模拟崩溃恢复：执行 slow 函数期间 kill 进程，重启后自动从检查点恢复
# （或手动触发）：
curl -s -X POST localhost:8000/admin/recover
```

## 测试覆盖

- `test_idempotency_retry.py`：重复调用（串行/并发）、退避重试、
  重试耗尽终态、不可重试错误；
- `test_recovery.py`：重启后从检查点恢复且已完成步骤不重跑、
  版本不兼容 / 检查点缺失 / 函数未注册三类拒绝原因、
  重试中断后的续跑；
- `test_concurrency_timeout.py`：并发上限、队列溢出拒绝（原因可区分、
  已接受调用不丢失）、超时终态及其不可恢复性；
- `test_api.py`：HTTP 层端到端（调用、查询、幂等、404、恢复端点）。
