# Durable Function Runtime（本地可运行的函数执行平台）

以本地 SQLite 持久化模拟服务端状态，提供函数的注册、幂等调用、退避重试、
检查点恢复、并发背压、单次调用时间预算、单步超时与主动撤回。
进程重启与重复请求下，结果确定且可解释。

## 架构

```
main.py                 入口：注册演示函数并启动 HTTP 服务
durable_runtime/
  models.py             状态机、错误分类、重试策略、执行记录
  storage.py            SQLite 持久化（WAL，单连接+锁，事务即落盘）
  retry.py              指数退避计算
  scheduler.py          有界并发 + 有界队列（超限拒绝，拒绝原因可区分）
  runtime.py            核心：幂等调用、检查点步骤、恢复校验、预算/撤回
  api.py                FastAPI 接口（/invoke、/executions/{key}、
                        /executions/{key}/cancel、/admin/recover）
tests/                  32 项测试（幂等/重试/恢复/并发/超时/预算/撤回/拒绝/HTTP）
```

## 执行与恢复模型

- 每次调用由**幂等键**（`idempotency_key`）唯一标识，对应一条持久化的
  `ExecutionRecord`，每次状态迁移与检查点都先落盘再推进。
- 用户函数签名为 `fn(ctx, payload)`，通过 `ctx.step(name, fn)` 执行**持久步骤**：
  步骤结果立即写入检查点；重试或重启后重放时，已完成的步骤直接返回存储值，
  **不会重复执行**，只有缺失的步骤会真正运行。
- 状态机：`queued → running → succeeded`，失败时经
  `failed_retryable → running` 循环，终态为
  `succeeded / failed / rejected / timed_out / cancelled`，彼此可区分。
- 进程重启后调用 `recover()`（HTTP 服务启动时自动执行）：
  - 扫描所有非终态记录并从中断处恢复（`cancelled` 等终态不会被恢复续跑）；
  - 检查点不可恢复时**明确拒绝**并落入可查询的 `failed` 终态，原因可区分：
    - `resume_unknown_function`：函数未注册；
    - `resume_version_mismatch`：检查点版本与运行时不兼容；
    - `resume_checkpoint_missing`：步骤索引缺少对应检查点值。
  - 绝不静默从头重跑，也不跳过步骤。

## 时间预算与单步超时

- **单次调用预算**：`invoke(..., timeout=秒)`（HTTP：`POST /invoke` 的
  `timeout` 字段）为这一次调用指定自己的时间预算；省略时回落到全局
  `execution_timeout` 默认值。预算从受理时刻起算，**排队等待、退避重试等待
  和执行过程全部计入**（截止时间是绝对时刻）。
- **单步超时**：`ctx.step(name, fn, timeout=秒)` 给该步骤单独的时间上限。
  实际生效的上限是「单步上限」与「整体预算剩余」两者的较小值。
- 步骤超时按**可重试失败**处理：该步骤不写入检查点，按指数退避重试，
  重试时已完成的步骤从检查点重放、不会重跑，只有超时的步骤会重新执行；
  退避等待同样受整体预算约束（等待会被截断到预算剩余时间）。
- 预算耗尽后，执行在步骤边界（或退避等待结束时）收敛到可查询的终态
  `timed_out`，不会停留在「执行中」不了了之。超步骤本身运行在守护线程上，
  被丢弃的结果不会写入检查点（Python 无法强杀线程，失控步骤的残余线程
  不影响状态正确性）。

## 主动撤回

- `runtime.cancel(key)`（HTTP：`POST /executions/{key}/cancel`）撤回一个
  还在排队或正在执行的调用。
- 语义：
  - **幂等**：重复撤回返回同一结论；对已是终态的调用撤回是空操作，
    不会改写既有结论（已 `succeeded` 的调用撤回后仍是 `succeeded`）；
  - 撤回成功落入终态 `cancelled`，与 `failed` / `timed_out` / `rejected`
    明确区分；
  - 撤回后剩余步骤不再推进：worker 在每个检查点/状态迁移边界以及步骤等待、
    退避等待期间都会观察撤回标记（长函数也可用 `ctx.cancelled()` 主动轮询）；
  - `cancelled` 是终态，进程重启后不会被 `recover()` 恢复续跑。
- 一致性：撤回与 worker 的状态持久化在同一把锁下串行化，worker 不会用
  过期的进行中状态覆盖已落盘的 `cancelled`。

## 幂等与重试语义

- 同一幂等键的重复调用：
  - 执行**完成后**到达 → 直接返回首次调用的持久化结论，不重跑；
  - 执行**进行中**并发到达 → 共享同一次执行（进程内 future 去重），
    所有调用方得到同一条记录；
  - 因崩溃遗留的非终态记录 → 触发断点恢复而非重新执行；
  - 曾被**拒绝**（`rejected`）的记录 → 拒绝是瞬态的（见下节），
    同一幂等键会重新走受理流程，而不是被永久判死。
- 错误分类：
  - 抛出 `RetryableError`（或未预期异常、步骤超时）→ 可重试，按指数退避
    （`base_delay * multiplier^(n-1)`，封顶 `max_delay`）重试，
    超过 `max_attempts` 后进入终态 `failed`（`error_kind=retryable`）；
  - 抛出 `NonRetryableError` → 立即终态 `failed`（`error_kind=non_retryable`）；
  - 预算/截止时间耗尽 → 终态 `timed_out`，不会被恢复重跑。
- 每次重试决策都记录在 `retry_history`（attempt、error、delay），随记录持久化。

## 并发、背压与拒绝

- `max_concurrency`：同时运行的执行数上限；
- `max_queue`：已接受但排队等待的执行数上限；
- 超出 `max_concurrency + max_queue` 的调用被**同步拒绝**：
  记录落为终态 `rejected`，`error=rejected:queue_full`（HTTP 返回 429）；
- `quotas={函数名: 上限}`：按函数限制累计受理次数，超出的调用被拒绝，
  `error=rejected:quota_exceeded`——**容量不足**（`queue_full`，瞬时的
  排队资源不够）与**配额用尽**（`quota_exceeded`，该函数的受理额度耗尽）
  是两种可区分的拒绝原因；
- **拒绝是瞬态的**：没被受理的调用从不占用排队名额（拒绝发生在分配排队
  许可之前），也不影响已受理的调用；因此 `rejected` 不代表这个幂等键被
  判死——容量恢复（或配额调整后）用同一幂等键重新提交，会重新走受理流程
  并给出真正的结论；
- 拒绝不影响已接受的调用：排队记录已持久化，worker 释放后照常执行。

配置方式（`DurableRuntime` / `create_app` 参数）：

| 参数 | 默认 | 含义 |
| --- | --- | --- |
| `max_concurrency` | 4 | 并发执行上限 |
| `max_queue` | 16 | 有界排队长度 |
| `execution_timeout` | 30s | 单次调用预算默认值（可被 `invoke(timeout=...)` 覆盖） |
| `retry_policy` | 3 次 / 0.1s 起 / ×2 / 封顶 5s | 重试策略 |
| `checkpoint_version` | 1 | 检查点兼容版本 |
| `quotas` | 无 | 按函数的受理配额（`{"fn": 次数}`） |

## 与既有模型的兼容范围

- 幂等键语义不变：只有 `rejected` 这一种终态允许同键重新提交（因为它从未
  被受理）；`succeeded/failed/timed_out/cancelled` 重复提交仍返回原结论。
- 恢复模型不变：`recover()` 只扫描 `queued/running/failed_retryable`，
  新增的 `cancelled` 与既有终态一样不会被恢复；检查点版本校验、三类
  恢复拒绝原因保持不变。
- 重试语义不变：步骤超时只是新增一种可重试失败来源，退避算法与
  `retry_history` 记录格式不变；已完成步骤不重跑的保证不变。
- 既有调用方不传 `timeout` / 不调用 `cancel` 时，行为与之前完全一致
  （全局 `execution_timeout` 兜底）。

## 本地验证

```bash
# 运行测试（日志会打印调用标识、状态迁移与重试记录）
uv run pytest -q

# 启动服务（默认 runtime.db，可用 RUNTIME_DB 覆盖）
uv run python main.py

# 调用（幂等键重复提交返回同一结论）
curl -s -X POST localhost:8000/invoke -H 'content-type: application/json' \
  -d '{"function":"add","idempotency_key":"k1","payload":{"a":1,"b":2}}'

# 单次调用预算：这次调用 1 秒内必须收敛（否则终态 timed_out）
curl -s -X POST localhost:8000/invoke -H 'content-type: application/json' \
  -d '{"function":"slow","idempotency_key":"k2","payload":{"seconds":60},"timeout":1}'

# 主动撤回（排队中或执行中均可；重复调用幂等）
curl -s -X POST localhost:8000/executions/k2/cancel

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
- `test_budget_cancel_reject.py`：单次预算覆盖全局默认、单步超时重试并
  收敛（已完成步骤不重跑）、预算优先于单步上限、退避等待计入预算、
  撤回排队中/执行中的调用、撤回幂等且不改写终态、撤回后重启不恢复、
  容量恢复后同一幂等键重新受理、拒绝不占排队名额、配额与容量两类拒绝
  原因可区分、四类终态可区分；
- `test_api.py`：HTTP 层端到端（调用、查询、幂等、404、恢复端点、
  单次预算、撤回端点）。
