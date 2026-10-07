# Durable Function Runtime（本地可运行的函数执行平台）

以本地 SQLite 持久化模拟服务端状态，提供函数的注册、幂等调用、退避重试、
检查点恢复、并发背压、时间预算、主动撤回与可区分的拒绝/终态。
进程重启与重复请求下，结果确定且可解释。

## 架构

```
main.py                 入口：注册演示函数并启动 HTTP 服务
durable_runtime/
  models.py             状态机、错误分类、重试策略、执行记录
  storage.py            SQLite 持久化（WAL，单连接+锁，事务即落盘）
  retry.py              指数退避计算
  scheduler.py          有界并发 + 有界队列（超限拒绝，原因可区分）
  runtime.py            核心：幂等调用、检查点步骤、恢复校验、预算/撤回
  api.py                FastAPI 接口（/invoke、/executions/{key}、
                        /executions/{key}/cancel、/admin/recover）
tests/                  32 项测试（幂等/重试/恢复/并发/预算/撤回/拒绝/HTTP）
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
- 终态是结论：运行时的状态迁移不会把记录 silently 移出终态
  （例如 worker 与撤回竞态时不会覆盖已落盘的 `cancelled`）。
- 进程重启后调用 `recover()`（HTTP 服务启动时自动执行）：
  - 扫描所有非终态记录并从中断处恢复；
  - 检查点不可恢复时**明确拒绝**并落入可查询的 `failed` 终态，原因可区分：
    - `resume_unknown_function`：函数未注册；
    - `resume_version_mismatch`：检查点版本与运行时不兼容；
    - `resume_checkpoint_missing`：步骤索引缺少对应检查点值。
  - 绝不静默从头重跑，也不跳过步骤；
  - 终态记录（含 `cancelled`、`timed_out`）**不会被恢复重跑**。

## 时间预算与单步超时

- 每次调用有一个**时间预算**：`invoke(..., timeout=...)` 按调用指定，
  缺省回落到全局 `execution_timeout`。预算在受理时折算成持久化的
  `deadline`，因此**排队等待、退避重试等待、执行过程**都计入同一份预算。
- `ctx.step(name, fn, timeout=...)` 给单个步骤设置时间上限。步骤超过上限时
  按**可重试失败**处理：该步骤结果不写入检查点，整个 attempt 走指数退避重试；
  已完成的步骤因检查点存在**不会被重跑**。
- 预算对重试的约束：
  - 退避睡眠会被截断到剩余预算内，不会睡过 deadline；
  - 每次 attempt 开始前、每个步骤前后、函数返回后都检查 deadline；
  - 预算耗尽后执行在有界时间内收敛到可查询的终态 `timed_out`，
    不会停留在 `running`/`failed_retryable` 无限重试。
- 单步超时与整体预算的关系：单步上限决定"这一步要不要重来"，
  整体预算决定"还要不要继续重来"——重试次数再多，预算一到就收敛。

## 主动撤回（cancel）

- `runtime.cancel(key)` / `POST /executions/{key}/cancel` 撤回一个
  **排队中或执行中**的调用，落入可区分的终态 `cancelled`：
  - 撤回决定**先落盘**（因此进程重启后不会被 `recover()` 重新拉起），
    再通知进程内执行协作式停止；
  - 执行中的函数在下一个步骤边界停止，剩余步骤不再推进；
  - 退避等待可被撤回立即打断，收敛不需要等完整个 backoff；
  - 对未知键返回 404 / `None`。
- **幂等**：重复撤回返回同一条 `cancelled` 结论；对已终态（如 `succeeded`）
  的调用撤回是 no-op，返回既有结论，不会改写。
- `cancelled` 与 `failed`（重试耗尽/不可重试错误）、`timed_out`（预算耗尽）、
  `rejected`（未受理）是四个不同的终态，可分别查询与统计。

## 幂等与重试语义

- 同一幂等键的重复调用：
  - 执行**完成后**到达 → 直接返回首次调用的持久化结论，不重跑；
  - 执行**进行中**并发到达 → 共享同一次执行（进程内 future 去重），
    所有调用方得到同一条记录；
  - 因崩溃遗留的非终态记录 → 触发断点恢复而非重新执行；
  - 唯一例外是 `rejected`（见下节）：拒绝是瞬态的，同键可重新提交。
- 错误分类：
  - 抛出 `RetryableError`（或未预期异常、单步超时）→ 可重试，按指数退避
    （`base_delay * multiplier^(n-1)`，封顶 `max_delay`）重试，
    超过 `max_attempts` 后进入终态 `failed`（`error_kind=retryable`）；
  - 抛出 `NonRetryableError` → 立即终态 `failed`（`error_kind=non_retryable`）；
  - 超过预算截止时间 → 终态 `timed_out`，不会被恢复重跑。
- 每次重试决策都记录在 `retry_history`（attempt、error、delay），随记录持久化；
  退避等待被预算截断时记录的是实际等待时长。

## 并发、背压与拒绝

- `max_concurrency`：同时运行的执行数上限；
- `max_queue`：已接受但排队等待的执行数上限；
- `invocation_quota`（可选）：本运行时累计**受理**的调用数上限；
- 两类拒绝原因可区分（HTTP 均返回 429，detail 不同）：
  - `rejected:queue_full`：**容量不足**——当前并发+排队已满，是瞬态的；
  - `rejected:quota_exhausted`：**配额用尽**——累计受理额度已耗尽。
- **拒绝是瞬态的**：被拒绝的调用只是"这一次没排上"，
  不会把幂等键永久判死。记录落为可查询的 `rejected`（原因可区分），
  但同一幂等键之后重新提交会被当作**全新调用**正常受理并给出结论
  （attempt、检查点、重试历史全部重置——它本来就没有执行过任何步骤）。
- 没被受理的调用不占排队名额（调度器许可未获取即拒绝）、不计入配额
  （`count_accepted` 排除 `rejected`），也不影响已受理的调用。

## 与既有模型的兼容范围

- 幂等键语义不变：已受理的调用仍然 exactly-once 结论可查；
  只有 `rejected` 这一种"从未受理"的状态允许同键重提。
- 恢复模型不变：`recover()` 只扫描非终态记录；新增的 `cancelled`
  与既有 `timed_out` 一样是终态，不会被恢复。
- 检查点格式不变：`checkpoint_version` 语义不变，单步超时只是
  "不落检查点 + 可重试失败"，旧记录无需迁移。
- 全局 `execution_timeout` 仍是缺省预算；不传 `timeout` 的调用行为与之前一致。

## 配置方式（`DurableRuntime` / `create_app` 参数）

| 参数 | 默认 | 含义 |
| --- | --- | --- |
| `max_concurrency` | 4 | 并发执行上限 |
| `max_queue` | 16 | 有界排队长度 |
| `execution_timeout` | 30s | 缺省时间预算（可被每次调用的 `timeout` 覆盖） |
| `invocation_quota` | 无限制 | 累计受理配额（超限拒绝 `quota_exhausted`） |
| `retry_policy` | 3 次 / 0.1s 起 / ×2 / 封顶 5s | 重试策略 |
| `checkpoint_version` | 1 | 检查点兼容版本 |

## 本地验证

```bash
# 运行测试（日志会打印调用标识、状态迁移与重试记录）
uv run pytest -q

# 启动服务（默认 runtime.db，可用 RUNTIME_DB 覆盖）
uv run python main.py

# 调用（幂等键重复提交返回同一结论；timeout 为本次调用的预算秒数）
curl -s -X POST localhost:8000/invoke -H 'content-type: application/json' \
  -d '{"function":"add","idempotency_key":"k1","payload":{"a":1,"b":2},"timeout":10}'

# 查询终态 / 重试历史
curl -s localhost:8000/executions/k1

# 撤回一个排队中/执行中的调用（重复调用结论一致）
curl -s -X POST localhost:8000/executions/k1/cancel

# 模拟崩溃恢复：执行 slow 函数期间 kill 进程，重启后自动从检查点恢复
# （cancelled/timed_out 等终态不会被恢复）；或手动触发：
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
- `test_budget_cancel_reject.py`：单次调用预算覆盖全局默认、
  预算耗尽收敛（而非无限重试）、单步超时重试且已完成步骤不重跑、
  撤回排队中/执行中/退避中的调用、撤回幂等且重启后不恢复、
  容量拒绝瞬态可重提、配额与容量拒绝原因可区分；
- `test_api.py`：HTTP 层端到端（调用、查询、幂等、404、恢复端点、
  按调用预算、撤回端点、配额 429）。
