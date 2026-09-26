# 本地持久化函数执行平台（Durable Function Runtime）

一个可在本地直接运行的函数执行平台：用**本地 JSON 文件 + 原子写**模拟服务端
持久化状态，使函数调用、自动重试与中断恢复在**进程重启**和**重复请求**下仍然
给出**确定且可解释**的结果。

- 幂等调用：同一 `idempotency_key` 的重复调用不产生重复副作用，返回首次结论
- 可重试 / 不可重试错误显式区分，指数退避重试，超限进入可查询的失败终态
- 每步检查点持久化；进程重启后从最近检查点继续，已完成步骤不重复
- 检查点缺失 / 版本不兼容 / 损坏以**不同错误码**拒绝恢复，绝不静默重跑
- 有界并发槽 + 有界队列 + 调用配额，超限拒绝且原因可区分
- 完整的状态迁移与重试记录，日志打印调用标识、迁移与重试

## 目录结构

```
dfr/
  errors.py       # 错误分类与稳定的错误码
  models.py       # 状态枚举、执行/重试/迁移记录、schema 版本
  backoff.py      # 指数退避重试策略
  storage.py      # 本地 JSON 原子写存储
  checkpoint.py   # 检查点校验（缺失/版本/损坏拒绝）
  registry.py     # 函数注册（函数 = 有序步骤列表）
  runtime.py      # 执行引擎（幂等/并发/排队/重试/恢复）
  demo.py         # 内置演示函数
  api.py          # FastAPI HTTP 接口
main.py           # 启动入口（启动时自动恢复）
tests/            # 幂等/恢复/重试超时/并发/HTTP 共 24 个测试
```

## 快速开始

```bash
uv sync                       # 安装依赖（含 dev 组的 pytest/httpx）
uv run python main.py         # 启动服务 http://127.0.0.1:8000，自动恢复 data/
uv run pytest -s              # 运行全部测试（-s 打印调用标识/迁移/重试日志）
```

接口（交互式文档见 `/docs`）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET  | `/health` | 健康检查与并发/队列/配额配置 |
| GET  | `/functions` | 已注册函数 |
| POST | `/invoke` | 幂等调用 |
| GET  | `/executions/{key}` | 查询执行（结果、迁移、重试、检查点） |
| POST | `/recover` | 恢复全部未完成执行 |
| POST | `/recover/{key}` | 恢复单个执行 |

调用示例：

```bash
curl -s -X POST localhost:8000/invoke -H 'content-type: application/json' -d '{
  "function": "flaky_then_append",
  "idempotency_key": "demo-001",
  "payload": "order-123"
}'
```

`flaky_then_append` 的前两次执行抛可重试错误，第三次成功。再次发送**完全相同
的 key**会立即返回首次结论（`replayed=true`），不会再次执行。

也可以使用一键本地验证脚本（见下文“本地验证”）：

```bash
bash scripts/demo.sh
```

## 执行与恢复模型

### 函数 = 有序步骤

一个注册函数被建模为有序步骤列表。运行时依次执行步骤，步骤签名：

```python
def step(ctx: StepContext):
    # ctx.payload  本次调用入参
    # ctx.previous 上一步的返回值
    return result
```

步骤通过抛异常表达错误语义：

- `RetryableError`：可重试（如临时网络故障、`StepTimeoutError` 是其子类）
- `NonRetryableError`：不可重试，立即失败

### 状态机

```
PENDING ──accepted──► RUNNING ──全部步骤成功──► SUCCEEDED   （终态）
                         │
                         ├── 不可重试错误 ─────► FAILED       （终态）
                         ├── 重试次数耗尽 ─────► FAILED       （终态）
                         ├── 超过整体 deadline ─► TIMED_OUT    （终态）
                         └── 手动挂点/进程中断 ─► SUSPENDED ──resume──► RUNNING
```

终态（SUCCEEDED / FAILED / TIMED_OUT）的记录可随时查询，永不重新执行。

### 检查点与原子写

- 每**成功完成一个步骤**，立即把整条执行记录写入磁盘：
  `data/<url-encoded-idempotency-key>.json`
- 写入使用“同目录临时文件 + `fsync` + `os.replace`”，崩溃不会留下半截文件
- 记录中保存 `next_step`（下一步序号）与每个已完成步骤的结果
- 恢复时从 `next_step` 继续；崩溃发生在“某一步执行中”时，该步会被再执行
  至多一次（at-least-once），此前已完成步骤绝不重复 —— 因此步骤应尽量使
  副作用幂等

### 恢复的前置校验（拒绝原因可区分）

恢复前对检查点做严格校验，不通过即拒绝，**不会静默从头重跑或跳过步骤**：

| 情况 | 错误码 | HTTP |
| --- | --- | --- |
| 检查点文件不存在 | `checkpoint_missing` | 409 |
| `schema_version` 与当前运行时不一致 | `checkpoint_version_incompatible` | 422 |
| JSON 损坏、字段缺失、`next_step != len(steps)`、状态非法等 | `checkpoint_corrupt` | 422 |

`schema_version` 为持久化格式版本（当前为 `1`），格式发生不兼容演进时会
提升版本号，旧检查点将被明确拒绝而不是错误解析。

挂起 / 超时的执行也有明确终态：恢复时若持久化的 `deadline_epoch` 已过，
执行直接进入 `TIMED_OUT`，不会再调用任何步骤。

## 幂等与重试语义

- **幂等键即身份**：`(function, idempotency_key)` 唯一确定一次执行。
  首次调用接受后，相同键的请求一律返回持久化的首次结论（`replayed=true`），
  即使两次携带不同 payload，也以首次请求为准。
- **重复调用不占并发槽 / 不消耗配额 / 不产生副作用**。
- **重试只针对可重试错误**：第 `attempt` 次失败后等待
  `min(base_delay * multiplier**(attempt-1), max_delay)` 秒。
- `max_attempts` 是总尝试次数（默认 3，即最多重试 2 次）。耗尽后进入
  `FAILED` 终态，`error_code` 保留最后一次错误（如 `retryable_error`、
  `step_timeout`），每次尝试都记录在 `retries` 中。
- 不可重试错误在第 1 次尝试即 `FAILED`。
- **两种超时**：单步 `timeout`（默认将步骤包装为可重试超时）；整体
  `deadline`（超过后进入 `TIMED_OUT` 终态，重试等待期间也会判定）。

执行记录中的 `transitions`（状态迁移，含原因与时间戳）与 `retries`
（尝试序号、步骤、错误码、退避时长）提供完整可解释性。

## 并发限制与配置

`RuntimeConfig`（`dfr/runtime.py`）：

| 参数 | 默认 | 含义 |
| --- | --- | --- |
| `max_concurrency` | 4 | 同时实际执行的调用上限（执行槽） |
| `max_queue` | 8 | 执行槽占满时的有界等待队列长度 |
| `max_calls` | `None` | 进程生命周期内接受的新调用总配额 |
| `default_step_timeout` | `None` | 单步默认超时（秒） |
| `default_deadline` | `None` | 整体默认截止时间（秒） |

背压行为：

- 执行槽有空：立即执行（`wait=true` 同步返回，或后台执行）。
- 执行槽满、队列有空：调用被**有界排队**，响应 `queued=true`，
  在后台等待执行槽（排队调用已持久化，不会丢失）。
- 执行槽与队列全满：立即以 `concurrency_limit_rejected`（HTTP 429）
  **有界拒绝**，不创建任何记录。
- 配额耗尽：以 `quota_exceeded`（HTTP 429）拒绝，与容量拒绝码可区分。
- **同一幂等键由键级互斥锁串行化**：同键并发调用只会真实执行一次，
  其余全部走重放分支。

`RetryPolicy`（`dfr/backoff.py`）：`max_attempts / base_delay /
multiplier / max_delay`。

> 该持久化模型面向单进程本地运行；同进程并发由键锁 + 信号量保证。
> 多进程/多机共享同一目录的强互斥不在本实现范围内。

## 本地验证

一键脚本会演示：幂等重放、可重试错误退避后成功、失败终态查询、
排队/拒绝原因：

```bash
bash scripts/demo.sh
```

手工验证“进程重启恢复”：

```bash
# 1) 调用时让执行挂在第 2 步之前
curl -s -X POST localhost:8000/invoke -H 'content-type: application/json' -d '{
  "function":"echo_then_append","idempotency_key":"resume-demo",
  "payload":"x","suspend_before_step":1}'
# status = SUSPENDED, next_step = 1

# 2) 重启进程（Ctrl+C 后再次 uv run python main.py，启动自动恢复）
# 3) 查询：应已 SUCCEEDED，且只有一次完整执行
curl -s localhost:8000/executions/resume-demo
```

验证拒绝原因（检查点版本不兼容）：

```bash
f=$(ls data/*bad*.json 2>/dev/null) # 或手工编辑任一 json 的 schema_version
sed -i 's/"schema_version": 1/"schema_version": 99/' "$f"
curl -s -X POST localhost:8000/recover   # error_code=checkpoint_version_incompatible
```

运行测试并观察可解释日志（调用标识、状态迁移、重试记录）：

```bash
uv run pytest -s
```

## 设计边界与说明

- 步骤副作用建议自身幂等：检查点在步骤成功后写下，“执行中崩溃”按
  at-least-once 语义可能重复一次该步骤。
- 步骤超时通过独立守护线程执行实现：超时会被判定并放弃等待，但 Python
  无法强杀正在运行的线程，步骤应协作式地及时返回。
- 时间戳使用 UTC ISO-8601；`deadline_epoch` 使用 `time.monotonic()`，
  仅用于同一机器上的相对超时判定，不参与序列化兼容性。
