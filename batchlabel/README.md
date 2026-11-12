# 批次标签服务（Batch Labelling Service）

纯 Python 标准库 + SQLite 实现的异步批次标注后端。包含三个可独立运行的
进程组件和一个端到端验收程序：

| 文件 | 角色 |
| --- | --- |
| `common.py` | 严格输入校验、指纹、HTTP/JSON 客户端、传输错误类型 |
| `server.py` | 后端批次服务：作业目录、逻辑时钟、epoch 租约、原子发布、崩溃钩子 |
| `worker.py` | 工作进程：领取/续租、远端幂等提交、轮询、严格响应校验、接管 |
| `mock_source.py` | HTTP 模拟标注源：`/tasks` 幂等、断连/503、夹具标签、非法响应体 |
| `acceptance_test.py` | 以**真实 OS 进程**编排的 12 项验收（两个 worker、强杀、时钟推进） |
| `fixtures.json` | 各作业的标签与故障夹具 |

无第三方依赖，全部用 `python3 -I` 运行（隔离模式，防止当前目录植入模块）。

---

## 1. 运行

要求 Python 3.10+（开发于 3.11）。三个组件分别启动：

```bash
# 1) 模拟标注源（标签与故障来自夹具文件）
python3 -I mock_source.py --fixture fixtures.json --port 9090

# 2) 后端批次服务（SQLite 路径不存在会自动建库）
python3 -I server.py --db /tmp/batch.db --port 8080

# 3) 一个或多个工作进程（可在不同 shell / 不同机器上启动多个）
python3 -I worker.py --server http://127.0.0.1:8080 \
                     --source http://127.0.0.1:9090 --name w1
python3 -I worker.py --server http://127.0.0.1:8080 \
                     --source http://127.0.0.1:9090 --name w2
```

可选参数：`--crash-hook <file>`（测试用崩溃钩子，见第 5 节）、
`--interval <秒>`（worker 轮询/续租节奏，默认 0.1）、`--verbose`。

### 一键验收

```bash
python3 -I acceptance_test.py
```

验收程序自行挑选空闲端口、在临时目录建库与夹具、拉起真实的
mock / server / worker 进程，结束后清理。输出 12 个 `PASS` 与
`=== 12/12 acceptance checks passed ===`，失败时进程退出码非零并打印
traceback 与全部组件日志。

---

## 2. HTTP 接口

### 后端批次服务

| 方法与路径 | 说明 |
| --- | --- |
| `POST /jobs` | 提交 `{"job","rows"}`；`rows` 为 `{"id","text"}` 数组。201 新建；同 job 同内容（按 id 排序后比较）返回 200 且 `replayed=true`；同 job 不同内容返回 409，原作业不变；非法输入返回 400 并带 `path`（如 `/rows/0/id`），且不建作业 |
| `GET /jobs/{job}` | 作业视图。成功时 `labels` 为按 id 排序的完整 `{id,label}`（保留真实 `null`）；未完成或失败时 `labels=null`，失败/未完成绝不暴露任何标签 |
| `POST /test/advance` | 测试接口，`{"delta":n}` 单调推进持久化时钟 `now`；`n` 必须是非负整数，**布尔被显式拒绝** |
| `GET /test/inspect` | 查看时钟与全部作业的状态/epoch/租约/票据/进度 |
| `POST /internal/claim` | worker 领取。无持有者或 `now` 达到到期值才可领，返回从 1 递增的 `epoch`，到期值=`now+5`；无可领作业返回 204 |
| `POST /internal/renew` | 续租到 `now+5`，须匹配当前 epoch 且租约未到期 |
| `POST /internal/progress` | 进度累加（非负整数，拒布尔），同样受 epoch/租约守卫 |
| `POST /internal/ticket` | 远端受理后持久化票据 |
| `POST /internal/complete` | 标签与 `succeeded` 终态在**单事务**内原子发布，校验 id 与输入一一对应 |
| `POST /internal/fail` | 写入 `failed` 终态；失效持有者（epoch 不符或租约过期）一律 409，状态不变 |

所有租约作用域写操作都先在同一写事务内检查资格（终态/epoch/到期），
不满足即 409，因此失效持有者的成功与错误写入都不可能改变状态。

### 模拟标注源

| 方法与路径 | 说明 |
| --- | --- |
| `POST /tasks` | 以 `job` 为幂等键。相同内容始终返回同一 `ticket`，且只创建一次任务（首次 201 `created=true`，重发 200 `created=false`）；同 job 不同内容返回 409；可按夹具注入“先受理后断连”和 503，两者都可安全重试且响应体不被校验 |
| `GET /tasks/{ticket}` | `pending`（无结果字段）/ `failed`（终态）/ `ready`（含 `job` 与全部 `{id,label}`，标签可为 JSON `null`）。终态不可变；可注入坏 JSON、错误 job、id 不匹配、非字符串标签等非法 200 体 |
| `POST /test/resolve` | 测试控制任务状态；对已终态任务的变更返回 409，相同状态重放为幂等 no-op |
| `GET /test/inspect` | 远端任务数与清单（验收据此断言“仅一个远端任务”） |

---

## 3. 关键正确性设计

### 逻辑时钟与 epoch 租约
- 时钟 `now` 是 SQLite 中持久化的非负整数，初值 0，**只能**经
  `POST /test/advance` 单调推进；所有整数入口都先排除 `bool`。
- 领取产生从 1 递增的 epoch，到期值 `now+5`。无持有者（`queued`）或
  `now >= 到期值` 才能被领取；领取本身在 `BEGIN IMMEDIATE` 事务内完成，
  多进程竞争由 SQLite 写锁串行化。
- 续租/进度/票据/终态写入都要求 `epoch` 相等且 `now < 到期值`，
  资格检查与写入处于同一事务。接管者拿到新 epoch 后，旧持有者的任何
  延迟成功或失败写入都得到 409，作业状态不被触碰；终态作业永不再被领取。

### 远端幂等与可重试故障
- 远端以 `job` 为幂等键，任务在模拟故障**之前**已落账，因此“受理后
  断连”重放只会取回同一票据，绝不产生第二个任务。
- worker 仅把连接级失败（`TransportError`，含 `RemoteDisconnected`、
  `IncompleteRead`、超时、拒连）和 HTTP 503 视为可重试，且**不解析其
  响应体**；其余非 2xx 状态码使作业失败。只有 200 响应体会被严格校验：
  `job` 必须匹配、结果 id 与输入一一对应、标签只能是字符串或 null，
  任何非法体都让作业进入 `failed` 且无标签。

### 原子发布与崩溃恢复
- `complete` 把全部标签写入与状态翻转放在一个事务：崩溃在 COMMIT 前，
  SQLite 回滚，不留部分标签，作业仍可被新 epoch 接管；COMMIT 后标签与
  完成状态一同可见。查询按 id 排序返回，未完成/失败一律无标签。
- 身份（job 与行 id）、租约（epoch/到期值）、票据、进度都在 SQLite 中，
  后端进程被 SIGKILL 后重启不丢失。

### 输入校验
- `job` 与行 `id` 必须是非空 ASCII 字母数字串；行 id 不可重复；`text`
  必须是字符串。错误响应带 JSON-Pointer 风格 `path` 精确定位，校验失败
  时不写入任何作业。空批（`rows: []`）合法。

---

## 4. 验收场景（`acceptance_test.py`，12 项）

1. 正常路径 + 同内容幂等重放（不同行序）+ 异内容 409 且原结果不变；
2. 真实 JSON `null` 标签端到端保留，输出按 id 排序；
3. 空批合法并成功，远端仍只有一个任务；
4. 远端“受理后断连”一次 + 503 一次，worker 重试成功，远端任务计数恰为 1；
5. 四类非法 200 体（坏 JSON / job 不符 / id 不匹配 / 标签类型错）均令
   作业失败，且失败作业无标签；
6. 远端终态 `failed` 透传为作业失败，终态不可再被变更或领取；
7. 非法输入逐路径定位、拒绝的作业不存在、时钟接口拒绝布尔；
8. SIGKILL 后端后重启：时钟、epoch、租约、票据、进度、已发布标签全部保留；
9. **两个真实 worker 进程**的旧 epoch 接管：旧持有者存活但租约到期，
   新持有者领取 epoch 2；旧持有者的延迟 `fail` 与 `complete` 都被 409
   拒绝，最终结果来自接管者且无 `STALE` 覆盖；
10. **强退点 A**——远端受理后、本地记票据前 worker 被 `os._exit` 强杀；
    到期接管后核对远端仅一个任务、票据不变、结果与无故障一致；
11. **强退点 B**——标签已写入事务、COMMIT 前后端被强杀；重启后无部分
    标签，接管者原子完成，标签行数不重复，远端仍仅一个任务；
12. 终态（成功/失败）作业在多轮领取扫描中永不被再次领取。

---

## 5. 测试崩溃钩子（仅测试用）

钩子是一个 JSON 文件，组件启动时用 `--crash-hook` 传入，规则触发后文件
会先被删除再动作，保证重启后不会重复触发。

worker 钩子：
```json
{
  "worker_pause": {"job": "jobTakeover"},
  "worker_exit_after_accept": {"job": "jobCrashTicket"}
}
```
- `worker_pause`：在远端 POST 前阻塞，直到钩子文件被删除，用于制造
  “旧持有者仍存活但租约已到期”的接管场景；
- `worker_exit_after_accept`：远端返回票据后、本地持久化票据前 `os._exit(2)`。

server 钩子：
```json
{"server_exit": {"job": "jobCrashCommit", "when": "before_commit_complete"}}
```
- 在 `complete` 事务内标签 UPDATE 之后、COMMIT 之前强杀后端进程。

---

## 6. 手动快速验证

```bash
# 启动三个组件后：
curl -s localhost:8080/jobs -H 'Content-Type: application/json' \
  -d '{"job":"jobBase","rows":[{"id":"a1","text":"first"},{"id":"b2","text":"second"}]}'
curl -s localhost:8080/jobs/jobBase                 # 轮询直到 succeeded
curl -s localhost:8080/test/advance -H 'Content-Type: application/json' -d '{"delta":5}'
curl -s localhost:9090/test/inspect                # 远端任务清单与计数
```
