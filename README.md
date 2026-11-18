# 故障日志服务：legacy_summary 列安全退役 + 旧版查询按号取消

FastAPI + PostgreSQL 16，双实例共享一个数据库。提供旧版检索（读 `legacy_summary`，
强制携带客户端唯一查询号、支持按号取消）、新版检索（读 `summary`）和旧列退役 API，保证：

- 退役受理后，**两个实例立即拒绝**新发起的旧版检索（409，完成后 410）；
- **已接纳的旧查询读完（或被取消回滚）后才删列**（事务级咨询锁，跨实例生效）；
- 退役任何阶段**新版检索不受影响**；
- 删列失败或实例重启时**保留 `retiring` 待退役状态**，旧版不能重新接单，重启后自动续跑；
- 重试与并发退役**不误报完成、不重复删列**（状态机 + 行锁 + `DROP COLUMN IF EXISTS` 与置位同事务）；
- 旧版检索**同号全局只接纳一次**；取消请求可发往**任一实例**，查询池占满时仍可取消；
- 取消**只作用于该号尚在执行的查询**，已结束或连接复用后不会打断下一条；
  只有在原查询停止、事务回滚、读锁/咨询锁释放后才回报 `cancelled=true`，
  中止未确认时如实返回 `acknowledged=false` 与当前状态。

## 协调协议

```
                 POST /api/retirement
   active  ───────────────────────►  retiring  ──(在途旧查询全部读完)──►  retired
                                      ▲   │                                  │
                                      │   ├─ 删列失败：记录 last_error，      │ 旧版 410
   旧版 200                            │   │   保持 retiring，可重试          │ 新版 200
   新版 200                            │   └─ 实例重启：启动时发现 retiring    │
                                      │       自动续跑退役任务                │
                                      └──── 重试/并发调用（幂等）
```

1. **`retirement_state` 单行表**（两实例共享）：`active → retiring → retired`。
   受理退役时**先提交 `retiring`** 再启动后台删列任务，因此两个实例的旧版检索
   都能立即看到状态并拒单（快速预检 + 锁内权威复检双重检查，消除竞态）。
2. **PostgreSQL 事务级咨询锁**（key 为常量，两实例相同）：
   - 旧版检索：事务内 `pg_advisory_xact_lock_shared(KEY)` → 复检状态 → 读 `legacy_summary`；
   - 退役任务：事务内 `pg_advisory_xact_lock(KEY)`（排他）→ 等所有在途旧查询读完
     → `ALTER TABLE fault_logs DROP COLUMN IF EXISTS legacy_summary`
     → 同事务把状态置为 `retired`。
   PG 锁队列的公平性保证排他请求排队后，新共享锁请求不会插队。
3. **删列与置位同一事务**：崩溃则整体回滚；`IF EXISTS` + 条件 `UPDATE` 使重试、
   并发调用安全，不会重复删列，也不会把未完成误报为完成（响应永远反映库中真实状态）。

## 旧版查询的查询号与取消协议

```
  客户端分配 query_id（8-128 位字母/数字/_/-）
        │  GET /api/v1/legacy/fault-logs?query_id=...
        ▼
  INSERT legacy_queries（主键）──冲突──► 409 同号只能接纳一次（跨实例全局生效）
        │ 接纳
        ▼
  进程内在途登记（query_id -> backend_pid）+ 事务内共享咨询锁 + 实际查询
        ▲                                   │
        │ pg_cancel_backend（属主实例）      ▼ 正常结束/取消/出错
POST .../queries/{id}/cancel（任一实例）   finally：摘登记 → 共享行置终态 → 连接还池
        │
        ├─ 属主在本实例：直接核对在途登记后发信号
        └─ 属主在另一实例：置 cancel_requested + pg_notify，属主监听协程执行
```

1. **共享登记表 `legacy_queries`**（两实例共享，见 `db/init/01_init.sql`）：
   主键即 `query_id`，记录属主实例与后端 pid，状态机
   `accepted → cancel_requested → cancelled | completed | failed`，
   另有崩溃对账终态 `interrupted`。
2. **取消只认真正在途的查询**：`pg_cancel_backend` 前必须在**进程内**在途登记表
   核对到该 query_id；查询协程在连接归还连接池之前就摘除条目，并执行一条 `SELECT 1`
   消耗可能滞留的取消信号，因此**连接复用后的下一条查询不可能被旧信号误伤**。
3. **池满可取消**：取消/登记走独立的控制连接与监听连接，不占用 `max_size=8` 的
   查询连接池；8 条慢查询把池占满、第 9 条请求在池外排队时，取消 API 依然毫秒级可用。
4. **成功语义不夸大**：取消端点轮询/等待共享终态，只有见到 `cancelled` 才回
   `cancelled=true`（此时事务已回滚、读锁与咨询锁已释放、连接已还池，退役可继续）；
   10 秒窗口内未确认则回 `acknowledged=false` 并附当前 `state`，可用
   `GET /api/v1/legacy/queries/{query_id}` 继续观察。
5. **崩溃对账**：属主实例在查询结束前崩溃时，重启后由启动对账把残留登记置为
   `interrupted`（用 `pg_terminate_backend` 做活性探测，规避 Unix socket 下
   孤儿后端长期留在 `pg_stat_activity` 的问题），绝不虚构 `cancelled`。

## 目录结构

```
app/main.py          FastAPI 应用：检索/查询号/取消/退役/状态 API、启动时对账与续跑
app/cancel.py        查询号登记、跨实例 LISTEN/NOTIFY 取消路由、控制连接、崩溃对账
app/retirement.py    退役后台任务（咨询锁 + 删列 + 状态置位）
db/init/01_init.sql  建表（含 legacy_queries）、预置 14 条故障日志、初始化状态机（可重入）
docker-compose.yml   postgres:16 + app1(:8001) + app2(:8002)
Dockerfile           应用镜像
scripts/e2e.py       端到端场景库（可对 compose 起的服务运行）
scripts/local_e2e.py 本地编排器：内嵌 PG16 + 双进程实例，含崩溃重启/对账场景
```

## 快速开始（Docker Compose）

```bash
docker compose up --build -d
```

- app1 Swagger: http://localhost:8001/docs
- app2 Swagger: http://localhost:8002/docs
- 数据库已预置 14 条故障日志（`summary` 与 `legacy_summary` 两列均有数据）。

## Swagger 操作手册

**1. 正常检索**：对 app1、app2 分别执行 `GET /api/v1/legacy/fault-logs?query_id=<8位以上唯一号>`
（返回 `legacy_summary`）和 `GET /api/v2/fault-logs`（返回 `summary`）。
旧版接口必须带 `query_id`，同号重复使用返回 409。

**2. 按号取消（可在任一实例操作）**：
- app2：`GET /api/v1/legacy/fault-logs?query_id=q-cross-demo-1&slow_ms=12000`（在途）；
- app1：`POST /api/v1/legacy/queries/q-cross-demo-1/cancel`
  → `cancelled=true, state=cancelled`，属主字段显示 `owner_instance=app2`；
- app2 上原查询返回 **503 / status=cancelled**；
- `GET /api/v1/legacy/queries/q-cross-demo-1` 可随时查共享状态；
- 对已结束/已取消的号再调取消：`cancelled=false` 并在 `detail` 说明，不产生新效果；
- 未知号返回 404；等待窗口内未确认时返回 `acknowledged=false` + 当前 `state`。

**3. 长查询、取消与退役交错**：
- app1/app2 各发一个 `slow_ms=20000` 的旧版查询（不同 query_id）；
- 任一实例 `POST /api/retirement` → **202 / retiring**，删列被在途查询阻塞；
- 从对侧实例分别取消两个查询 → 两者确认 `cancelled`（锁释放）；
- `GET /api/retirement` 观察状态随后变为 `retired`、`legacy_column_present=false`；
- 此后旧版检索 → **410**，新版检索全程 **200**。

**4. 池满仍可取消 + 连接复用**：向同一实例连发 8 个 `slow_ms=30000` 的旧版查询占满
连接池（第 9 个会排队），此时对任一在途号调取消仍立即成功；取消后用**新查询号**
连发请求验证连接复用安全（旧号不可再接纳，新查询全部 200）。

**5. 删列失败与重试**（演示钩子，compose 中已开启 `ENABLE_TEST_HOOKS=true`）：
- app1：`POST /api/test-hooks/fail-next-drops?times=1`；
- app1：`POST /api/retirement` → 202；
- `GET /api/retirement` → `status=retiring`、`failed_attempts=1`、`last_error` 有记录，
  旧列仍在，两实例旧版均 409；
- app2：`POST /api/retirement`（重试）→ 稍候 `GET /api/retirement` → `retired`。

**6. 退役途中实例重启**：
- app2：`GET /api/v1/legacy/fault-logs?query_id=q-restart-demo&slow_ms=9000`（保持在途）；
- app1：`POST /api/retirement` → 202 后立即 `docker compose restart app1`；
- 期间 app2：旧版 409、新版 200、`GET /api/retirement` 保持 `retiring`；
- app1 重启完成后自动续跑：长查询读完 → 状态变 `retired`。
  若 app1 崩溃时自己名下还有在途查询，重启对账会把这些号置为 `interrupted`。

**7. 并发退役**：同时（两个浏览器标签/脚本）对 app1、app2 发 `POST /api/retirement`
→ 两者都返回 202 且 `status=retiring`、同一 `request_id`，不会重复删列、不误报完成。

## API 一览

| 方法/路径 | 说明 |
| --- | --- |
| `GET /api/v1/legacy/fault-logs?query_id=...` | 旧版检索（query_id 必填、全局唯一）；被取消→503，retiring→409，retired→410 |
| `POST /api/v1/legacy/queries/{query_id}/cancel` | 按号取消（任一实例受理，跨实例 NOTIFY 路由）；成功 `cancelled=true`，未确认 `acknowledged=false` |
| `GET /api/v1/legacy/queries/{query_id}` | 查询号状态：accepted/cancel_requested/cancelled/completed/failed/interrupted |
| `GET /api/v2/fault-logs` | 新版检索，读 `summary`；退役各阶段均可用 |
| `POST /api/retirement` | 受理退役（幂等/可重试）；202=进行中，200=已完成 |
| `GET /api/retirement` | 退役状态（含 `failed_attempts`、`last_error`、`legacy_column_present`） |
| `POST /api/test-hooks/fail-next-drops` | [仅测试] 模拟删列失败，需 `ENABLE_TEST_HOOKS=true` |
| `GET /healthz` | 健康检查 |

## 自测

无 Docker 的本机环境（内嵌 PostgreSQL 16.2 + 两个 uvicorn 进程）：

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt -r requirements-dev.txt
.venv/bin/python scripts/local_e2e.py     # 8 个场景、110 条断言
```

对 compose 起的服务运行（重启/对账场景除外，它们需要 kill 本地进程）：

```bash
python3 scripts/e2e.py --app1 http://localhost:8001 --app2 http://localhost:8002 \
    --pg-dsn postgresql://ops:ops_password@localhost:5432/faultdb
```

场景覆盖：① 双实例基线检索 + 同号唯一/跨实例冲突；② 删列失败→保留 retiring 拒单→
跨实例重试成功；③ 长查询交错 + 双实例并发退役；④ 同实例/跨实例取消、已结束不误伤、
重复取消幂等、连接复用安全；⑤ 取消在途查询释放锁后退役自动完成；⑥ 查询池占满仍可
取消 + 排队请求与连接复用交错；⑦ 退役途中 kill -9 实例→状态保留→重启自动续跑；
⑧ 属主崩溃在途查询→重启对账 interrupted、未确认取消如实回报。

## 验证状态（据实）

- **已验证**（真实 PostgreSQL 16.2，双实例共享库，`scripts/local_e2e.py` 110/110
  断言通过）：上述全部场景，含跨实例 NOTIFY 取消、池满取消（实测不被连接池阻塞，
  约 0.2s 返回）、连接复用后新查询不被旧信号打断、取消后退役继续完成、
  崩溃重启自动续跑与 interrupted 对账、Swagger `/docs` 与 `/openapi.json` 可用、
  测试钩子默认关闭（404）。另以独立脚本验证了属主实例被 kill 时取消端点在 10s
  窗口后返回 `acknowledged=false` + `cancel_requested`，重启后对账 `interrupted`。
- **未在本环境执行**：`docker compose up` 本身（本环境无 Docker）。compose 文件已做
  YAML 静态校验，保留 postgres:16 + app1/app2 双实例与 8001/8002 Swagger 入口不变；
  应用逻辑在与 compose 相同的主版本（PG 16）上完整验证过。首次在有 Docker 的环境
  启动时如遇到问题，请优先检查 `db` 健康检查与端口占用。

## 设计取舍说明

- 取消成功的 HTTP 语义：原查询被中止时返回 **503**（服务因取消未能完成该请求），
  响应体 `status=cancelled`；取消端点本身始终用 200 + 布尔/状态字段表达处理结果，
  只有实际见到中止终态才给 `cancelled=true`。
- `legacy_queries` 是登记表而非审计表：锁内拒单（未真正执行）的行直接删除，
  正常终态行保留以便同号拒绝与状态查询；数据量很小，未做定期清理任务。
- `slow_ms` 参数与 fail-next-drops 钩子是为演示/自测长查询与失败路径而设，
  后者默认关闭；生产部署请移除或保持关闭。
- 退役为单向操作，不提供"回退到 active"的 API（列删除不可逆，状态机如实反映）。
- 删列瞬间 `ALTER TABLE` 需短暂获取表级 ACCESS EXCLUSIVE 锁；因在途旧查询已被
  咨询锁排空、新版查询都很短，实际阻塞窗口极小。
