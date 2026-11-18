# 故障日志服务：legacy_summary 列安全退役

FastAPI + PostgreSQL 16，双实例共享一个数据库。提供旧版检索（读 `legacy_summary`）、
新版检索（读 `summary`）和旧列退役 API，保证：

- 退役受理后，**两个实例立即拒绝**新发起的旧版检索（409，完成后 410）；
- **已接纳的旧查询读完后才删列**（事务级咨询锁，跨实例生效）；
- 退役任何阶段**新版检索不受影响**；
- 删列失败或实例重启时**保留 `retiring` 待退役状态**，旧版不能重新接单，重启后自动续跑；
- 重试与并发退役**不误报完成、不重复删列**（状态机 + 行锁 + `DROP COLUMN IF EXISTS` 与置位同事务）。

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

## 目录结构

```
app/main.py          FastAPI 应用：检索/退役/状态 API、启动时续跑逻辑
app/retirement.py    退役后台任务（咨询锁 + 删列 + 状态置位）
db/init/01_init.sql  建表、预置 14 条故障日志、初始化状态机（可重入）
docker-compose.yml   postgres:16 + app1(:8001) + app2(:8002)
Dockerfile           应用镜像
scripts/e2e.py       端到端场景库（可对 compose 起的服务运行）
scripts/local_e2e.py 本地编排器：内嵌 PG16 + 双进程实例，含崩溃重启场景
```

## 快速开始（Docker Compose）

```bash
docker compose up --build -d
```

- app1 Swagger: http://localhost:8001/docs
- app2 Swagger: http://localhost:8002/docs
- 数据库已预置 14 条故障日志（`summary` 与 `legacy_summary` 两列均有数据）。

## Swagger 操作手册

**1. 正常检索**：对 app1、app2 分别执行 `GET /api/v1/legacy/fault-logs`
（返回 `legacy_summary`）和 `GET /api/v2/fault-logs`（返回 `summary`）。

**2. 长查询与退役交错**：
- app1：`GET /api/v1/legacy/fault-logs?slow_ms=8000`（`slow_ms` 为演示参数，模拟长查询）；
- 查询在途时，app2：`POST /api/retirement` → 返回 **202 / status=retiring**；
- 立即对两个实例再发旧版检索 → **409**；新版检索 → **200**；
- `GET /api/retirement` 观察：长查询读完前 `legacy_column_present=true`，
  读完后状态变为 `retired`、`legacy_column_present=false`；
- 此后旧版检索 → **410**。

**3. 删列失败与重试**（演示钩子，compose 中已开启 `ENABLE_TEST_HOOKS=true`）：
- app1：`POST /api/test-hooks/fail-next-drops?times=1`；
- app1：`POST /api/retirement` → 202；
- `GET /api/retirement` → `status=retiring`、`failed_attempts=1`、`last_error` 有记录，
  旧列仍在，两实例旧版均 409；
- app2：`POST /api/retirement`（重试）→ 稍候 `GET /api/retirement` → `retired`。

**4. 退役途中实例重启**：
- app2：`GET /api/v1/legacy/fault-logs?slow_ms=9000`（保持在途）；
- app1：`POST /api/retirement` → 202 后立即 `docker compose restart app1`；
- 期间 app2：旧版 409、新版 200、`GET /api/retirement` 保持 `retiring`；
- app1 重启完成后自动续跑：长查询读完 → 状态变 `retired`。

**5. 并发退役**：同时（两个浏览器标签/脚本）对 app1、app2 发 `POST /api/retirement`
→ 两者都返回 202 且 `status=retiring`、同一 `request_id`，不会重复删列、不误报完成。

## API 一览

| 方法/路径 | 说明 |
| --- | --- |
| `GET /api/v1/legacy/fault-logs` | 旧版检索，读 `legacy_summary`；retiring→409，retired→410 |
| `GET /api/v2/fault-logs` | 新版检索，读 `summary`；退役各阶段均可用 |
| `POST /api/retirement` | 受理退役（幂等/可重试）；202=进行中，200=已完成 |
| `GET /api/retirement` | 退役状态（含 `failed_attempts`、`last_error`、`legacy_column_present`） |
| `POST /api/test-hooks/fail-next-drops` | [仅测试] 模拟删列失败，需 `ENABLE_TEST_HOOKS=true` |
| `GET /healthz` | 健康检查 |

## 自测

无 Docker 的本机环境（内嵌 PostgreSQL 16.2 + 两个 uvicorn 进程）：

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt -r requirements-dev.txt
.venv/bin/python scripts/local_e2e.py     # 4 个场景、59 条断言
```

对 compose 起的服务运行（重启场景除外）：

```bash
python3 scripts/e2e.py --app1 http://localhost:8001 --app2 http://localhost:8002 \
    --pg-dsn postgresql://ops:ops_password@localhost:5432/faultdb
```

场景覆盖：① 双实例基线检索；② 删列失败→保留 retiring 拒单→跨实例重试成功；
③ 长查询交错 + 双实例并发退役；④ 退役途中 kill -9 实例→状态保留→重启自动续跑。

## 验证状态（据实）

- **已验证**（真实 PostgreSQL 16.2，双实例共享库，59/59 断言通过）：上述全部场景，
  含长查询读完才删列、失败保留 retiring、重启自动续跑、并发/重试幂等、
  Swagger `/docs` 与 `/openapi.json` 可用、测试钩子默认关闭（404）。
- **未在本环境执行**：`docker compose up` 本身（本环境无 Docker）。compose 文件已做
  YAML 静态校验；应用逻辑在与 compose 相同的主版本（PG 16）上完整验证过。
  首次在有 Docker 的环境启动时如遇到问题，请优先检查 `db` 健康检查与端口占用。

## 设计取舍说明

- `slow_ms` 参数与 fail-next-drops 钩子是为演示/自测长查询与失败路径而设，
  后者默认关闭；生产部署请移除或保持关闭。
- 退役为单向操作，不提供"回退到 active"的 API（列删除不可逆，状态机如实反映）。
- 删列瞬间 `ALTER TABLE` 需短暂获取表级 ACCESS EXCLUSIVE 锁；因在途旧查询已被
  咨询锁排空、新版查询都很短，实际阻塞窗口极小。
