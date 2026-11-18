"""故障日志服务：旧版/新版检索 + legacy_summary 列退役协调 + 旧版查询取消。

部署形态：两个本服务实例（app1/app2）共享同一个 PostgreSQL 16 数据库。

旧版检索取消模型见 ``app/cancel.py``：客户端为每次旧版检索提供全局唯一
``query_id``，同号只能接纳一次；``POST .../queries/{query_id}/cancel``
可发往任一实例，跨实例经 LISTEN/NOTIFY 路由到属主实例执行。
"""
from __future__ import annotations

import asyncio
import contextlib
import os
import re
import uuid
from contextlib import asynccontextmanager
from typing import Any

import asyncpg
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from .cancel import (
    NOTIFY_CHANNEL,
    ControlDB,
    QueryEntry,
    listener_supervisor,
    reconcile_stale_queries,
    signal_cancel,
    wait_terminal,
)
from .retirement import ADVISORY_LOCK_KEY, ensure_retirement_worker

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://ops:ops_password@localhost:5432/faultdb"
)
INSTANCE_ID = os.environ.get("INSTANCE_ID", "local")
ENABLE_TEST_HOOKS = os.environ.get("ENABLE_TEST_HOOKS", "").lower() in {"1", "true", "yes"}
MAX_SLOW_MS = 120_000  # slow_ms 演示参数上限

# 客户端查询号格式：8-128 位字母/数字/_/-，首字符为字母或数字（也可安全放进 NOTIFY 负载）。
QUERY_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{7,127}$")


def _validate_query_id(query_id: str) -> None:
    if not QUERY_ID_PATTERN.fullmatch(query_id):
        raise HTTPException(
            status_code=422,
            detail="query_id 需为 8-128 位字符，允许字母、数字、下划线、中划线，且以字母或数字开头",
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=8)
    # 取消/登记专用控制连接与监听连接：独立于查询池，池满时取消路径不受阻。
    app.state.control_dsn = DATABASE_URL
    app.state.control = ControlDB(DATABASE_URL)
    await app.state.control.connect()
    app.state.query_registry: dict[str, QueryEntry] = {}
    app.state.instance_id = INSTANCE_ID
    app.state.listener_stop = asyncio.Event()
    app.state.listener_conn = None
    app.state.fail_drops_remaining = 0  # 测试钩子计数器（仅 ENABLE_TEST_HOOKS 时可被武装）
    listener_task = asyncio.create_task(listener_supervisor(app))
    # 崩溃重启对账：本实例残留、后端进程已不存在的在途登记置为 interrupted 终态。
    await reconcile_stale_queries(app)
    # 退役途中实例重启：发现库里仍是 retiring，自动续跑退役任务；
    # 重启期间及之后旧版检索依然被拒绝（状态在共享库中）。
    async with app.state.pool.acquire() as conn:
        status = await conn.fetchval("SELECT status FROM retirement_state WHERE id = 1")
    if status == "retiring":
        await ensure_retirement_worker(app)
    try:
        yield
    finally:
        app.state.listener_stop.set()
        listener_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await listener_task
        if app.state.listener_conn is not None:
            with contextlib.suppress(Exception):
                await app.state.listener_conn.close()
        await app.state.control.close()
        await app.state.pool.close()


app = FastAPI(
    title="Fault Log Service",
    version="1.1.0",
    description=(
        "故障日志检索服务：旧版检索读 `legacy_summary`（需带客户端唯一 query_id，"
        "支持按号取消），新版检索读 `summary`；"
        "`POST /api/retirement` 受理 legacy_summary 列退役，两个实例通过共享数据库协调。"
    ),
    lifespan=lifespan,
)


# ---------------------------------------------------------------- helpers

def _reject_if_closed(status: str) -> None:
    """退役受理后旧版检索立即拒单（retiring=409 临时关闭，retired=410 永久关闭）。"""
    if status == "retiring":
        raise HTTPException(
            status_code=409,
            detail="legacy_summary 退役已受理，旧版检索不再接受新请求（新版 GET /api/v2/fault-logs 仍可用）",
        )
    if status == "retired":
        raise HTTPException(
            status_code=410,
            detail="legacy_summary 已退役删除，旧版检索永久关闭（请使用 GET /api/v2/fault-logs）",
        )


async def _state_payload(conn) -> dict[str, Any]:
    row = await conn.fetchrow("SELECT * FROM retirement_state WHERE id = 1")
    col_present = await conn.fetchval(
        """SELECT EXISTS (
                 SELECT 1 FROM information_schema.columns
                  WHERE table_schema = current_schema()
                    AND table_name = 'fault_logs'
                    AND column_name = 'legacy_summary')"""
    )
    return {
        "status": row["status"],
        "request_id": str(row["request_id"]) if row["request_id"] else None,
        "requested_at": row["requested_at"],
        "updated_at": row["updated_at"],
        "failed_attempts": row["failed_attempts"],
        "last_error": row["last_error"],
        "legacy_column_present": col_present,
        "served_by": INSTANCE_ID,
    }


# ---------------------------------------------------------------- endpoints

@app.get("/healthz", tags=["meta"], summary="健康检查")
async def healthz():
    return {"status": "ok", "instance": INSTANCE_ID}


@app.get("/api/v1/legacy/fault-logs", tags=["legacy"], summary="旧版检索（读 legacy_summary，带查询号，可取消）")
async def legacy_search(
    request: Request,
    query_id: str = Query(
        ...,
        description="客户端唯一查询号：同号全局只能接纳一次，也是取消 API 的凭据",
    ),
    severity: str | None = Query(None, description="按严重级别过滤：INFO/WARN/ERROR/CRITICAL"),
    limit: int = Query(50, ge=1, le=500),
    slow_ms: int = Query(
        0, ge=0, le=MAX_SLOW_MS,
        description="演示/自测用：让本次读取慢 slow_ms 毫秒，模拟长查询",
    ),
):
    """读取 `legacy_summary` 列。退役受理（retiring）后返回 409，退役完成（retired）后返回 410。

    已接纳的请求受共享咨询锁保护：退役任务会等它们读完（或被取消回滚）才删列。
    被取消 API 实际中止时返回 **503 + status=cancelled**，事务回滚、读锁与咨询锁立即释放。
    """
    _validate_query_id(query_id)
    pool = request.app.state.pool
    control: ControlDB = request.app.state.control
    registry: dict[str, QueryEntry] = request.app.state.query_registry

    async with pool.acquire() as conn:
        # 快速预检：绝大多数情况下立即拒绝，不必排队等锁。
        status = await conn.fetchval("SELECT status FROM retirement_state WHERE id = 1")
        _reject_if_closed(status)

        # 唯一接纳：共享表主键保证同号在两个实例上全局只接纳一次。
        try:
            await conn.execute(
                """
                INSERT INTO legacy_queries (query_id, instance_id, backend_pid)
                VALUES ($1, $2, pg_backend_pid())
                """,
                query_id,
                INSTANCE_ID,
            )
        except asyncpg.UniqueViolationError:
            raise HTTPException(
                status_code=409,
                detail=f"查询号 {query_id} 已被接纳过，同号只能接纳一次",
            )

        # 进程内在途登记：取消信号只发给登记表里仍存在的 query_id；
        # 本协程结束并归还连接前就会摘除，连接复用后不可能误伤下一条查询。
        entry = QueryEntry(query_id=query_id, backend_pid=conn.get_server_pid())
        registry[query_id] = entry

        rows = None
        # outcome 仅取四值：rejected_inside（锁内拒单，删行）/ cancelled / failed / completed
        outcome = "completed"
        try:
            async with conn.transaction():
                # 共享咨询锁：与退役任务的排他锁互斥；锁内权威复检状态，消除竞态。
                await conn.execute("SELECT pg_advisory_xact_lock_shared($1)", ADVISORY_LOCK_KEY)
                status = await conn.fetchval("SELECT status FROM retirement_state WHERE id = 1")
                try:
                    _reject_if_closed(status)
                except HTTPException:
                    # 预检与锁内复检之间退役被受理：本请求未执行，登记行直接删除。
                    outcome = "rejected_inside"
                    raise
                if slow_ms:
                    await conn.execute("SELECT pg_sleep($1)", slow_ms / 1000.0)
                rows = await conn.fetch(
                    """
                    SELECT id, created_at, severity, service, message, legacy_summary
                      FROM fault_logs
                     WHERE ($1::text IS NULL OR severity = $1)
                     ORDER BY id
                     LIMIT $2
                    """,
                    severity,
                    limit,
                )
        except asyncpg.QueryCanceledError:
            # 事务随异常自动 ROLLBACK：共享咨询锁与读锁在此刻已经释放。
            outcome = "cancelled"
        except HTTPException:
            # 锁内拒单：事务回滚、登记行将在 finally 删除，原样向上抛 409/410。
            raise
        except Exception:
            # 取消以外的数据库错误：事务回滚，如实标 failed，不伪装成 completed。
            outcome = "failed"
            raise
        finally:
            # 先摘进程内登记（杜绝信号落到复用后的连接），再落共享终态。
            registry.pop(query_id, None)
            entry.done.set()
            try:
                if outcome == "rejected_inside":
                    await control.execute(
                        "DELETE FROM legacy_queries WHERE query_id = $1", query_id
                    )
                else:
                    await control.execute(
                        """
                        UPDATE legacy_queries
                           SET status = $2, finished_at = now()
                         WHERE query_id = $1
                           AND status NOT IN ('cancelled', 'completed',
                                              'failed', 'interrupted')
                        """,
                        query_id,
                        outcome,
                    )
                # 消耗可能在收尾窗口滞留的取消信号，保证连接回池后是干净连接。
                try:
                    await conn.fetchval("SELECT 1")
                except asyncpg.QueryCanceledError:
                    pass
            except Exception:  # noqa: BLE001 - 收尾失败不能掩盖原始业务异常/取消响应
                pass

        if outcome == "cancelled":
            return JSONResponse(
                status_code=503,
                content={
                    "status": "cancelled",
                    "query_id": query_id,
                    "instance": INSTANCE_ID,
                    "detail": "该查询已被取消 API 中止，事务已回滚，锁已释放",
                },
            )

    return {
        "status": "completed",
        "query_id": query_id,
        "instance": INSTANCE_ID,
        "source_column": "legacy_summary",
        "count": len(rows),
        "items": [dict(r) for r in rows],
    }


@app.post(
    "/api/v1/legacy/queries/{query_id}/cancel",
    tags=["legacy"],
    summary="按查询号取消旧版检索（可发往任一实例）",
    responses={
        200: {"description": "取消请求处理结果（cancelled=true 表示确认中止；否则见 state/detail）"},
        404: {"description": "查询号未知（从未被任一实例接纳）"},
        422: {"description": "query_id 格式非法"},
    },
)
async def cancel_legacy_query(request: Request, query_id: str):
    """取消指定查询号尚在执行的查询。

    - 同号重复取消安全（幂等）：已取消/已完成则如实报告，不产生新效果；
    - 可发往任一实例：属主是其他实例时经 pg_notify 路由，不占用查询池连接；
    - 只有 ``cancelled=true`` 才代表取消成功——此时原事务已回滚、读锁与咨询锁已释放；
    - 等待期内未见到终态时返回 ``acknowledged=false`` 并附当前 state，绝不虚报成功。
    """
    _validate_query_id(query_id)
    app = request.app
    control: ControlDB = app.state.control

    row = await control.fetchrow(
        "SELECT query_id, instance_id, backend_pid, status "
        "FROM legacy_queries WHERE query_id = $1",
        query_id,
    )
    if row is None:
        raise HTTPException(status_code=404, detail=f"未知查询号 {query_id}：从未被任一实例接纳")

    if row["status"] in ("cancelled", "completed", "interrupted", "failed"):
        detail = {
            "cancelled": "该查询此前已被取消；本次重复调用不产生新效果（不会影响连接上的后续查询）",
            "completed": "查询已正常结束，取消不产生任何效果（不会影响连接上的后续查询）",
            "interrupted": "属主实例曾在查询结束前中断，该查询未确认取消，已按 interrupted 终态对账",
            "failed": "查询已因取消以外的错误结束（事务已回滚），取消不产生任何效果",
        }[row["status"]]
        return {
            "query_id": query_id,
            "state": row["status"],
            # cancelled=true 仅在“本次调用实际促成中止”时给出；终态上的重复调用一律 false。
            "cancelled": False,
            "acknowledged": True,
            "served_by": INSTANCE_ID,
            "owner_instance": row["instance_id"],
            "detail": detail,
        }

    # 在途：先落共享取消标记（重复取消幂等），再向属主实例发信号。
    await control.execute(
        """
        UPDATE legacy_queries
           SET status = 'cancel_requested'
         WHERE query_id = $1 AND status IN ('accepted', 'cancel_requested')
        """,
        query_id,
    )
    owner_is_local = row["instance_id"] == INSTANCE_ID
    if owner_is_local:
        await signal_cancel(app, query_id)
    else:
        await control.execute(
            "SELECT pg_notify($1, $2)", NOTIFY_CHANNEL, query_id
        )

    terminal = await wait_terminal(app, query_id, owner_is_local)
    if terminal is None:
        current = await control.fetchrow(
            "SELECT status, instance_id FROM legacy_queries WHERE query_id = $1",
            query_id,
        )
        return JSONResponse(
            status_code=200,
            content={
                "query_id": query_id,
                "state": current["status"],
                "cancelled": False,
                "acknowledged": False,
                "served_by": INSTANCE_ID,
                "owner_instance": current["instance_id"],
                "detail": "中止尚未确认：取消信号已送达属主实例，但等待窗口内查询未进入终态，请用 GET 状态接口继续观察",
            },
        )

    if terminal["status"] == "cancelled":
        return {
            "query_id": query_id,
            "state": "cancelled",
            "cancelled": True,
            "acknowledged": True,
            "served_by": INSTANCE_ID,
            "owner_instance": terminal["instance_id"],
            "detail": "查询确认已中止：事务回滚完成，读锁与咨询锁已释放，连接归还连接池",
        }
    if terminal["status"] == "interrupted":
        return {
            "query_id": query_id,
            "state": "interrupted",
            "cancelled": False,
            "acknowledged": True,
            "served_by": INSTANCE_ID,
            "owner_instance": terminal["instance_id"],
            "detail": "属主实例在查询结束前中断，无法确认取消，已按 interrupted 终态对账（未虚报取消成功）",
        }
    if terminal["status"] == "failed":
        return {
            "query_id": query_id,
            "state": "failed",
            "cancelled": False,
            "acknowledged": True,
            "served_by": INSTANCE_ID,
            "owner_instance": terminal["instance_id"],
            "detail": "查询因取消以外的错误结束，事务已回滚，未被取消",
        }
    return {
        "query_id": query_id,
        "state": "completed",
        "cancelled": False,
        "acknowledged": True,
        "served_by": INSTANCE_ID,
        "owner_instance": terminal["instance_id"],
        "detail": "查询在取消信号生效前已正常结束，未被取消",
    }


@app.get(
    "/api/v1/legacy/queries/{query_id}",
    tags=["legacy"],
    summary="按查询号查询旧版检索状态",
)
async def get_legacy_query(request: Request, query_id: str):
    """返回查询号的共享登记状态：accepted/cancel_requested/cancelled/completed。"""
    _validate_query_id(query_id)
    row = await request.app.state.control.fetchrow(
        "SELECT query_id, instance_id, backend_pid, status, accepted_at, finished_at "
        "FROM legacy_queries WHERE query_id = $1",
        query_id,
    )
    if row is None:
        raise HTTPException(status_code=404, detail=f"未知查询号 {query_id}：从未被任一实例接纳")
    payload = dict(row)
    payload["served_by"] = INSTANCE_ID
    payload["running_locally"] = query_id in request.app.state.query_registry
    return payload


@app.get("/api/v2/fault-logs", tags=["v2"], summary="新版检索（读 summary）")
async def v2_search(
    request: Request,
    severity: str | None = Query(None, description="按严重级别过滤：INFO/WARN/ERROR/CRITICAL"),
    limit: int = Query(50, ge=1, le=500),
):
    """读取 `summary` 列。退役的任何阶段都可用。"""
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, created_at, severity, service, message, summary
              FROM fault_logs
             WHERE ($1::text IS NULL OR severity = $1)
             ORDER BY id
             LIMIT $2
            """,
            severity,
            limit,
        )
    return {
        "instance": INSTANCE_ID,
        "source_column": "summary",
        "count": len(rows),
        "items": [dict(r) for r in rows],
    }


@app.post(
    "/api/retirement",
    tags=["retirement"],
    summary="受理 legacy_summary 列退役（幂等，可重试）",
    responses={
        202: {"description": "退役已受理/正在进行（status=retiring）"},
        200: {"description": "退役此前已完成（status=retired）"},
    },
)
async def request_retirement(request: Request):
    """把状态机推进到 `retiring` 并确保本实例有后台任务在执行删列。

    - 并发调用：数据库行锁串行化，只有第一个请求做状态迁移，其余看到既有状态；
    - 重试：已处于 retiring 时再次调用会（在本实例）补起一个后台任务，不会重复删列，
      也不会把未完成误报为完成——响应始终反映数据库里的真实状态。
    """
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                "SELECT status FROM retirement_state WHERE id = 1 FOR UPDATE"
            )
            if row["status"] == "active":
                await conn.execute(
                    """
                    UPDATE retirement_state
                       SET status = 'retiring', request_id = $1,
                           requested_at = now(), updated_at = now(), last_error = NULL
                     WHERE id = 1
                    """,
                    uuid.uuid4(),
                )
        payload = await _state_payload(conn)
    if payload["status"] == "retiring":
        await ensure_retirement_worker(request.app)
        return JSONResponse(jsonable_encoder(payload), status_code=202)
    return JSONResponse(jsonable_encoder(payload), status_code=200)


@app.get("/api/retirement", tags=["retirement"], summary="查询退役状态")
async def get_retirement(request: Request):
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        return await _state_payload(conn)


# ---------------------------------------------------------------- test hooks

if ENABLE_TEST_HOOKS:

    @app.post(
        "/api/test-hooks/fail-next-drops",
        tags=["test-hooks"],
        summary="[仅测试] 让本实例接下来 times 次删列尝试失败",
    )
    async def fail_next_drops(request: Request, times: int = Query(1, ge=1, le=10)):
        """用于演示/自测“删列失败保留 retiring 状态、可重试”的路径。仅在
        环境变量 ENABLE_TEST_HOOKS=true 时注册。"""
        request.app.state.fail_drops_remaining = times
        return {"armed_failures": times, "instance": INSTANCE_ID}
