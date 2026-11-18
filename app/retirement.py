"""legacy_summary 列的退役执行器（每实例一个后台任务，跨实例由数据库协调）。

协调协议（两个实例共享同一个 PostgreSQL 数据库）：

1. ``retirement_state`` 单行表保存状态机 ``active -> retiring -> retired``。
   受理退役时**先提交** ``retiring``，再启动本任务；因此两个实例的旧版检索
   都能立即看到状态并拒绝新请求。实例重启后若发现状态仍是 ``retiring``，
   会自动续跑本任务（见 ``app.main.lifespan``）。

2. PostgreSQL 事务级咨询锁（两个实例使用同一个 key）：
   - 旧版检索在事务内先取 ``pg_advisory_xact_lock_shared(KEY)``，再校验状态、
     再读 ``legacy_summary``；
   - 本任务在事务内取 ``pg_advisory_xact_lock(KEY)``（排他），因此必然等待
     所有已接纳的旧查询读完才执行 ``DROP COLUMN``；
   - PG 锁队列的公平性保证：排他请求排队后，新的共享锁请求排在它后面，
     不会插队（应用层的状态检查也会先把它们拒掉）。

3. ``DROP COLUMN IF EXISTS`` 与状态置 ``retired`` 在**同一事务**提交：
   崩溃则整体回滚，重试/并发不会重复删列，也不会误报完成。
"""
from __future__ import annotations

import asyncio
import logging

log = logging.getLogger("retirement")

# 两个实例共享的咨询锁 key（任意固定的 64 位整数）。
ADVISORY_LOCK_KEY = 2_026_111_801

_worker_task: "asyncio.Task | None" = None
_worker_lock = asyncio.Lock()


async def ensure_retirement_worker(app) -> None:
    """若本进程没有正在运行的退役任务则启动一个。

    幂等：重复调用、多实例并发调用都安全——各实例的任务会被咨询锁串行化，
    后进入者看到列已删/状态已置位时自然成为空操作。
    """
    global _worker_task
    async with _worker_lock:
        if _worker_task is None or _worker_task.done():
            _worker_task = asyncio.create_task(_run_retirement(app))
            log.info("retirement worker started")


async def _run_retirement(app) -> None:
    pool = app.state.pool
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                # 排他咨询锁：等待所有在途旧版查询（共享锁持有者）结束。
                await conn.execute("SELECT pg_advisory_xact_lock($1)", ADVISORY_LOCK_KEY)

                # —— 测试钩子（仅 ENABLE_TEST_HOOKS=true 时前端可触发）：模拟删列失败 ——
                if getattr(app.state, "fail_drops_remaining", 0) > 0:
                    app.state.fail_drops_remaining -= 1
                    raise RuntimeError("simulated DROP COLUMN failure (test hook)")

                # IF EXISTS：并发退役/重试时不会重复删列报错；
                # 与状态更新同事务：要么一起提交，要么一起回滚，不会误报完成。
                await conn.execute(
                    "ALTER TABLE fault_logs DROP COLUMN IF EXISTS legacy_summary"
                )
                await conn.execute(
                    """
                    UPDATE retirement_state
                       SET status = 'retired', updated_at = now(), last_error = NULL
                     WHERE id = 1 AND status = 'retiring'
                    """
                )
        log.info("retirement finished: legacy_summary dropped, status=retired")
    except Exception as exc:  # noqa: BLE001 - 任何失败都必须保留 retiring 状态以便重试
        log.error("retirement attempt failed: %r", exc)
        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE retirement_state
                       SET failed_attempts = failed_attempts + 1,
                           last_error = $1,
                           updated_at = now()
                     WHERE id = 1 AND status = 'retiring'
                    """,
                    f"{type(exc).__name__}: {exc}",
                )
        except Exception:  # noqa: BLE001
            log.exception("could not persist retirement failure")
