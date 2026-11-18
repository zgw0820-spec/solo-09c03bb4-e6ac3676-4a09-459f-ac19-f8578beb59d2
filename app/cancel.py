"""旧版检索的客户端查询号登记与取消协调（双实例共享一个 PostgreSQL）。

机制概述
========

1. **共享登记表 ``legacy_queries``**：每条被接纳的旧版检索占一个 ``query_id``
   （主键），记录接纳实例 ``instance_id`` 与后端进程号 ``backend_pid``。
   主键约束保证同一个查询号在两个实例上全局只能接纳一次。

2. **进程内在途登记表**（本模块的 ``query_registry``）：只有查询协程自己
   在持有连接期间登记/清除条目。取消信号发出前必须在此表核对到该 pid
   当前确实仍在执行这个 query_id —— 查询结束、连接归还连接池之前条目就被
   清除，因此连接复用后绝不可能误伤下一条查询。

3. **跨实例路由用 LISTEN/NOTIFY**：取消请求落到任一实例均可。若查询属主
   是另一个实例，先把共享行置为 ``cancel_requested`` 再 ``pg_notify``，
   属主实例的监听协程收到后执行本进程核对 + ``pg_cancel_backend``。

4. **控制连接与监听连接独立于查询连接池**：查询池被慢查询占满时，取消 API
   依然能立刻登记状态、发信号和等待确认，不参与连接池排队。

5. **终态与锁释放**：查询协程在 ``finally``（仍持有连接、尚未还池）中清除
   进程内登记、把共享行置为终态（``cancelled``/``completed``/``failed``），
   并执行一条 ``SELECT 1`` 消耗可能滞留的取消信号。取消导致事务回滚，共享咨询锁
   与读锁随事务结束释放，退役任务因此可以继续推进。属主进程崩溃留下的残留登记
   由启动对账置为 ``interrupted``（见 ``reconcile_stale_queries``）。
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass, field
from typing import Any

import asyncpg

log = logging.getLogger("cancel")

# pg_notify 通道名（两个实例必须一致）。
NOTIFY_CHANNEL = "legacy_query_cancel"

# 取消确认的等待上限：超过后如实返回“未确认”，由调用方凭状态接口继续观察。
CANCEL_WAIT_TIMEOUT = 10.0
# interrupted：属主进程在查询结束前消亡（崩溃/被 kill），启动对账时置位的终态。
# failed：查询协程因取消以外的数据库错误结束，事务已回滚。
TERMINAL_STATUSES = frozenset(
    {"cancelled", "completed", "interrupted", "failed"}
)
RUNNING_STATUSES = frozenset({"accepted", "cancel_requested"})


async def reconcile_stale_queries(app) -> None:
    """启动对账：本实例残留的非终态登记，若其后端已死则置 interrupted。

    实例崩溃时在途查询随进程一起消亡，既不可能 completed 也无法确认 cancelled；
    不把它们虚构成取消成功，而是如实标记 interrupted，避免查询号永久挂在
    “在途”状态、也避免同号无法再被解释清楚。

    活性探测不能只看 ``pg_stat_activity``：客户端进程被 SIGKILL 且走 Unix
    socket 时，数据库后端可能长时间收不到断连通知，作为“孤儿后端”继续留在
    pg_stat_activity 里。``pg_terminate_backend`` 是非阻塞函数：
    - 后端活着 -> 发 SIGTERM 并立即返回 true（后端收到信号后终止，事务回滚）；
    - 后端已死 -> 返回 false，无任何副作用。
    同角色后端之间默认有信号权限（pg_signal_backend 对同角色成员天然成立）。
    探测放在带重试的循环里，直到本实例名下没有非终态登记为止。
    """
    control: ControlDB = app.state.control
    signaled: set[int] = set()  # 本轮启动对账中已发过终止信号的后端，不重复发
    for _ in range(20):
        rows = await control.fetch(
            "SELECT query_id, backend_pid FROM legacy_queries "
            "WHERE instance_id = $1 AND status IN ('accepted', 'cancel_requested')",
            app.state.instance_id,
        )
        if not rows:
            return
        for row in rows:
            if row["backend_pid"] in signaled:
                # 上一轮已要求终止：检查后端是否真正退出，退出才如实置 interrupted。
                alive = await control.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1)",
                    row["backend_pid"],
                )
                if not alive:
                    await control.execute(
                        """
                        UPDATE legacy_queries
                           SET status = 'interrupted', finished_at = now()
                         WHERE query_id = $1
                           AND status IN ('accepted', 'cancel_requested')
                        """,
                        row["query_id"],
                    )
                    log.warning(
                        "startup reconciliation: %r backend pid=%s gone -> interrupted",
                        row["query_id"], row["backend_pid"],
                    )
                continue
            alive = await control.fetchval(
                "SELECT pg_terminate_backend($1)", row["backend_pid"]
            )
            if alive:
                # 后端此前还活着（正常情况只可能是上个进程留下的孤儿后端）：
                # 终止信号已送达，等它退出后在下一轮置 interrupted。
                signaled.add(row["backend_pid"])
            else:
                await control.execute(
                    """
                    UPDATE legacy_queries
                       SET status = 'interrupted', finished_at = now()
                     WHERE query_id = $1
                       AND status IN ('accepted', 'cancel_requested')
                    """,
                    row["query_id"],
                )
                log.warning(
                    "startup reconciliation: %r backend pid=%s already dead -> interrupted",
                    row["query_id"], row["backend_pid"],
                )
        await asyncio.sleep(0.5)


@dataclass
class QueryEntry:
    """一个在本进程内正在执行的旧版检索。"""

    query_id: str
    backend_pid: int
    cancel_requested: bool = False
    done: asyncio.Event = field(default_factory=asyncio.Event)


class ControlDB:
    """供取消/登记路径使用的专用连接，独立于 asyncpg 查询连接池。

    查询池占满时本连接仍可用，因此“池满仍可取消”不依赖连接池释放连接。
    带串行锁与断线重连，取消接口本身是低频操作。
    """

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._conn: asyncpg.Connection | None = None
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        if self._conn is None or self._conn.is_closed():
            self._conn = await asyncpg.connect(self._dsn)

    async def close(self) -> None:
        if self._conn is not None:
            with contextlib.suppress(Exception):
                await self._conn.close()
            self._conn = None

    async def _healthy(self) -> asyncpg.Connection:
        if self._conn is None or self._conn.is_closed():
            self._conn = await asyncpg.connect(self._dsn)
        return self._conn

    async def _call(self, method: str, *args: Any, **kwargs: Any):
        async with self._lock:
            conn = await self._healthy()
            try:
                return await getattr(conn, method)(*args, **kwargs)
            except (asyncpg.ConnectionDoesNotExistError, asyncpg.InterfaceError):
                log.warning("control connection lost, reconnecting")
                self._conn = await asyncpg.connect(self._dsn)
                return await getattr(self._conn, method)(*args, **kwargs)

    async def fetchrow(self, *args: Any, **kwargs: Any):
        return await self._call("fetchrow", *args, **kwargs)

    async def fetch(self, *args: Any, **kwargs: Any):
        return await self._call("fetch", *args, **kwargs)

    async def fetchval(self, *args: Any, **kwargs: Any):
        return await self._call("fetchval", *args, **kwargs)

    async def execute(self, *args: Any, **kwargs: Any):
        return await self._call("execute", *args, **kwargs)


# ----------------------------------------------------------------- listener

def _make_notify_handler(app):
    def _handler(conn, pid: int, channel: str, payload: str) -> None:
        # 停机过程中不再派生新任务，避免事件循环关闭时的 pending task 告警。
        if app.state.listener_stop.is_set():
            return
        # asyncpg 在监听连接的事件循环里回调；具体处理另起任务，避免阻塞监听。
        asyncio.create_task(_handle_notify(app, payload))

    return _handler


async def _handle_notify(app, query_id: str) -> None:
    try:
        await signal_cancel(app, query_id)
    except Exception:  # noqa: BLE001 - 监听协程不能因单条通知异常而退出
        log.exception("handle cancel notify failed for %r", query_id)


async def listener_supervisor(app) -> None:
    """维护监听连接：断线自动重连并重新 LISTEN。"""
    dsn = app.state.control_dsn
    stop_event: asyncio.Event = app.state.listener_stop
    while not stop_event.is_set():
        conn = None
        try:
            conn = await asyncpg.connect(dsn)
            app.state.listener_conn = conn
            await conn.add_listener(NOTIFY_CHANNEL, _make_notify_handler(app))
            log.info("cancel listener connected")
            while not stop_event.is_set() and not conn.is_closed():
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            log.exception("cancel listener error; will reconnect")
        finally:
            if conn is not None:
                with contextlib.suppress(Exception):
                    await conn.close()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(stop_event.wait(), timeout=0.5)


# ----------------------------------------------------------------- cancel ops

async def signal_cancel(app, query_id: str) -> bool:
    """属主实例侧的取消动作：核对在途登记后向对应后端发取消信号。

    只有进程内登记表里仍存在该 query_id 时才调用 ``pg_cancel_backend``：
    条目在查询协程释放连接回池之前就会被删除，因此不会把信号发给已结束
    或已被复用的连接。
    """
    entry: QueryEntry | None = app.state.query_registry.get(query_id)
    if entry is None:
        log.info("cancel signal skipped: %r not running on this instance", query_id)
        return False
    entry.cancel_requested = True
    ok = await app.state.control.fetchval(
        "SELECT pg_cancel_backend($1)", entry.backend_pid
    )
    log.info("pg_cancel_backend(pid=%s) for %r -> %s",
             entry.backend_pid, query_id, ok)
    return bool(ok)


async def wait_terminal(app, query_id: str, owner_is_local: bool,
                        timeout: float = CANCEL_WAIT_TIMEOUT) -> dict | None:
    """等待共享登记行进入终态，返回终态行；超时返回 None。

    本实例属主时优先等进程内 ``done`` 事件（更快），跨实例时轮询共享表；
    跨实例等待过半后补发一次 NOTIFY，兜底监听丢通知的竞态。
    """
    control: ControlDB = app.state.control
    deadline = asyncio.get_running_loop().time() + timeout
    renotified = False
    while True:
        row = await control.fetchrow(
            "SELECT query_id, instance_id, backend_pid, status "
            "FROM legacy_queries WHERE query_id = $1",
            query_id,
        )
        if row is not None and row["status"] in TERMINAL_STATUSES:
            return dict(row)
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            return None
        if owner_is_local:
            entry: QueryEntry | None = app.state.query_registry.get(query_id)
            if entry is not None:
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(entry.done.wait(), timeout=min(remaining, 1.0))
            else:
                await asyncio.sleep(0.05)
        else:
            if not renotified and remaining < timeout / 2:
                renotified = True
                with contextlib.suppress(Exception):
                    await control.execute(
                        "SELECT pg_notify($1, $2)", NOTIFY_CHANNEL, query_id
                    )
            await asyncio.sleep(0.1)
