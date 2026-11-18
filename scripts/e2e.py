#!/usr/bin/env python3
"""端到端自测场景库：对两个运行中的服务实例执行（compose 或本地进程均可）。

用法：
    python3 scripts/e2e.py --app1 http://localhost:8001 --app2 http://localhost:8002 \
        --pg-dsn postgresql://ops:ops_password@localhost:5432/faultdb

--pg-dsn 用于在场景之间重置数据库（重新执行 db/init/01_init.sql）。
被 local_e2e.py 导入复用，也可独立对 docker compose 起的服务运行。
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import threading
import time

import asyncpg
import httpx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INIT_SQL = os.path.join(ROOT, "db", "init", "01_init.sql")

TIMEOUT = httpx.Timeout(30.0, connect=5.0)

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"


class Check:
    """断言收集器：记录每条断言，最后汇总，失败即非零退出。"""

    def __init__(self) -> None:
        self.failures: list[str] = []
        self.count = 0

    def __call__(self, cond: bool, label: str, extra: str = "") -> None:
        self.count += 1
        if cond:
            print(f"  [{PASS}] {label}")
        else:
            msg = f"{label}  {extra}"
            self.failures.append(msg)
            print(f"  [{FAIL}] {label}  {extra}")


def get(url: str, **kw) -> httpx.Response:
    return httpx.get(url, timeout=TIMEOUT, **kw)


def post(url: str, **kw) -> httpx.Response:
    return httpx.post(url, timeout=TIMEOUT, **kw)


def wait_until(fn, timeout: float, interval: float = 0.4, desc: str = "") -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if fn():
                return True
        except Exception:
            pass
        time.sleep(interval)
    print(f"  .. wait_until 超时: {desc}")
    return False


def reset_db(dsn: str) -> None:
    """重新执行初始化 SQL，把库恢复到 active 初始状态。"""
    with open(INIT_SQL, encoding="utf-8") as f:
        sql = f.read()

    async def _run() -> None:
        conn = await asyncpg.connect(dsn)
        try:
            await conn.execute(sql)  # 无参数 => 简单查询协议，可多语句
        finally:
            await conn.close()

    asyncio.run(_run())


def retirement_state(base: str) -> dict:
    return get(f"{base}/api/retirement").json()


# ---------------------------------------------------------------- scenarios

def scenario_basic(app1: str, app2: str, ck: Check) -> None:
    print("\n== 场景 1：初始状态，两实例旧版/新版检索均可用 ==")
    for name, base in (("app1", app1), ("app2", app2)):
        r = get(f"{base}/api/v1/legacy/fault-logs",
                params={"query_id": f"basic-{name}-0001"})
        body = r.json()
        ck(r.status_code == 200, f"{name} 旧版检索 200", f"got {r.status_code} {r.text[:120]}")
        ck(body["count"] > 0 and "legacy_summary" in body["items"][0],
           f"{name} 旧版返回 legacy_summary 数据")
        ck(body.get("source_column") == "legacy_summary" and body.get("status") == "completed",
           f"{name} 旧版来源列正确且标记 completed")

        r = get(f"{base}/api/v2/fault-logs")
        body = r.json()
        ck(r.status_code == 200, f"{name} 新版检索 200", f"got {r.status_code}")
        ck(body["count"] > 0 and "summary" in body["items"][0],
           f"{name} 新版返回 summary 数据")

        r = get(f"{base}/api/v1/legacy/fault-logs",
                params={"severity": "CRITICAL", "query_id": f"basic-{name}-crit"})
        ck(r.status_code == 200 and all(i["severity"] == "CRITICAL" for i in r.json()["items"]),
           f"{name} 旧版按 severity 过滤")

        # 同号只能接纳一次：重复使用相同 query_id -> 409
        r = get(f"{base}/api/v1/legacy/fault-logs",
                params={"query_id": f"basic-{name}-0001"})
        ck(r.status_code == 409, f"{name} 重复查询号被拒(409，同号只接纳一次)",
           f"got {r.status_code}")

    # 跨实例同号也必须冲突（全局唯一）
    r = get(f"{app2}/api/v1/legacy/fault-logs", params={"query_id": "basic-app1-0001"})
    ck(r.status_code == 409, "跨实例重复查询号同样被拒(409)", f"got {r.status_code}")
    # 非法 query_id -> 422
    r = get(f"{app1}/api/v1/legacy/fault-logs", params={"query_id": "bad id"})
    ck(r.status_code == 422, "非法 query_id 返回 422", f"got {r.status_code}")

    st = retirement_state(base)
    ck(st["status"] == "active" and st["legacy_column_present"],
       f"{name} 退役状态为 active 且旧列存在")


def scenario_failure_and_retry(app1: str, app2: str, ck: Check) -> None:
    print("\n== 场景 2：删列失败 -> 保留 retiring 拒单 -> 跨实例重试成功 ==")
    # 武装 app1：下一次删列必失败（测试钩子）
    r = post(f"{app1}/api/test-hooks/fail-next-drops", params={"times": 1})
    ck(r.status_code == 200, "app1 测试钩子已武装", f"got {r.status_code}")

    r = post(f"{app1}/api/retirement")
    ck(r.status_code == 202 and r.json()["status"] == "retiring",
       "app1 受理退役返回 202/retiring", f"got {r.status_code} {r.text[:120]}")

    # 等失败被记录
    ok = wait_until(lambda: retirement_state(app1)["failed_attempts"] >= 1, 15,
                    desc="failed_attempts>=1")
    ck(ok, "删列失败被记录（failed_attempts>=1）")
    st = retirement_state(app2)
    ck(st["status"] == "retiring", "失败后状态仍为 retiring（未误报完成）")
    ck(st["last_error"] and "simulated" in st["last_error"], "last_error 记录了失败原因",
       f"last_error={st['last_error']!r}")
    ck(st["legacy_column_present"], "失败后旧列仍在")

    # 失败保留 retiring 期间：两实例旧版都拒单，新版可用
    for name, base in (("app1", app1), ("app2", app2)):
        r = get(f"{base}/api/v1/legacy/fault-logs",
                params={"query_id": f"retry-{name}-reject1"})
        ck(r.status_code == 409, f"{name} retiring 期间旧版拒单(409)", f"got {r.status_code}")
        r = get(f"{base}/api/v2/fault-logs")
        ck(r.status_code == 200, f"{name} retiring 期间新版可用(200)", f"got {r.status_code}")

    # 从另一个实例重试（其未武装失败钩子）
    r = post(f"{app2}/api/retirement")
    ck(r.status_code == 202, "app2 重试退役被受理(202)", f"got {r.status_code} {r.text[:120]}")
    ok = wait_until(lambda: retirement_state(app2)["status"] == "retired", 20,
                    desc="status==retired")
    ck(ok, "重试后退役完成(retired)")
    st = retirement_state(app1)
    ck(not st["legacy_column_present"], "旧列已删除（两实例视图一致）")
    ck(st["failed_attempts"] >= 1, "失败历史保留可查")

    for name, base in (("app1", app1), ("app2", app2)):
        r = get(f"{base}/api/v1/legacy/fault-logs",
                params={"query_id": f"retry-{name}-gone001"})
        ck(r.status_code == 410, f"{name} 退役完成后旧版 410", f"got {r.status_code}")
        r = get(f"{base}/api/v2/fault-logs")
        ck(r.status_code == 200 and r.json()["count"] > 0, f"{name} 退役完成后新版仍可用")

    # 完成后再调用退役：幂等，不得重复删列/报错
    r = post(f"{app1}/api/retirement")
    ck(r.status_code == 200 and r.json()["status"] == "retired",
       "完成后重复调用退役：幂等 200/retired", f"got {r.status_code} {r.text[:120]}")


def scenario_interleave_and_concurrent(app1: str, app2: str, ck: Check) -> None:
    print("\n== 场景 3：长查询与退役交错 + 双实例并发退役 ==")
    results: dict[str, httpx.Response] = {}

    def slow_legacy() -> None:
        results["slow"] = get(f"{app1}/api/v1/legacy/fault-logs",
                              params={"slow_ms": 6000, "query_id": "interleave-slow-001"})

    t = threading.Thread(target=slow_legacy)
    t.start()
    time.sleep(1.0)  # 确保慢查询已被接纳（持共享锁）

    posts: dict[str, httpx.Response] = {}

    def do_post(key: str, base: str) -> None:
        posts[key] = post(f"{base}/api/retirement")

    t1 = threading.Thread(target=do_post, args=("p1", app1))
    t2 = threading.Thread(target=do_post, args=("p2", app2))
    t1.start(); t2.start(); t1.join(); t2.join()

    ck(posts["p1"].status_code == 202 and posts["p2"].status_code == 202,
       "并发退役两个请求均被受理(202)",
       f"p1={posts['p1'].status_code} p2={posts['p2'].status_code}")
    ck(posts["p1"].json()["status"] == "retiring" and posts["p2"].json()["status"] == "retiring",
       "并发退役均如实返回 retiring（无误报完成）",
       f"p1={posts['p1'].json()['status']} p2={posts['p2'].json()['status']}")
    ck(posts["p1"].json()["request_id"] == posts["p2"].json()["request_id"],
       "并发退役共享同一 request_id（只迁移一次状态）")

    # 退役受理后立即：两实例旧版拒单、新版可用、列仍在（慢查询还没读完）
    for name, base in (("app1", app1), ("app2", app2)):
        r = get(f"{base}/api/v1/legacy/fault-logs",
                params={"query_id": f"interleave-{name}-rej"})
        ck(r.status_code == 409, f"{name} 退役受理后立即拒单(409)", f"got {r.status_code}")
        r = get(f"{base}/api/v2/fault-logs")
        ck(r.status_code == 200, f"{name} 退役受理后新版仍可用(200)")
    st = retirement_state(app1)
    ck(st["status"] == "retiring" and st["legacy_column_present"],
       "慢查询在途期间：状态 retiring 且旧列尚未删除")

    # 已接纳的慢查询必须完整读完
    t.join(timeout=30)
    slow = results.get("slow")
    ck(slow is not None and slow.status_code == 200,
       "已接纳的长查询正常读完(200)", f"got {None if slow is None else slow.status_code}")
    if slow is not None and slow.status_code == 200:
        ck(slow.json()["count"] > 0 and "legacy_summary" in slow.json()["items"][0],
           "长查询返回完整 legacy_summary 数据")

    # 慢查询读完后删列才完成
    ok = wait_until(lambda: retirement_state(app2)["status"] == "retired", 25,
                    desc="status==retired")
    ck(ok, "长查询读完后退役完成(retired)")
    st = retirement_state(app1)
    ck(not st["legacy_column_present"], "旧列已删除")
    ck(st["failed_attempts"] == 0, "并发退役零失败（未重复删列）",
       f"failed_attempts={st['failed_attempts']}")


def _start_slow(base: str, query_id: str, slow_ms: int = 12000) -> tuple[threading.Thread, dict]:
    """发起一个慢旧版查询（线程 + 结果容器），返回后稍候由调用方确认在途。"""
    results: dict[str, httpx.Response] = {}

    def _run() -> None:
        results["resp"] = get(
            f"{base}/api/v1/legacy/fault-logs",
            params={"slow_ms": slow_ms, "query_id": query_id},
        )

    t = threading.Thread(target=_run)
    t.start()
    return t, results


def _wait_query_state(base: str, query_id: str, states: set[str], timeout: float = 8.0):
    """轮询状态接口直到进入 states，返回最终状态 dict；超时返回 None。"""
    def _snapshot() -> dict | None:
        r = get(f"{base}/api/v1/legacy/queries/{query_id}")
        if r.status_code == 200 and r.json().get("status") in states:
            return r.json()
        return None

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        snap = _snapshot()
        if snap is not None:
            return snap
        time.sleep(0.2)
    print(f"  .. 等待查询 {query_id} 进入 {states} 超时")
    return None


def scenario_cancel_basic(app1: str, app2: str, ck: Check) -> None:
    print("\n== 场景 4：查询号取消——同实例/跨实例、已结束不误伤、重复取消幂等 ==")

    # 5.1 同实例取消：app1 发慢查询，app1 自己取消
    t, res = _start_slow(app1, "cancel-local-0001")
    ck(_wait_query_state(app1, "cancel-local-0001", {"accepted", "cancel_requested"}) is not None,
       "同实例慢查询已登记为在途")
    r = post(f"{app1}/api/v1/legacy/queries/cancel-local-0001/cancel")
    body = r.json()
    ck(r.status_code == 200 and body.get("cancelled") is True and body.get("state") == "cancelled",
       "同实例取消明确报告 cancelled=true", f"got {r.status_code} {r.text[:160]}")
    t.join(timeout=20)
    resp = res.get("resp")
    ck(resp is not None and resp.status_code == 503 and resp.json().get("status") == "cancelled",
       "被取消的原查询返回 503/cancelled", f"got {None if resp is None else resp.status_code}")
    st = get(f"{app1}/api/v1/legacy/queries/cancel-local-0001").json()
    ck(st["status"] == "cancelled" and st["instance_id"] == "app1",
       "共享登记终态为 cancelled 且记录属主实例")
    # 重复取消幂等：不再产生效果，明确告知已结束
    r = post(f"{app1}/api/v1/legacy/queries/cancel-local-0001/cancel")
    ck(r.status_code == 200 and r.json()["cancelled"] is False
       and r.json()["state"] == "cancelled",
       "重复取消同号：幂等返回 cancelled=false 且 state=cancelled", f"got {r.text[:160]}")

    # 5.2 跨实例取消：app2 发慢查询，从 app1 取消（NOTIFY 路由）
    t, res = _start_slow(app2, "cancel-cross-0002")
    ck(_wait_query_state(app2, "cancel-cross-0002", {"accepted", "cancel_requested"}) is not None,
       "app2 慢查询已登记（属主 app2）")
    r = post(f"{app1}/api/v1/legacy/queries/cancel-cross-0002/cancel")
    body = r.json()
    ck(r.status_code == 200 and body.get("cancelled") is True
       and body.get("owner_instance") == "app2" and body.get("served_by") == "app1",
       "跨实例取消：app1 受理、路由到 app2 并确认 cancelled",
       f"got {r.status_code} {r.text[:160]}")
    t.join(timeout=20)
    resp = res.get("resp")
    ck(resp is not None and resp.status_code == 503 and resp.json()["status"] == "cancelled",
       "跨实例取消后原查询 503/cancelled",
       f"got {None if resp is None else resp.status_code}")

    # 5.3 已正常结束的查询不能再被取消，且连接复用后的下一条查询不受影响
    r = get(f"{app2}/api/v1/legacy/fault-logs", params={"query_id": "cancel-done-0003"})
    ck(r.status_code == 200 and r.json()["status"] == "completed",
       "短查询正常完成(200/completed)")
    r = post(f"{app2}/api/v1/legacy/queries/cancel-done-0003/cancel")
    ck(r.status_code == 200 and r.json()["cancelled"] is False
       and r.json()["state"] == "completed",
       "取消已结束查询：明确报 completed、未被取消", f"got {r.text[:160]}")
    # 连接复用：换一个新查询号走同一实例（很可能复用刚归还的连接），必须正常
    r = get(f"{app2}/api/v1/legacy/fault-logs",
            params={"query_id": "cancel-reuse-0004", "slow_ms": 1500})
    ck(r.status_code == 200 and r.json()["query_id"] == "cancel-reuse-0004"
       and r.json()["status"] == "completed",
       "连接复用后的下一条查询正常完成（未被上一个取消信号打断）",
       f"got {r.status_code} {r.text[:120]}")

    # 5.4 未知查询号 -> 404
    r = post(f"{app1}/api/v1/legacy/queries/never-seen-00001/cancel")
    ck(r.status_code == 404, "未知查询号取消返回 404", f"got {r.status_code}")

    # 5.5 取消进行中再取消（重复信号）仍只确认一次，状态不回退
    t, res = _start_slow(app1, "cancel-twice-0005")
    _wait_query_state(app1, "cancel-twice-0005", {"accepted", "cancel_requested"})
    r1 = post(f"{app1}/api/v1/legacy/queries/cancel-twice-0005/cancel")
    r2 = post(f"{app2}/api/v1/legacy/queries/cancel-twice-0005/cancel")  # 换实例重复
    ck(r1.json().get("cancelled") is True, "第一次取消确认 cancelled")
    ck(r2.status_code == 200 and r2.json()["state"] == "cancelled"
       and r2.json()["cancelled"] is False,
       "终态后跨实例重复取消幂等、不回退状态", f"got {r2.text[:160]}")
    t.join(timeout=20)


def scenario_cancel_unblocks_retirement(app1: str, app2: str, ck: Check) -> None:
    print("\n== 场景 5：取消在途旧查询释放锁，退役随后继续完成 ==")
    # 两个慢查询分别在两个实例上持共享咨询锁，把退役卡在 retiring
    t1, r1 = _start_slow(app1, "unblock-q1-0001", slow_ms=20000)
    t2, r2 = _start_slow(app2, "unblock-q2-0002", slow_ms=20000)
    ck(_wait_query_state(app1, "unblock-q1-0001", {"accepted", "cancel_requested"}) is not None,
       "app1 慢查询在途")
    ck(_wait_query_state(app2, "unblock-q2-0002", {"accepted", "cancel_requested"}) is not None,
       "app2 慢查询在途")

    r = post(f"{app1}/api/retirement")
    ck(r.status_code == 202 and r.json()["status"] == "retiring",
       "退役受理(202/retiring)，被在途查询阻塞", f"got {r.status_code} {r.text[:120]}")
    time.sleep(1.5)
    ck(retirement_state(app1)["status"] == "retiring"
       and retirement_state(app1)["legacy_column_present"],
       "查询未取消前：退役保持 retiring、旧列仍在")

    # 从“非属主侧”交错取消两个查询
    c1 = post(f"{app2}/api/v1/legacy/queries/unblock-q1-0001/cancel")  # 路由回 app1
    c2 = post(f"{app1}/api/v1/legacy/queries/unblock-q2-0002/cancel")  # 路由回 app2
    ck(c1.json().get("cancelled") is True and c2.json().get("cancelled") is True,
       "两个在途查询均确认 cancelled（事务/读锁已释放）",
       f"c1={c1.text[:120]} c2={c2.text[:120]}")
    t1.join(timeout=20); t2.join(timeout=20)
    ck(r1["resp"].status_code == 503 and r2["resp"].status_code == 503,
       "两个被取消查询都返回 503")

    ok = wait_until(lambda: retirement_state(app2)["status"] == "retired", 25,
                    desc="status==retired")
    ck(ok, "锁释放后退役自动继续至 retired")
    st = retirement_state(app1)
    ck(not st["legacy_column_present"], "旧列已删除（取消真正为退役放行）")

    # 取消之后：新版检索两实例仍可用；旧版按退役状态 410
    for name, base in (("app1", app1), ("app2", app2)):
        r = get(f"{base}/api/v2/fault-logs")
        ck(r.status_code == 200 and r.json()["count"] > 0, f"{name} 取消/退役后新版仍可用")
        r = get(f"{base}/api/v1/legacy/fault-logs",
                params={"query_id": f"unblock-{name}-gone"})
        ck(r.status_code == 410, f"{name} 退役完成后旧版 410", f"got {r.status_code}")


def scenario_cancel_with_full_pool(app1: str, app2: str, ck: Check) -> None:
    print("\n== 场景 6：查询池占满仍可取消 + 排队请求与连接复用交错 ==")
    # 应用连接池 max_size=8：打满 8 个慢查询，使第 9 个请求只能在池外排队
    N = 8
    threads, results, qids = [], [], []
    for i in range(N):
        qid = f"fullpool-slow-{i:02d}-xx"
        qids.append(qid)
        t, res = _start_slow(app1, qid, slow_ms=30000)
        threads.append(t)
        results.append(res)
    # 等 8 个查询全部进入在途（占满 8 条池连接）
    all_running = wait_until(
        lambda: all(
            get(f"{app1}/api/v1/legacy/queries/{q}").json().get("status")
            in {"accepted", "cancel_requested"}
            for q in qids
        ),
        12, interval=0.3, desc="8 个慢查询全部在途")
    ck(all_running, "8 个慢查询全部在途（查询池已满）")

    # 第 9 个请求：池满，只能排队等待连接，尚未登记
    queued: dict[str, httpx.Response] = {}

    def _queued() -> None:
        queued["resp"] = get(
            f"{app1}/api/v1/legacy/fault-logs",
            params={"slow_ms": 30000, "query_id": "fullpool-queued-09"},
        )

    tq = threading.Thread(target=_queued)
    tq.start()
    time.sleep(2.0)  # 确保它已在 pool.acquire() 上排队
    r = get(f"{app1}/api/v1/legacy/queries/fullpool-queued-09")
    ck(r.status_code == 404, "排队请求尚未登记（没有池连接可用）")

    # 关键断言：池满时取消 API 仍然立即可用（走独立控制连接，不参与池排队）
    t0 = time.monotonic()
    r = post(f"{app2}/api/v1/legacy/queries/fullpool-slow-00-xx/cancel")  # 跨实例
    elapsed = time.monotonic() - t0
    ck(r.status_code == 200 and r.json().get("cancelled") is True,
       "池满且跨实例：取消成功并明确确认 cancelled", f"got {r.text[:160]}")
    ck(elapsed < 8.0, f"池满时取消未被连接池阻塞（耗时 {elapsed:.1f}s < 8s）")

    # 再取消 3 个，腾出连接让排队请求被接纳，然后把它也取消（验证排队->接纳->取消链路）
    for q in qids[1:4]:
        rr = post(f"{app1}/api/v1/legacy/queries/{q}/cancel")
        ck(rr.json().get("cancelled") is True, f"池满取消 {q} 确认 cancelled",
           f"got {rr.text[:120]}")
    ck(_wait_query_state(app1, "fullpool-queued-09", {"accepted", "cancel_requested"},
                         timeout=12) is not None,
       "腾出连接后排队请求被接纳并登记")
    rr = post(f"{app2}/api/v1/legacy/queries/fullpool-queued-09/cancel")
    ck(rr.status_code == 200 and rr.json().get("cancelled") is True,
       "排队请求被接纳后同样可取消并确认", f"got {rr.text[:160]}")

    # 取消剩余慢查询，全部线程收尾
    for q in qids[4:]:
        post(f"{app1}/api/v1/legacy/queries/{q}/cancel")
    for t in threads:
        t.join(timeout=25)
    tq.join(timeout=25)
    ck(all(res["resp"].status_code == 503 for res in results),
       "8 个占池查询全部返回 503/cancelled")
    ck(queued["resp"].status_code == 503 and queued["resp"].json()["status"] == "cancelled",
       "排队请求最终也是 503/cancelled")

    # 连接复用交错：池排空后，立刻用新查询号打两轮请求，均不得被旧取消信号误伤
    ok_round1 = all(
        get(f"{app1}/api/v1/legacy/fault-logs",
            params={"query_id": f"reuse-after-full-{i:02d}"}).status_code == 200
        for i in range(9)
    )
    ck(ok_round1, "池排空后第一轮 9 个新查询全部 200（连接复用安全）")
    ok_round2 = all(
        get(f"{app2}/api/v2/fault-logs").status_code == 200
        for _ in range(4)
    )
    ck(ok_round2, "同期 app2 新版检索始终可用")
    # 旧号终态可查、不可再接纳
    r = get(f"{app1}/api/v1/legacy/fault-logs",
            params={"query_id": "fullpool-slow-00-xx"})
    ck(r.status_code == 409, "已取消的旧查询号不能被再次接纳(409)")
    st = get(f"{app1}/api/v1/legacy/queries/fullpool-slow-00-xx").json()
    ck(st["status"] == "cancelled", "旧号共享登记保留 cancelled 终态")


ALL_SCENARIOS = [
    ("basic", scenario_basic),
    ("failure_and_retry", scenario_failure_and_retry),
    ("interleave_and_concurrent", scenario_interleave_and_concurrent),
    ("cancel_basic", scenario_cancel_basic),
    ("cancel_unblocks_retirement", scenario_cancel_unblocks_retirement),
    ("cancel_with_full_pool", scenario_cancel_with_full_pool),
]


def run_scenarios(app1: str, app2: str, dsn: str, names: list[str], ck: Check) -> None:
    selected = [s for s in ALL_SCENARIOS if s[0] in names] if names else ALL_SCENARIOS
    for name, fn in selected:
        print(f"  .. 重置数据库（场景 {name}）")
        reset_db(dsn)
        fn(app1, app2, ck)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--app1", default="http://localhost:8001")
    ap.add_argument("--app2", default="http://localhost:8002")
    ap.add_argument("--pg-dsn", default="postgresql://ops:ops_password@localhost:5432/faultdb")
    ap.add_argument("--scenario", action="append", default=[],
                    help="只跑指定场景（可多次给出）；默认全部")
    args = ap.parse_args()

    ck = Check()
    run_scenarios(args.app1, args.app2, args.pg_dsn, args.scenario, ck)
    print(f"\n==== 断言 {ck.count} 条，失败 {len(ck.failures)} 条 ====")
    if ck.failures:
        for f in ck.failures:
            print(f"  失败: {f}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
