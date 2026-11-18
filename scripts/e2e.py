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
        r = get(f"{base}/api/v1/legacy/fault-logs")
        body = r.json()
        ck(r.status_code == 200, f"{name} 旧版检索 200", f"got {r.status_code}")
        ck(body["count"] > 0 and "legacy_summary" in body["items"][0],
           f"{name} 旧版返回 legacy_summary 数据")
        ck(body.get("source_column") == "legacy_summary", f"{name} 旧版来源列正确")

        r = get(f"{base}/api/v2/fault-logs")
        body = r.json()
        ck(r.status_code == 200, f"{name} 新版检索 200", f"got {r.status_code}")
        ck(body["count"] > 0 and "summary" in body["items"][0],
           f"{name} 新版返回 summary 数据")

        r = get(f"{base}/api/v1/legacy/fault-logs", params={"severity": "CRITICAL"})
        ck(r.status_code == 200 and all(i["severity"] == "CRITICAL" for i in r.json()["items"]),
           f"{name} 旧版按 severity 过滤")

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
        r = get(f"{base}/api/v1/legacy/fault-logs")
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
        r = get(f"{base}/api/v1/legacy/fault-logs")
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
                              params={"slow_ms": 6000})

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
        r = get(f"{base}/api/v1/legacy/fault-logs")
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


ALL_SCENARIOS = [
    ("basic", scenario_basic),
    ("failure_and_retry", scenario_failure_and_retry),
    ("interleave_and_concurrent", scenario_interleave_and_concurrent),
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
