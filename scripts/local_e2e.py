#!/usr/bin/env python3
"""本地端到端自测编排器（本环境无 Docker，用真实 PostgreSQL 跑全部场景）：

  1. 用 pgserver（pip 包内嵌 PostgreSQL 16）初始化并启动一个数据库；
  2. 应用 db/init/01_init.sql 预置故障日志；
  3. 启动两个 uvicorn 实例（127.0.0.1:8001 / :8002）共享该库；
  4. 运行 scripts/e2e.py 的全部场景（含长查询交错、失败重试、并发退役）；
  5. 运行“退役途中实例崩溃重启”场景（kill -9 后重启，验证状态保留与自动续跑）。

用法： python3 scripts/local_e2e.py
"""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import threading
import time

import asyncpg
import httpx
import pgserver

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import e2e  # noqa: E402

PY = sys.executable
APP1 = "http://127.0.0.1:8001"
APP2 = "http://127.0.0.1:8002"


def wait_http(url: str, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if httpx.get(url, timeout=3).status_code == 200:
                return
        except Exception:
            pass
        time.sleep(0.3)
    raise RuntimeError(f"等待超时: {url}")


class AppInstance:
    def __init__(self, name: str, port: int, dsn: str, logdir: str) -> None:
        self.name, self.port, self.dsn = name, port, dsn
        self.logpath = os.path.join(logdir, f"{name}.log")
        self.proc: subprocess.Popen | None = None

    def start(self) -> None:
        env = dict(
            os.environ,
            DATABASE_URL=self.dsn,
            INSTANCE_ID=self.name,
            ENABLE_TEST_HOOKS="true",
            PYTHONPATH=ROOT,
        )
        logf = open(self.logpath, "ab")
        self.proc = subprocess.Popen(
            [PY, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(self.port)],
            cwd=ROOT, env=env, stdout=logf, stderr=subprocess.STDOUT,
        )
        wait_http(f"http://127.0.0.1:{self.port}/healthz")
        print(f"  .. {self.name} 已启动 (pid {self.proc.pid})")

    def kill(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.kill()  # SIGKILL：模拟宕机，不给优雅退出机会
            self.proc.wait()
            print(f"  .. {self.name} 已 kill -9")

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.kill()


def scenario_restart_mid_retirement(app1: AppInstance, app2: AppInstance,
                                    dsn: str, ck: e2e.Check) -> None:
    print("\n== 场景 4：退役途中实例崩溃重启 -> 状态保留、旧版不复单、自动续跑 ==")
    e2e.reset_db(dsn)
    results: dict[str, httpx.Response] = {}

    # 在幸存实例 app2 上发起长旧版查询（退役期间它一直持有共享锁）
    t = threading.Thread(
        target=lambda: results.update(
            slow=e2e.get(f"{APP2}/api/v1/legacy/fault-logs", params={"slow_ms": 9000})))
    t.start()
    time.sleep(1.5)  # 确保慢查询已被接纳

    r = e2e.post(f"{APP1}/api/retirement")  # 退役受理在 app1
    ck(r.status_code == 202 and r.json()["status"] == "retiring",
       "app1 受理退役(202/retiring)", f"got {r.status_code}")
    time.sleep(1.0)  # app1 的后台任务正在等咨询锁

    app1.kill()  # 退役途中 app1 宕机

    st = e2e.retirement_state(APP2)
    ck(st["status"] == "retiring", "app1 宕机后状态保留 retiring（未丢未误报）")
    ck(st["legacy_column_present"], "app1 宕机后旧列仍在（长查询未读完）")
    r = e2e.get(f"{APP2}/api/v1/legacy/fault-logs")
    ck(r.status_code == 409, "app1 宕机后 app2 旧版仍拒单(409)", f"got {r.status_code}")
    r = e2e.get(f"{APP2}/api/v2/fault-logs")
    ck(r.status_code == 200, "app1 宕机后 app2 新版仍可用(200)")

    app1.start()  # 重启 app1：启动时应发现 retiring 并自动续跑退役任务

    r = e2e.get(f"{APP1}/api/v1/legacy/fault-logs")
    ck(r.status_code == 409, "app1 重启后旧版仍拒单(409，不能重新接单)", f"got {r.status_code}")

    t.join(timeout=40)
    slow = results.get("slow")
    ck(slow is not None and slow.status_code == 200 and slow.json()["count"] > 0,
       "跨实例宕机期间，已接纳的长查询在 app2 上正常读完(200)",
       f"got {None if slow is None else slow.status_code}")

    ok = e2e.wait_until(lambda: e2e.retirement_state(APP2)["status"] == "retired", 40,
                        desc="重启后续跑至 retired")
    ck(ok, "app1 重启后自动续跑退役直至完成(retired)")
    st = e2e.retirement_state(APP1)
    ck(not st["legacy_column_present"], "旧列最终已删除")
    for name, base in (("app1", APP1), ("app2", APP2)):
        r = e2e.get(f"{base}/api/v1/legacy/fault-logs")
        ck(r.status_code == 410, f"{name} 最终旧版 410", f"got {r.status_code}")
        r = e2e.get(f"{base}/api/v2/fault-logs")
        ck(r.status_code == 200, f"{name} 最终新版仍可用(200)")


def main() -> int:
    workdir = tempfile.mkdtemp(prefix="faultlog-e2e-")
    print(f".. 工作目录: {workdir}")
    print(".. 启动内嵌 PostgreSQL (pgserver)")
    srv = pgserver.get_server(os.path.join(workdir, "pgdata"))
    pg_uri = srv.get_uri()          # postgresql://.../postgres
    pg_ver = srv.pg_version if hasattr(srv, "pg_version") else "?"
    print(f".. PostgreSQL 就绪: {pg_uri} (major={pg_ver})")

    async def setup_db() -> str:
        conn = await asyncpg.connect(pg_uri)
        try:
            await conn.execute("CREATE DATABASE faultdb")
        finally:
            await conn.close()
        # pgserver 走 unix socket：host 在 query 参数里，路径段是库名，只替换路径段
        from urllib.parse import urlparse, urlunparse
        return urlunparse(urlparse(pg_uri)._replace(path="/faultdb"))

    dsn = asyncio_run(setup_db())
    print(f".. faultdb 已创建: {dsn}")
    e2e.reset_db(dsn)  # 应用启动前建好 schema（compose 中由 initdb 脚本完成）
    print(".. schema 与预置数据已就绪")

    app1 = AppInstance("app1", 8001, dsn, workdir)
    app2 = AppInstance("app2", 8002, dsn, workdir)
    ck = e2e.Check()
    try:
        app1.start()
        app2.start()

        # 场景 1-3（每个非初始场景前自行重置数据库）
        e2e.run_scenarios(APP1, APP2, dsn, [], ck)

        # 场景 4：崩溃重启
        scenario_restart_mid_retirement(app1, app2, dsn, ck)
    finally:
        app1.stop()
        app2.stop()
        srv.cleanup()

    print(f"\n==== 断言 {ck.count} 条，失败 {len(ck.failures)} 条 ====")
    if ck.failures:
        for f in ck.failures:
            print(f"  失败: {f}")
        print(f".. 实例日志见 {workdir}/app1.log, app2.log")
        return 1
    print("全部场景通过。")
    return 0


def asyncio_run(coro):
    import asyncio
    return asyncio.run(coro)


if __name__ == "__main__":
    sys.exit(main())
