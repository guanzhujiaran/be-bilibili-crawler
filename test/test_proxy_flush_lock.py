# -*- coding: utf-8 -*-
"""proxy_tab 批量写死锁防护的单测（不连 Redis / MySQL）。

背景：`flush_proxy_health`（按 `computed_proxy_str` 索引批量改 status/update_ts）与
`refresh_proxy`（按 `status` 索引批量复位）并发时，两个事务顺着不同索引去改同一批行
的二级索引条目，形成 AB-BA 循环等待 → MySQL 1213。

覆盖：
1. 建议锁：同一 scope 互斥、释放后可再抢、token 不匹配不误删；
2. `flush_proxy_health` 拿不到锁时直接跳过，不产生任何写；
3. 心跳去重谓词：`min_interval>0` 时只更新「状态变了 / update_ts 够旧」的行。
"""

import time

import pytest

from CONFIG import CONFIG
from Utils.代理.数据库操作.ProxyHealth import ProxyHealthStore
from Utils.代理.数据库操作.async_proxy_op_alchemy_mysql_ver import SQLHelper


class _FakeLockRedis:
    """只实现 SET NX / GET / DEL 的最小 Redis 桩。"""

    def __init__(self) -> None:
        self.data: dict[str, str] = {}

    async def set(self, key: str, value: str, nx: bool = False, ex: int | None = None):
        if nx and key in self.data:
            return None
        self.data[key] = value
        return True

    async def get(self, key: str):
        return self.data.get(key)

    async def delete(self, *keys: str) -> int:
        deleted = 0
        for key in keys:
            if key in self.data:
                del self.data[key]
                deleted += 1
        return deleted


async def test_write_lock_is_exclusive_and_releasable(monkeypatch):
    store = ProxyHealthStore()
    fake = _FakeLockRedis()
    monkeypatch.setattr(store, "_get_client", lambda: fake, raising=False)

    token = await store.acquire_write_lock("proxy_tab:batch_write", ttl=60, wait=0)
    assert token
    # 同一 scope 已被占用：wait=0 立即返回 None（不阻塞周期任务）
    assert await store.acquire_write_lock("proxy_tab:batch_write", ttl=60, wait=0) is None

    await store.release_write_lock("proxy_tab:batch_write", token)
    assert fake.data == {}
    # 释放后可以重新抢到
    assert await store.acquire_write_lock("proxy_tab:batch_write", ttl=60, wait=0)


async def test_release_ignores_foreign_token(monkeypatch):
    store = ProxyHealthStore()
    fake = _FakeLockRedis()
    monkeypatch.setattr(store, "_get_client", lambda: fake, raising=False)

    token = await store.acquire_write_lock("scope", ttl=60, wait=0)
    assert token
    # 锁过期后别人抢到了新 token：旧持有者不能把它删掉
    new_token = await store.acquire_write_lock("scope", ttl=60, wait=0)
    assert new_token is None
    fake.data[store._write_lock_key("scope")] = "someone-else"
    await store.release_write_lock("scope", token)
    assert fake.data[store._write_lock_key("scope")] == "someone-else"


async def test_flush_skips_when_lock_held(monkeypatch):
    """拿不到批量写锁时本轮心跳直接跳过，不产生任何 UPDATE。"""
    monkeypatch.setattr(SQLHelper, "_health_bootstrapped", True, raising=False)

    async def _acquire():
        return None

    async def _boom(*args, **kwargs):
        raise AssertionError("拿不到锁时不应该写库")

    monkeypatch.setattr(SQLHelper, "_acquire_batch_write_lock", _acquire)
    monkeypatch.setattr(SQLHelper, "_mark_proxy_unavailable", _boom)
    monkeypatch.setattr(SQLHelper, "_flush_prefix_cooling", _boom)

    assert await SQLHelper.flush_proxy_health() == 0


async def test_heartbeat_predicate_skips_fresh_rows():
    ts = int(time.time())
    chunk = ["http://1.2.3.4:8080", "http://5.6.7.8:8080"]
    plain = SQLHelper._build_unavailable_stmt(chunk, -412, ts)
    plain_sql = str(plain.compile(compile_kwargs={"literal_binds": True}))
    assert "update_ts <" not in plain_sql  # 复位后的立即回写：必须无条件更新

    heartbeat = SQLHelper._build_unavailable_stmt(
        chunk, -412, ts, min_interval=CONFIG.proxy_health.persist_heartbeat_seconds
    )
    heartbeat_sql = str(heartbeat.compile(compile_kwargs={"literal_binds": True}))
    # 心跳：已是目标状态且 update_ts 仍新鲜的行跳过，减少行锁数量
    assert "update_ts <" in heartbeat_sql
    assert "status !=" in heartbeat_sql


@pytest.mark.parametrize("interval", [0, -1])
async def test_zero_or_negative_interval_means_no_dedup(interval: int):
    ts = int(time.time())
    stmt = SQLHelper._build_unavailable_stmt(["http://1.2.3.4:8080"], -412, ts, interval)
    compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
    assert "update_ts <" not in compiled
