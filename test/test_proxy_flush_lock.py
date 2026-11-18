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


# ---------------------------------------------------------------------------
# 心跳读快照：到期筛选 + pipeline 分片
# ---------------------------------------------------------------------------


class _FakePipeline:
    """记录命令并按 execute 批次返回预设结果的 pipeline 桩。"""

    def __init__(self, client: "_FakeRedis") -> None:
        self._client = client
        self.commands: list[tuple[str, tuple]] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False

    def zremrangebyscore(self, *args, **kwargs):
        self.commands.append(("zrem", args))

    def zrangebyscore(self, *args, **kwargs):
        self.commands.append(("zrange", args))

    def hgetall(self, key: str):
        self.commands.append(("h", (key,)))

    def smembers(self, key: str):
        self.commands.append(("s", (key,)))

    async def execute(self):
        batch = list(self.commands)
        self.commands.clear()
        self._client.batch_sizes.append(len(batch))
        # callable 表示「后续所有分片都按同一规则生成响应」，保留在队首复用
        if callable(self._client.responses[0]):
            response = self._client.responses[0]
        else:
            response = self._client.responses.pop(0)
        if callable(response):
            return [response(command) for command in batch]
        return response


class _FakeRedis:
    def __init__(self, *responses) -> None:
        self.responses = list(responses)
        self.batch_sizes: list[int] = []

    def pipeline(self, transaction: bool = False):
        return _FakePipeline(self)


async def test_persist_due_filter_uses_heartbeat_interval():
    """到期筛选按 heartbeat 记账；地址级 / 前缀级命名空间互不干扰。"""
    store = ProxyHealthStore()
    assert store._due_persist_members(["a", "b"], "a", 1800) == ["a", "b"]

    store.mark_persist_seen(["a"], ["p1"], heartbeat_seconds=1800)
    assert store._due_persist_members(["a", "b"], "a", 1800) == ["b"]
    assert store._due_persist_members(["p1", "p2"], "p", 1800) == ["p2"]
    # 同一字符串在不同命名空间下互不影响
    assert store._due_persist_members(["a"], "p", 1800) == ["a"]
    # 非正间隔 = 不做到期筛选（历史行为）
    assert store._due_persist_members(["a", "b"], "a", 0) == ["a", "b"]


async def test_read_persist_snapshot_chunks_pipeline(monkeypatch):
    """5000+ 命令必须拆成固定大小的批次逐组 await（单段同步解析 <100ms）。"""
    from CONFIG import CONFIG

    monkeypatch.setattr(CONFIG.proxy_health, "persist_pipeline_chunk", 2)
    now = int(time.time())
    detail = {"cd": str(now + 3600), "state": "quarantine", "cf": "1"}
    prefix_detail = {"cd": str(now + 3600), "state": "quarantine", "rl": "1"}

    def _second_pipeline_response(command: tuple[str, tuple]):
        kind, args = command
        if kind == "h" and "prefix_detail" in args[0]:
            return prefix_detail
        if kind == "h":
            return detail
        return {"http://1.2.3.4:8080"}

    fake = _FakeRedis(
        [0, ["k1", "k2"], 0, ["p1", "p2"]],
        _second_pipeline_response,
    )
    store = ProxyHealthStore()
    monkeypatch.setattr(store, "_get_client", lambda: fake, raising=False)

    snapshot = await store.read_persist_snapshot(limit=10, heartbeat_seconds=1800)

    assert snapshot.cooling_keys == ["k1", "k2"]
    assert snapshot.cooling_prefixes == ["p1", "p2"]
    assert snapshot.address.has("k1") and snapshot.address.has("k2")
    assert snapshot.prefixes.has("p1") and snapshot.prefixes.has("p2")
    assert snapshot.prefix_members["p1"] == ["http://1.2.3.4:8080"]
    # 第 1 段 4 条（zrem/zrange ×2）；第 2 段 6 条命令按 chunk=2 拆成 3 批
    assert fake.batch_sizes == [4, 2, 2, 2]
