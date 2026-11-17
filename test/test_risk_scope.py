# -*- coding: utf-8 -*-
"""风控域（risk scope）与熔断窗口的回归单测。

覆盖：
1. ``risk_scope_of``：web / gRPC 按「通道 + 一级业务域」归并，去掉 query / 业务 id；
2. 前缀明细 / 冷却 zset 的 key 按 scope 隔离，全局桶沿用历史 key；
3. **跨 scope 不连坐**：gRPC 的 352 不会让 web 接口的前缀进入冷却；
4. 网络层失败仍写全局前缀桶（scope 只影响风控冷却维度）；
5. 熔断成功率窗口在高 QPS 下仍是 O(n) 且样本数有硬上限。
"""

import time

from CONFIG import CONFIG
from Service.BaseCrawler.resilience import ProxyCircuitBreaker
from Utils.代理.mdoel.RiskScope import risk_scope_of
from Utils.代理.数据库操作.ProxyHealth import (
    ProxyFailureKind,
    ProxyHealthStore,
    proxy_prefix_of,
)


# ---------------------------------------------------------------------------
# scope 归并口径
# ---------------------------------------------------------------------------


def test_web_scope_merges_by_channel_and_domain():
    url = "https://t.bilibili.com/x/topic/detail?topic_id=123&page=2"
    assert risk_scope_of(url, channel="web") == "web:topic"
    assert (
        risk_scope_of(
            "https://api.bilibili.com/x/polymer/web-dynamic/v1/feed/space?host_mid=1",
            channel="web",
        )
        == "web:dynamic"
    )


def test_grpc_scope_merges_by_method_path():
    assert (
        risk_scope_of(
            "https://grpc.biliapi.net/bilibili.app.dynamic.v2.Dynamic/DynDetail",
            channel="grpc",
        )
        == "grpc:dynamic"
    )
    # 直接传 func_name 同样命中
    assert (
        risk_scope_of(
            "/bilibili.app.dynamic.v2.Dynamic/DynDetail", channel="grpc"
        )
        == "grpc:dynamic"
    )


def test_unknown_url_falls_back_to_default_scope():
    assert risk_scope_of("https://api.bilibili.com/x/other/path", channel="web") == (
        "web:default"
    )
    assert risk_scope_of(None, channel="grpc") == "grpc:default"


def test_scope_keys_are_namespaced_and_global_bucket_is_legacy():
    store = ProxyHealthStore()
    prefix = "v6:2001:db8::/64"
    assert store._prefix_detail_key(prefix, "*") == f"proxy_health:v1:prefix_detail:{prefix}"
    assert (
        store._prefix_detail_key(prefix, "grpc:dynamic")
        == f"proxy_health:v1:prefix_detail:grpc:dynamic|{prefix}"
    )
    assert store._prefix_cooling_key("*") == "proxy_health:v1:prefix_cooling"
    assert (
        store._prefix_cooling_key("web:topic")
        == "proxy_health:v1:prefix_cooling:web:topic"
    )


# ---------------------------------------------------------------------------
# 跨 scope 不连坐（内存 Redis 桩）
# ---------------------------------------------------------------------------


class _FakePipeline:
    def __init__(self, store: "_FakeRedis") -> None:
        self.store = store
        self.ops: list = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def hgetall(self, key):
        self.ops.append(lambda: dict(self.store.hashes.get(key, {})))

    def hset(self, key, mapping):
        def _write():
            self.store.hashes.setdefault(key, {}).update(
                {k: str(v) for k, v in mapping.items()}
            )
            return 1

        self.ops.append(_write)

    def expire(self, *args):
        self.ops.append(lambda: True)

    def sadd(self, key, member):
        self.ops.append(lambda: self.store.sets.setdefault(key, set()).add(member))

    def srem(self, key, member):
        self.ops.append(lambda: self.store.sets.setdefault(key, set()).discard(member))

    def zadd(self, key, mapping):
        self.ops.append(
            lambda: self.store.zsets.setdefault(key, {}).update(mapping)
        )

    def zrem(self, key, member):
        self.ops.append(
            lambda: self.store.zsets.setdefault(key, {}).pop(member, None)
        )

    async def execute(self):
        return [op() for op in self.ops]


class _FakeRedis:
    """只实现健康状态机用到的 Redis 子集（pipeline + SET NX）。"""

    def __init__(self) -> None:
        self.hashes: dict[str, dict] = {}
        self.sets: dict[str, set] = {}
        self.zsets: dict[str, dict] = {}
        self.strings: dict[str, str] = {}

    def pipeline(self, transaction: bool = False):
        return _FakePipeline(self)

    async def hget(self, key: str, field: str):
        return (self.hashes.get(key) or {}).get(field)

    async def hgetall(self, key: str):
        return dict(self.hashes.get(key) or {})

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.strings:
            return None
        self.strings[key] = str(value)
        return True


async def test_cross_scope_risk_cooldown_does_not_leak(monkeypatch):
    store = ProxyHealthStore()
    fake = _FakeRedis()
    monkeypatch.setattr(store, "_get_client", lambda: fake, raising=False)
    proxy_key = "http://[2001:db8:1:2::7]:8080"
    prefix = proxy_prefix_of(proxy_key)

    # gRPC 接口命中 352 → 只有 grpc:dynamic 冷却
    await store.record_failure(
        proxy_key, ProxyFailureKind.RISK_CONTROL, scope="grpc:dynamic"
    )
    assert await store.is_prefix_cooling(prefix, "grpc:dynamic") is True
    assert await store.is_prefix_cooling(prefix, "web:topic") is False
    assert fake.zsets["proxy_health:v1:prefix_cooling:grpc:dynamic"]

    # web 接口命中 412 → 两个 scope 独立累计，互不覆盖
    await store.record_failure(
        proxy_key, ProxyFailureKind.RISK_CONTROL, scope="web:topic"
    )
    grpc_record = await store.get_prefix(prefix, "grpc:dynamic")
    web_record = await store.get_prefix(prefix, "web:topic")
    assert grpc_record.risk_level == 1
    assert web_record.risk_level == 1
    assert await store.is_prefix_cooling(prefix, "web:topic") is True
    # 历史全局桶没有被这两个 scope 写入
    assert await store.is_prefix_cooling(prefix) is False


async def test_network_failure_stays_in_global_prefix_bucket(monkeypatch):
    store = ProxyHealthStore()
    fake = _FakeRedis()
    monkeypatch.setattr(store, "_get_client", lambda: fake, raising=False)
    proxy_key = "http://45.74.31.7:8080"
    prefix = proxy_prefix_of(proxy_key)
    threshold = max(1, CONFIG.proxy_health.prefix_network_fail_threshold)

    for _ in range(threshold):
        await store.record_failure(
            proxy_key, ProxyFailureKind.NETWORK, scope="web:topic"
        )

    # 网络层冷却按「前缀」维度，与风控域无关：写全局桶
    assert await store.is_prefix_cooling(prefix) is True
    assert await store.is_prefix_cooling(prefix, "web:topic") is False


# ---------------------------------------------------------------------------
# 熔断窗口：O(n) + 硬上限
# ---------------------------------------------------------------------------


async def test_breaker_window_is_bounded_and_fast():
    from CONFIG import CrawlerBreakerConfig

    config = CrawlerBreakerConfig(
        window_seconds=300.0, window_max_samples=2000
    )
    breaker = ProxyCircuitBreaker(
        "test", config=config, clock=lambda: 1000.0
    )
    for _ in range(10000):
        await breaker.record_success()
    await breaker.record_failure("网络错误", pool_level=False)

    started = time.perf_counter()
    snapshot = await breaker.snapshot()
    elapsed = time.perf_counter() - started

    assert snapshot.samples <= 2000
    assert elapsed < 0.05, f"snapshot 耗时 {elapsed * 1000:.1f}ms"

def test_risk_scope_rules_are_configurable():
    """映射表来自 CONFIG，不在 Utils/代理/** 里硬编码业务域名。"""
    rules = CONFIG.proxy_health.risk_scope_rules
    assert any(scope == "topic" for _, scope in rules)
    assert all(isinstance(item, tuple) and len(item) == 2 for item in rules)
