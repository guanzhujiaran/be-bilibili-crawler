# -*- coding: utf-8 -*-
"""专线独占（``RequestConf.local_exit_only``）通道的单测（内存桩，不连 Redis / MySQL）。

覆盖：
1. 通道语义：专线独占走独立 QPS 通道，业务请求仍走原通道，两者互不抢占；
2. ``ignore_risk_cooldown`` 跳过「自建出口前缀风控冷却」闸门，全局开关可收回该豁免；
3. 专线独占取不到自建出口时**不降级到代理池**，抛 ``reason=local_exit_only``；
4. 专线独占请求不写池健康 / 前缀冷却（失败记账被跳过），但本任务内去重仍保留。
"""

import curl_cffi
import pytest

from CONFIG import CONFIG
from Service.GrpcModule.Models.CustomRequestErrorModel import (
    RequestProxyNetworkError,
    RequestProxyUnavailableError,
)
from Utils.代理.数据库操作.SqlAlcheyObj.ProxyModel import ProxyTab
from Utils.代理.mdoel.ProxyTier import ProxyTier
from Utils.代理.mdoel.RequestConf import RequestConf
from Utils.代理.redisProxyRequest import RedisRequestProxy as proxy_mod
from Utils.代理.redisProxyRequest.RedisRequestProxy import RequestWithProxy

#: 测试用的自建出口地址（不对应真实端口：单测不会真的发请求）
_LOCAL_EXIT = "http://127.0.0.1:3128"
_LOCAL_EXIT_KEY = _LOCAL_EXIT


class _FakeHealthStore:
    """只实现专线闸门用到的两个方法（前缀冷却 / 通道 QPS）。"""

    def __init__(self, *, cooling: bool = False, qps_ok: bool = True) -> None:
        self.cooling = cooling
        self.qps_ok = qps_ok
        self.qps_calls: list[tuple[str, float]] = []

    async def is_prefix_cooling(self, prefix: str, scope: str | None = None) -> bool:
        return self.cooling

    async def acquire_qps(
        self, channel: str, per_second: float, max_wait: float = 1.0
    ) -> bool:
        self.qps_calls.append((channel, per_second))
        return self.qps_ok


class _FakeQpsRedis:
    """只实现 INCR / EXPIRE 的最小 Redis 桩（用于验证通道独立计数）。"""

    def __init__(self) -> None:
        self.counters: dict[str, int] = {}

    async def incr(self, key: str) -> int:
        self.counters[key] = self.counters.get(key, 0) + 1
        return self.counters[key]

    async def expire(self, key: str, ttl: int) -> bool:
        return True


def _local_proxy_tab() -> ProxyTab:
    """自建出口的代理行（与 ``RequestWithProxy._local_ipv6_proxy`` 同形状）。"""
    return ProxyTab(
        **{
            "proxy_id": 1,
            "proxy": CONFIG.custom_proxy,
            "status": 0,
            "update_ts": 0,
            "score": 0,
            "add_ts": 0,
            "success_times": 0,
            "zhihu_status": 0,
            "computed_proxy_str": CONFIG.my_ipv6_addr,
        }
    )


def _async_recorder(calls: list[str], name: str):
    async def _record(*args, **kwargs):
        calls.append(name)

    return _record


class _FailingHttpx:
    """模拟「自建出口端口指向黑洞」：请求直接抛连接错误。"""

    def __init__(self, exc: BaseException) -> None:
        self.exc = exc

    async def request(self, **kwargs):
        raise self.exc


# ---------------------------------------------------------------------------
# 通道 / 配额语义
# ---------------------------------------------------------------------------


def test_local_exit_only_uses_its_own_quota_channel():
    cfg = CONFIG.proxy_health
    assert RequestWithProxy._local_exit_quota(
        RequestConf(local_exit_only=True)
    ) == (cfg.local_exit_only_channel, cfg.local_exit_only_qps)
    # 业务请求仍走原通道，语义不变
    assert RequestWithProxy._local_exit_quota(RequestConf()) == (
        cfg.local_ipv6_channel,
        cfg.local_ipv6_qps,
    )
    assert cfg.local_exit_only_channel != cfg.local_ipv6_channel


async def test_qps_counting_is_independent_between_channels(monkeypatch):
    """两条通道各自计数：专线独占跑满也不影响业务通道取额度。"""
    from Utils.代理.数据库操作.ProxyHealth import ProxyHealthStore

    store = ProxyHealthStore()
    fake = _FakeQpsRedis()
    monkeypatch.setattr(store, "_get_client", lambda: fake, raising=False)
    cfg = CONFIG.proxy_health
    # 业务通道：配额 1/s
    assert (
        await store.acquire_qps(cfg.local_ipv6_channel, 1, max_wait=0) is True
    )
    assert (
        await store.acquire_qps(cfg.local_ipv6_channel, 1, max_wait=0) is False
    )
    # 专线独占通道：同样配额 1/s，但计数从 0 开始（不共享上面的 1 次）
    assert (
        await store.acquire_qps(cfg.local_exit_only_channel, 1, max_wait=0) is True
    )
    assert (
        await store.acquire_qps(cfg.local_exit_only_channel, 1, max_wait=0) is False
    )


# ---------------------------------------------------------------------------
# 风控冷却闸门
# ---------------------------------------------------------------------------


async def test_ignore_risk_cooldown_skips_prefix_cooling(monkeypatch):
    rwp = RequestWithProxy()
    monkeypatch.setattr(CONFIG, "my_ipv6_addr", _LOCAL_EXIT)
    store = _FakeHealthStore(cooling=True, qps_ok=True)
    monkeypatch.setattr(proxy_mod, "proxy_health_store", store)

    # 业务请求：冷却中 → 闸门拒绝
    gate = await rwp._acquire_local_exit(ProxyTier.FAST, RequestConf())
    assert gate.proxy is None
    assert "风控冷却" in gate.reason
    assert store.qps_calls == []

    # 激活类请求（ignore_risk_cooldown）：跳过冷却判定，且走独立通道配额
    gate = await rwp._acquire_local_exit(
        ProxyTier.FAST, RequestConf(local_exit_only=True, ignore_risk_cooldown=True)
    )
    assert gate.proxy is not None
    cfg = CONFIG.proxy_health
    assert store.qps_calls == [(cfg.local_exit_only_channel, cfg.local_exit_only_qps)]


async def test_ignore_risk_cooldown_can_be_disabled_globally(monkeypatch):
    rwp = RequestWithProxy()
    monkeypatch.setattr(CONFIG, "my_ipv6_addr", _LOCAL_EXIT)
    store = _FakeHealthStore(cooling=True, qps_ok=True)
    monkeypatch.setattr(proxy_mod, "proxy_health_store", store)
    monkeypatch.setattr(CONFIG.proxy_health, "local_exit_ignore_risk_cooldown", False)

    gate = await rwp._acquire_local_exit(
        ProxyTier.FAST, RequestConf(local_exit_only=True, ignore_risk_cooldown=True)
    )
    assert gate.proxy is None
    assert "风控冷却" in gate.reason


# ---------------------------------------------------------------------------
# 不降级到代理池
# ---------------------------------------------------------------------------


async def test_local_exit_only_never_falls_back_to_pool(monkeypatch):
    rwp = RequestWithProxy()
    monkeypatch.setattr(CONFIG, "my_ipv6_addr", _LOCAL_EXIT)
    monkeypatch.setattr(
        proxy_mod, "proxy_health_store", _FakeHealthStore(cooling=True)
    )

    async def _no_pool(tier):
        raise AssertionError("local_exit_only 请求不允许降级到公共代理池")

    monkeypatch.setattr(rwp, "_pool_proxy", _no_pool)

    # 冷却且未声明豁免 → 闸门拒绝 → 直接抛错（不走池）
    with pytest.raises(RequestProxyUnavailableError) as exc:
        await rwp._select_proxy(
            RequestConf(local_exit_only=True), ProxyTier.FAST
        )
    err = exc.value
    assert err.reason == RequestProxyUnavailableError.REASON_LOCAL_EXIT_ONLY
    assert "专线独占请求且自建出口不可用" in str(err)
    assert "不降级到代理池" in str(err)
    assert _LOCAL_EXIT in str(err)


async def test_local_exit_only_fails_fast_when_quota_exhausted(monkeypatch):
    rwp = RequestWithProxy()
    monkeypatch.setattr(CONFIG, "my_ipv6_addr", _LOCAL_EXIT)
    monkeypatch.setattr(
        proxy_mod, "proxy_health_store", _FakeHealthStore(qps_ok=False)
    )

    async def _no_pool(tier):
        raise AssertionError("local_exit_only 请求不允许降级到公共代理池")

    monkeypatch.setattr(rwp, "_pool_proxy", _no_pool)

    with pytest.raises(RequestProxyUnavailableError) as exc:
        await rwp._select_proxy(
            RequestConf(local_exit_only=True, ignore_risk_cooldown=True),
            ProxyTier.FAST,
        )
    assert exc.value.reason == RequestProxyUnavailableError.REASON_LOCAL_EXIT_ONLY
    assert "QPS" in str(exc.value)


async def test_business_request_still_falls_back_to_pool(monkeypatch):
    """业务请求的降级行为保持不变（专线冷却 → 公共池）。"""
    rwp = RequestWithProxy()
    monkeypatch.setattr(CONFIG, "my_ipv6_addr", _LOCAL_EXIT)
    monkeypatch.setattr(
        proxy_mod, "proxy_health_store", _FakeHealthStore(cooling=True)
    )
    pool_proxy = ProxyTab(
        **{
            "proxy_id": 2,
            "proxy": {"http": "http://1.2.3.4:8080", "https": "http://1.2.3.4:8080"},
            "status": 0,
            "update_ts": 0,
            "score": 0,
            "add_ts": 0,
            "success_times": 0,
            "zhihu_status": 0,
            "computed_proxy_str": "http://1.2.3.4:8080",
        }
    )
    pool_calls: list[ProxyTier] = []

    async def _pool(tier):
        pool_calls.append(tier)
        return pool_proxy

    monkeypatch.setattr(rwp, "_pool_proxy", _pool)
    monkeypatch.setattr(CONFIG.proxy_health, "local_ipv6_cooling_pool_fallback", True)

    proxy, is_local = await rwp._select_proxy(
        RequestConf(is_use_custom_proxy=True), ProxyTier.FAST
    )
    assert is_local is False
    assert pool_calls == [ProxyTier.FAST]
    assert proxy is pool_proxy


# ---------------------------------------------------------------------------
# 失败记账不污染池健康
# ---------------------------------------------------------------------------


async def test_local_exit_only_failure_skips_pool_health_accounting(monkeypatch):
    rwp = RequestWithProxy()
    monkeypatch.setattr(CONFIG, "my_ipv6_addr", _LOCAL_EXIT)
    calls: list[str] = []
    for name in (
        "handle_proxy_412",
        "handle_proxy_352",
        "handle_proxy_request_fail",
        "handle_proxy_succ",
    ):
        monkeypatch.setattr(proxy_mod, name, _async_recorder(calls, name))
    monkeypatch.setattr(
        proxy_mod,
        "my_async_httpx",
        _FailingHttpx(curl_cffi.requests.exceptions.ConnectionError("blackhole")),
    )
    proxy = _local_proxy_tab()

    with pytest.raises(RequestProxyNetworkError) as exc:
        await rwp._dispatch_request(
            proxy=proxy,
            proxy_key=_LOCAL_EXIT_KEY,
            request_conf=RequestConf(local_exit_only=True),
            method="GET",
            url="https://www.bilibili.com",
        )
    assert calls == []
    # 可观测性：异常里必须能 grep 到 reason=local_exit_only 与出口地址
    assert "reason=local_exit_only" in str(exc.value)
    assert _LOCAL_EXIT in str(exc.value)


async def test_business_failure_still_books_pool_health(monkeypatch):
    rwp = RequestWithProxy()
    monkeypatch.setattr(CONFIG, "my_ipv6_addr", _LOCAL_EXIT)
    calls: list[str] = []
    for name in (
        "handle_proxy_412",
        "handle_proxy_352",
        "handle_proxy_request_fail",
        "handle_proxy_succ",
    ):
        monkeypatch.setattr(proxy_mod, name, _async_recorder(calls, name))
    monkeypatch.setattr(
        proxy_mod,
        "my_async_httpx",
        _FailingHttpx(curl_cffi.requests.exceptions.ConnectionError("blackhole")),
    )
    proxy = _local_proxy_tab()

    with pytest.raises(RequestProxyNetworkError):
        await rwp._dispatch_request(
            proxy=proxy,
            proxy_key=_LOCAL_EXIT_KEY,
            request_conf=RequestConf(),
            method="GET",
            url="https://www.bilibili.com",
        )
    assert calls == ["handle_proxy_request_fail"]
