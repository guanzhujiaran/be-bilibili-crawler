# -*- coding: utf-8 -*-
"""自建出口业务通道「不限速」与会话池并发的配置单测（内存桩，不连 Redis / MySQL）。

覆盖：
1. ``Settings.web_session_pool_max_clients`` 随 ``CONCURRENCY__LOTTERY_API`` 变化，
   且始终落在 [32, 256]；显式 ``WEB_SESSION__POOL_MAX_CLIENTS`` 优先；
2. ``local_ipv6_qps=0`` 时 ``_acquire_local_exit`` 不做配额计数、永不因配额返回
   ``proxy=None``（回滚到 >0 时行为由 ``test_local_exit_only_channel.py`` 覆盖）。
"""

from CONFIG import CONFIG, Settings
from Utils.代理.mdoel.ProxyTier import ProxyTier
from Utils.代理.mdoel.RequestConf import RequestConf
from Utils.代理.redisProxyRequest import RedisRequestProxy as proxy_mod
from Utils.代理.redisProxyRequest.RedisRequestProxy import RequestWithProxy

#: 测试用的自建出口地址（不对应真实端口：单测不会真的发请求）
_LOCAL_EXIT = "http://127.0.0.1:3128"


class _AlwaysDenyQuotaStore:
    """前缀不冷却，但配额永远拒绝：用来验证 qps=0 时根本不走配额分支。"""

    def __init__(self) -> None:
        self.qps_calls: list[tuple[str, float]] = []

    async def is_prefix_cooling(self, prefix: str) -> bool:
        return False

    async def acquire_qps(
        self, channel: str, per_second: float, max_wait: float = 1.0
    ) -> bool:
        self.qps_calls.append((channel, per_second))
        return False


def test_web_session_pool_max_clients_scales_with_lottery_api(monkeypatch):
    monkeypatch.setenv("CONCURRENCY__LOTTERY_API", "5")
    small = Settings().web_session_pool_max_clients
    monkeypatch.setenv("CONCURRENCY__LOTTERY_API", "120")
    large = Settings().web_session_pool_max_clients

    assert large > small
    assert 32 <= small <= 256
    assert 32 <= large <= 256


def test_web_session_pool_max_clients_explicit_value_wins(monkeypatch):
    monkeypatch.setenv("WEB_SESSION__POOL_MAX_CLIENTS", "7")
    assert Settings().web_session_pool_max_clients == 7


async def test_local_ipv6_qps_zero_never_blocks_by_quota(monkeypatch):
    rwp = RequestWithProxy()
    monkeypatch.setattr(CONFIG, "my_ipv6_addr", _LOCAL_EXIT)
    monkeypatch.setattr(CONFIG.proxy_health, "local_ipv6_qps", 0.0)
    store = _AlwaysDenyQuotaStore()
    monkeypatch.setattr(proxy_mod, "proxy_health_store", store)

    for _ in range(20):
        gate = await rwp._acquire_local_exit(ProxyTier.FAST, RequestConf())
        assert gate.proxy is not None
        assert gate.qps == 0.0

    # 不限速时连 QPS 计数都不做：既不阻塞，也不产生 blocked_quota / 配额日志
    assert store.qps_calls == []
