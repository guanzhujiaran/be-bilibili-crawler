# -*- coding: utf-8 -*-
"""fast→slow 降档与「池空异常类型」回归（不连 MySQL / Redis）。

覆盖两个线上问题：

1. fast 预算（2 轮 × 3 次 / 累计 10s）耗尽后直接抛 ``ProxyRetryBudgetExhausted``，
   slow 车道的大预算完全没被用上；
2. ``pool_only`` 分支构造「池内无候选」异常时漏传 ``local_tried``，抛的是
   ``TypeError``，导致池级失败无法被 ``is_pool_level_error`` 识别。
"""

import pytest

from CONFIG import CONFIG, ProxyRetryTierConfig
from Service.BaseCrawler.resilience import is_pool_level_error
from Service.GrpcModule.Grpc.Bapi.BapiUtils import request_wrapper
from Service.GrpcModule.Models.CustomRequestErrorModel import (
    ProxyRetryBudgetExhausted,
    RequestProxyNetworkError,
    RequestProxyUnavailableError,
)
from Utils.代理.数据库操作.ProxyHealth import ProxyHealthCounts, proxy_health_store
from Utils.代理.mdoel import request_context
from Utils.代理.mdoel.ProxyTier import ProxyTier
from Utils.代理.mdoel.RequestConf import RequestConf, RequestRetryPolicy
from Utils.代理.redisProxyRequest.RedisRequestProxy import RequestWithProxy


def _fast_conf() -> RequestConf:
    return RequestConf(
        tier=ProxyTier.FAST,
        retry_policy=RequestRetryPolicy(
            tier=ProxyTier.FAST,
            max_rounds=2,
            attempts_per_round=3,
            backoff_base=0.0,
            backoff_max=0.0,
            backoff_jitter=0.0,
            log_interval=0,
        ),
    )


async def test_fast_budget_exhausted_downgrades_to_slow(monkeypatch) -> None:
    """fast 6 次后切 slow；slow 也耗尽才抛 -5，且文案标明 fast→slow。"""
    seen_tiers: list[ProxyTier] = []

    async def _always_fail(request_conf: RequestConf):
        seen_tiers.append(request_conf.tier)
        raise RequestProxyNetworkError("blackhole", -1, proxy_key="http://127.0.0.1:1")

    wrapped = request_wrapper(_always_fail)

    async def _no_sleep(_delay: float) -> None:
        return None

    # 把 slow 预算缩小到 2 次：降档必须换成 CONFIG 的 slow 策略，
    # 否则测试会真的按 180s / 60s 退避跑很久。
    monkeypatch.setattr(
        CONFIG.proxy_retry,
        "slow",
        ProxyRetryTierConfig(
            max_rounds=2,
            attempts_per_round=1,
            backoff_base=0.0,
            backoff_max=0.0,
            backoff_jitter=0.0,
            max_total_seconds=60.0,
            log_interval=0,
        ),
    )
    conf = _fast_conf()
    token = request_context.bind_retry_yield(_no_sleep)
    try:
        with pytest.raises(ProxyRetryBudgetExhausted) as exc_info:
            await wrapped(conf)
    finally:
        request_context.reset_retry_yield(token)

    # fast 2 轮 × 3 次 = 6 次；降档后 slow 再走 2 次仍未成功才抛
    assert seen_tiers == [ProxyTier.FAST] * 6 + [ProxyTier.SLOW] * 2
    # 本次调用的 RequestConf 必须同步改成 slow（resolve_tier 里它优先级最高）
    assert conf.tier is ProxyTier.SLOW
    assert "fast→slow" in str(exc_info.value)
    assert exc_info.value.code == -5


async def test_pool_only_empty_pool_raises_unavailable(monkeypatch) -> None:
    """pool_only 且池内无候选：抛 RequestProxyUnavailableError，而不是 TypeError。"""
    rwp = RequestWithProxy()

    async def _empty_pool(_tier: ProxyTier):
        return None

    async def _counts(*_args, **_kwargs) -> ProxyHealthCounts:
        # 池子整体有货、只是本次没有候选 → reason 归为 pool_empty（而非 both）
        return ProxyHealthCounts(prefix_hot=10)

    monkeypatch.setattr(rwp, "_pool_proxy", _empty_pool)
    # 让「自建出口」判定直接走未配置分支，不触碰 Redis
    monkeypatch.setattr(CONFIG, "my_ipv6_addr", "")
    monkeypatch.setattr(proxy_health_store, "get_counts", _counts)

    with pytest.raises(RequestProxyUnavailableError) as exc_info:
        await rwp._select_proxy_impl(
            RequestConf(pool_only=True, tier=ProxyTier.FAST), ProxyTier.FAST
        )

    err = exc_info.value
    assert is_pool_level_error(err) is True
    assert err.reason == RequestProxyUnavailableError.REASON_POOL_EMPTY
    assert "pool_only" in str(err)
