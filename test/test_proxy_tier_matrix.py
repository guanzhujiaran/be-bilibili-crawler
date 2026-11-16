# -*- coding: utf-8 -*-
"""代理档位矩阵回归：谁快、谁慢、谁优先 IPv6。

约定（与 Service/BaseCrawler/config.py 的注释一致）：

- FAST（高可用池，快速失败）：预约 / 话题 / 抽奖 API / 直播 / 他人动态；
- SLOW（可掺 cold 探测）：动态详情 / 取关对象 / 抽奖库刷新；
- 山姆不走 B 站代理链路（直连），档位保持默认，不参与比较。

另锁定一个易踩的坑：``is_use_available_proxy=True`` 会把档位解析成 SLOW，
所以 GetOthersLotDyn 内部的 RequestConf 必须显式带 ``tier=FAST``。
"""

import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from Service.BaseCrawler.config import (
    BiliLiveCrawlerConfig,
    CRAWLER_CONFIG_REGISTRY,
    DynDetailScrapyConfig,
    GetOthersLotDynRobotConfig,
    GetRmFollowingListV2Config,
    LotteryApiRobotConfig,
    RefreshBiliLotDatabaseConfig,
    ReserveScrapyRobotConfig,
    SamsClubCrawlerConfig,
    SamsClubSPUDetailCrawlerConfig,
    TopicRobotConfig,
)
from CONFIG import CONFIG
from Utils.代理.数据库操作.SqlAlcheyObj.ProxyModel import ProxyTab
from Utils.代理.mdoel.ProxyTier import ProxyTier
from Utils.代理.mdoel.RequestConf import RequestConf
from Utils.代理.mdoel.request_context import resolve_tier
from Utils.代理.redisProxyRequest.RedisRequestProxy import (
    LocalExitGate,
    RequestWithProxy,
)

#: 期望矩阵：key=爬虫配置类，value=期望档位
EXPECTED_TIERS = {
    DynDetailScrapyConfig: ProxyTier.SLOW,
    GetRmFollowingListV2Config: ProxyTier.SLOW,
    ReserveScrapyRobotConfig: ProxyTier.FAST,
    TopicRobotConfig: ProxyTier.FAST,
    LotteryApiRobotConfig: ProxyTier.FAST,
    RefreshBiliLotDatabaseConfig: ProxyTier.SLOW,
    SamsClubCrawlerConfig: ProxyTier.SLOW,
    SamsClubSPUDetailCrawlerConfig: ProxyTier.SLOW,
    BiliLiveCrawlerConfig: ProxyTier.FAST,
    GetOthersLotDynRobotConfig: ProxyTier.FAST,
}


def test_registry_covers_expected_tiers() -> None:
    assert set(CRAWLER_CONFIG_REGISTRY) == set(EXPECTED_TIERS)


def test_each_crawler_uses_expected_proxy_tier() -> None:
    for config_cls, expected in EXPECTED_TIERS.items():
        actual = CRAWLER_CONFIG_REGISTRY[config_cls].proxy_tier
        assert actual is expected, f"{config_cls.__name__} 期望 {expected}，实际 {actual}"


def test_explicit_tier_overrides_legacy_available_proxy_flag() -> None:
    """GetOthersLotDyn 的覆盖点：tier=FAST 必须压过 is_use_available_proxy=True。"""
    conf = RequestConf(is_use_available_proxy=True, tier=ProxyTier.FAST)
    assert resolve_tier(conf) is ProxyTier.FAST


def test_legacy_flag_alone_still_means_slow() -> None:
    """没有显式 tier 时，历史参数仍是 slow（保持旧调用方行为不变）。"""
    assert resolve_tier(RequestConf(is_use_available_proxy=True)) is ProxyTier.SLOW


def _pool_proxy_tab() -> ProxyTab:
    return ProxyTab(
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


async def test_pool_only_never_touches_local_exit(monkeypatch) -> None:
    """他人动态：只走公共池 hot/warm，绝不去尝试自建出口。"""
    rwp = RequestWithProxy()
    pool_proxy = _pool_proxy_tab()
    pool_calls: list[ProxyTier] = []

    async def _local(*args, **kwargs):
        raise AssertionError("pool_only 请求不允许走自建出口")

    async def _pool(tier):
        pool_calls.append(tier)
        return pool_proxy

    monkeypatch.setattr(rwp, "_acquire_local_exit", _local)
    monkeypatch.setattr(rwp, "_pool_proxy", _pool)

    proxy, is_local = await rwp._select_proxy_impl(
        RequestConf(pool_only=True, tier=ProxyTier.FAST), ProxyTier.FAST
    )
    assert is_local is False
    assert proxy is pool_proxy
    assert pool_calls == [ProxyTier.FAST]


async def test_custom_proxy_falls_back_to_fast_pool_when_local_unavailable(
    monkeypatch,
) -> None:
    """预约/话题/抽奖 API/直播：自建出口不可用时才用 hot/warm 兜底。"""
    rwp = RequestWithProxy()
    pool_proxy = _pool_proxy_tab()
    pool_calls: list[ProxyTier] = []

    async def _local(tier, request_conf=None):
        return LocalExitGate(reason="自建出口本次失败")

    async def _pool(tier):
        pool_calls.append(tier)
        return pool_proxy

    monkeypatch.setattr(rwp, "_acquire_local_exit", _local)
    monkeypatch.setattr(rwp, "_pool_proxy", _pool)
    monkeypatch.setattr(CONFIG.proxy_health, "local_ipv6_cooling_pool_fallback", True)

    proxy, is_local = await rwp._select_proxy_impl(
        RequestConf(is_use_custom_proxy=True), ProxyTier.FAST
    )
    assert is_local is False
    assert proxy is pool_proxy
    assert pool_calls == [ProxyTier.FAST]


async def test_live_area_api_accepts_request_conf(monkeypatch) -> None:
    """直播分区接口必须接受 RequestConf（此前调用方传错关键字，实际从未生效）。"""
    from Service.GrpcModule.Grpc.Bapi import BiliApi

    captured: dict = {}

    async def _fake(**kwargs):
        captured.update(kwargs)
        return {}

    monkeypatch.setattr(
        BiliApi.request_with_proxy_internal, "request_with_proxy", _fake
    )
    conf = RequestConf(is_use_custom_proxy=True)
    await BiliApi.xlive_web_interface_v1_index_getWebAreaList(request_conf=conf)
    assert captured["request_conf"] is conf
    assert captured["request_conf"].is_use_custom_proxy is True
