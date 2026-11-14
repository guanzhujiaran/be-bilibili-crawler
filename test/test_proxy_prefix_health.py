# -*- coding: utf-8 -*-
"""前缀级（网段）健康判定的单测（纯计算 + 内存桩，不连 Redis / MySQL）。

覆盖：
1. 前缀归并口径：IPv4 /24、IPv6 /64（长度可配）、域名按主机、可解析性；
2. 412/352 冷却阶梯 2h → 6h → 24h，且不会无限增长；
3. 前缀级失败计划：风控立即冷却、网络层达到阈值才冷却；
4. 风控冷却期内成功**不解冻**（不能靠「恰好一条成功」绕过惩罚）；
5. 前缀闸门：冷却 / 在途配额 / 本任务排除，三者任一命中即拒；
6. 自建出口的通道 QPS 配额（固定 1 秒窗口，超配额快速失败）。
"""

import time

from CONFIG import CONFIG
from Utils.代理.数据库操作.ProxyHealth import (
    ProxyFailureKind,
    ProxyHealthState,
    ProxyHealthStore,
    ProxyInFlightMap,
    ProxyPrefixMap,
    ProxyPrefixRecord,
    ProxySelectionState,
    proxy_prefix_of,
)
from Utils.代理.mdoel.ProxyPrefix import (
    parse_proxy_endpoint,
    prefix_of_endpoint,
    proxy_prefix,
)

_store = ProxyHealthStore()


# ---------------------------------------------------------------------------
# 前缀归并
# ---------------------------------------------------------------------------


def test_ipv4_merges_by_24():
    assert proxy_prefix("http://45.74.31.7:8080") == "v4:45.74.31.0/24"
    assert proxy_prefix("http://45.74.31.99:8080") == "v4:45.74.31.0/24"
    # 不同 /24 必须分开
    assert proxy_prefix("http://45.74.32.7:8080") == "v4:45.74.32.0/24"


def test_ipv6_merges_by_64():
    a = proxy_prefix("socks5://[2001:db8:1:2::7]:1080")
    b = proxy_prefix("http://[2001:db8:1:2::ffff]:8080")
    assert a == "v6:2001:db8:1:2::/64"
    assert a == b
    # 相邻 /64 不能合并
    assert proxy_prefix("http://[2001:db8:1:3::7]:8080") != a


def test_prefix_length_is_configurable():
    assert proxy_prefix("http://45.74.31.7:8080", v4_length=16) == "v4:45.74.0.0/16"
    assert (
        proxy_prefix("http://[2001:db8:1:2::7]:1080", v6_length=48)
        == "v6:2001:db8:1::/48"
    )
    # 非法长度被夹回合法区间，不会抛异常
    assert proxy_prefix("http://45.74.31.7:8080", v4_length=99) == "v4:45.74.31.7/32"


def test_hostname_and_unparsable_are_stable_keys():
    assert proxy_prefix("http://proxy.example.com:80") == "host:proxy.example.com"
    assert proxy_prefix("http://proxy.example.com:8080") == "host:proxy.example.com"
    # 空串不参与前缀维度
    assert proxy_prefix("") == ""
    assert proxy_prefix_of("") == ""


def test_parse_endpoint_handles_auth_and_bad_port():
    endpoint = parse_proxy_endpoint("http://user:pass@1.2.3.4:8080")
    assert endpoint is not None and endpoint.host == "1.2.3.4"
    assert endpoint.port == 8080
    # 端口越界：无法解析，交给兜底口径（不抛异常）
    assert parse_proxy_endpoint("http://1.2.3.4:99999") is None
    assert prefix_of_endpoint(endpoint, 24, 64) == "v4:1.2.3.0/24"


# ---------------------------------------------------------------------------
# 冷却阶梯
# ---------------------------------------------------------------------------


def test_risk_cooldown_ladder_is_2h_6h_24h():
    cfg = CONFIG.proxy_health
    assert _store.prefix_risk_ladder() == (7200, 21600, 86400)
    assert _store.prefix_risk_cooldown(0) == cfg.prefix_risk_cooldown_first
    assert _store.prefix_risk_cooldown(1) == cfg.prefix_risk_cooldown_second
    assert _store.prefix_risk_cooldown(2) == cfg.prefix_risk_cooldown_max
    # 再往后只停在上限，不允许无限增长
    assert _store.prefix_risk_cooldown(99) == cfg.prefix_risk_cooldown_max


def test_risk_plan_escalates_ladder_per_episode():
    base = ProxyPrefixRecord()
    plan1 = _store.plan_prefix_failure(base, ProxyFailureKind.RISK_CONTROL)
    assert plan1.risk_control is True
    assert plan1.claim_episode is True
    assert plan1.state is ProxyHealthState.QUARANTINE
    assert plan1.cooldown == CONFIG.proxy_health.prefix_risk_cooldown_first
    assert plan1.next_risk_level == 1

    plan2 = _store.plan_prefix_failure(
        ProxyPrefixRecord(risk_level=1), ProxyFailureKind.RISK_CONTROL
    )
    assert plan2.cooldown == CONFIG.proxy_health.prefix_risk_cooldown_second
    assert plan2.next_risk_level == 2

    plan3 = _store.plan_prefix_failure(
        ProxyPrefixRecord(risk_level=2), ProxyFailureKind.RISK_CONTROL
    )
    assert plan3.cooldown == CONFIG.proxy_health.prefix_risk_cooldown_max
    # 档位封顶，不会突破阶梯末级
    assert plan3.next_risk_level == 2


def test_network_failure_needs_prefix_threshold():
    threshold = max(1, CONFIG.proxy_health.prefix_network_fail_threshold)
    # 阈值内：只累计，不冷却（单个坏代理掉线不该停掉整段）
    for fails in range(0, threshold - 1):
        plan = _store.plan_prefix_failure(
            ProxyPrefixRecord(consecutive_fail=fails), ProxyFailureKind.NETWORK
        )
        assert plan.cooldown == 0
    # 达到阈值：开始冷却（10~30 分钟区间）
    plan = _store.plan_prefix_failure(
        ProxyPrefixRecord(consecutive_fail=threshold - 1), ProxyFailureKind.NETWORK
    )
    assert plan.cooldown >= CONFIG.proxy_health.network_cooldown_min
    assert plan.cooldown <= CONFIG.proxy_health.network_cooldown_max
    assert plan.claim_episode is False


def test_prefix_success_does_not_unfreeze_risk_cooldown():
    """风控冷却期内的成功不解冻：那些请求是冷却生效前发出去的。"""
    now = int(time.time())
    record = ProxyPrefixRecord(
        state=ProxyHealthState.QUARANTINE,
        cooldown_until=now + 3600,
        last_kind=ProxyFailureKind.RISK_CONTROL,
        risk_level=1,
        consecutive_fail=4,
    )
    _store._prefix_success_transition(record, now)
    assert record.is_cooling(now) is True
    assert record.consecutive_fail == 0
    assert record.success_total == 1

    # 非风控冷却（网络层抖动）成功即恢复
    network = ProxyPrefixRecord(
        state=ProxyHealthState.WARM,
        cooldown_until=now + 600,
        last_kind=ProxyFailureKind.NETWORK,
        consecutive_fail=3,
    )
    _store._prefix_success_transition(network, now)
    assert network.is_cooling(now) is False
    assert network.state is ProxyHealthState.HOT


def test_prefix_success_decays_risk_level():
    now = int(time.time())
    record = ProxyPrefixRecord(
        state=ProxyHealthState.HOT, risk_level=2, cooldown_until=0
    )
    _store._prefix_success_transition(record, now)
    assert record.risk_level == 1
    _store._prefix_success_transition(record, now)
    assert record.risk_level == 0
    _store._prefix_success_transition(record, now)
    assert record.risk_level == 0  # 不会降到负数


# ---------------------------------------------------------------------------
# 前缀闸门
# ---------------------------------------------------------------------------


def _gate(**kwargs) -> ProxySelectionState:
    return ProxySelectionState(**kwargs)


def test_gate_rejects_cooling_prefix():
    now = int(time.time())
    prefix = "v4:45.74.31.0/24"
    gate = _gate(
        prefixes=ProxyPrefixMap(
            records={
                prefix: ProxyPrefixRecord(
                    state=ProxyHealthState.QUARANTINE, cooldown_until=now + 600
                )
            }
        )
    )
    assert gate.prefix_allows(prefix, now) is False
    # 未到期后就放行
    assert gate.prefix_allows(prefix, now + 601) is True


def test_gate_rejects_excluded_prefix_and_full_inflight():
    limit = max(1, CONFIG.proxy_health.prefix_inflight_limit)
    prefix = "v4:45.74.31.0/24"
    assert (
        _gate(excluded_prefixes=frozenset({prefix})).prefix_allows(prefix) is False
    )
    assert (
        _gate(prefix_in_flight=ProxyInFlightMap(counts={prefix: limit})).prefix_allows(
            prefix
        )
        is False
    )
    assert (
        _gate(prefix_in_flight=ProxyInFlightMap(counts={prefix: limit - 1})).prefix_allows(
            prefix
        )
        is True
    )
    # 无前缀（解析失败）不参与判定，不能因此把请求全拒
    assert _gate().prefix_allows("") is True


# ---------------------------------------------------------------------------
# 自建出口的通道 QPS 配额
# ---------------------------------------------------------------------------


class _FakeQpsRedis:
    """只实现 INCR / EXPIRE 的最小 Redis 桩。"""

    def __init__(self) -> None:
        self.counters: dict[str, int] = {}
        self.expires: dict[str, int] = {}

    async def incr(self, key: str) -> int:
        self.counters[key] = self.counters.get(key, 0) + 1
        return self.counters[key]

    async def expire(self, key: str, ttl: int) -> bool:
        self.expires[key] = ttl
        return True


async def test_qps_quota_limits_local_exit(monkeypatch):
    fake = _FakeQpsRedis()
    monkeypatch.setattr(_store, "_get_client", lambda: fake, raising=False)
    # 固定时间窗口：避免用例跨秒导致配额被重置
    monkeypatch.setattr(time, "time", lambda: 1_000_000.0)

    assert await _store.acquire_qps("local-ipv6", per_second=2, max_wait=0) is True
    assert await _store.acquire_qps("local-ipv6", per_second=2, max_wait=0) is True
    # 第三次超出配额：max_wait=0 → 立即快速失败（而不是把请求挂住）
    assert await _store.acquire_qps("local-ipv6", per_second=2, max_wait=0) is False
    # 不同通道互不影响
    assert await _store.acquire_qps("other-channel", per_second=1, max_wait=0) is True


async def test_qps_zero_means_unlimited(monkeypatch):
    fake = _FakeQpsRedis()
    monkeypatch.setattr(_store, "_get_client", lambda: fake, raising=False)
    for _ in range(20):
        assert await _store.acquire_qps("local-ipv6", per_second=0, max_wait=0) is True
    assert fake.counters == {}
