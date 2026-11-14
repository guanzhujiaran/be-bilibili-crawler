# -*- coding: utf-8 -*-
"""代理健康状态机 / 重试预算的单测（纯计算，不连 Redis / MySQL）。

覆盖：
1. 连续失败 → warm / suspect / quarantine / dead 降档，成功清零；
2. 网络层冷却夹在 10~30 分钟，风控层夹在 2~6 小时；
3. 冷启动播种口径（只有「历史成功过」才给 warm + 达标 EWMA）；
4. fast / slow 档准入条件；
5. 重试预算：fast 2 轮 × 3 次快速失败、slow 无限 + 指数退避封顶 60s；
6. 失败任务的延迟重入队：指数增长且不超过上限。
"""

import time

from CONFIG import CONFIG
from Service.BaseCrawler.CrawlerType import UnlimitedCrawler
from Utils.代理.数据库操作.ProxyHealth import (
    ProxyFailureKind,
    ProxyHealthRecord,
    ProxyHealthState,
    ProxyHealthStore,
    seed_health_record,
)
from Utils.代理.mdoel.RequestConf import RequestRetryPolicy
from Utils.代理.mdoel.ProxyTier import ProxyTier

_store = ProxyHealthStore()


# ---------------------------------------------------------------------------
# 状态机
# ---------------------------------------------------------------------------


def test_failure_ladder_degrades_by_consecutive_fails():
    cfg = CONFIG.proxy_health
    assert _store._state_for_fails(1) is ProxyHealthState.HOT
    assert _store._state_for_fails(cfg.warm_after_fails) is ProxyHealthState.WARM
    assert _store._state_for_fails(cfg.suspect_after_fails) is ProxyHealthState.SUSPECT
    assert (
        _store._state_for_fails(cfg.quarantine_after_fails)
        is ProxyHealthState.QUARANTINE
    )
    assert _store._state_for_fails(cfg.dead_after_fails) is ProxyHealthState.DEAD


def test_network_cooldown_is_10_to_30_minutes():
    cfg = CONFIG.proxy_health
    low = cfg.network_cooldown_min
    high = cfg.network_cooldown_max
    # 抖动后也必须落在 [10min, 30min]（dead 走单独的长冷却，单独断言）
    for fails, state in (
        (1, ProxyHealthState.HOT),
        (3, ProxyHealthState.WARM),
        (6, ProxyHealthState.SUSPECT),
        (12, ProxyHealthState.QUARANTINE),
    ):
        for _ in range(20):
            cooldown = _store._network_cooldown(fails, state)
            assert low <= cooldown <= high, (fails, state, cooldown)
    # 降档后冷却指数增长，但不越过上限
    assert _store._network_cooldown(3, ProxyHealthState.WARM) >= low
    assert _store._network_cooldown(12, ProxyHealthState.QUARANTINE) == high or (
        _store._network_cooldown(12, ProxyHealthState.QUARANTINE) <= high
    )


def test_dead_cooldown_uses_dead_setting():
    cfg = CONFIG.proxy_health
    cooldown = _store._network_cooldown(99, ProxyHealthState.DEAD)
    assert cooldown >= cfg.network_cooldown_min
    assert cooldown <= max(cfg.dead_cooldown, cfg.network_cooldown_max) * 1.0


def test_risk_cooldown_is_2_to_6_hours():
    cfg = CONFIG.proxy_health
    for _ in range(50):
        cooldown = _store._risk_cooldown()
        assert cfg.risk_cooldown_min <= cooldown <= cfg.risk_cooldown_max


def test_seed_record_only_for_proven_proxies():
    cfg = CONFIG.proxy_health
    cold = seed_health_record(0, 100)
    assert cold.state is ProxyHealthState.COLD
    assert cold.ewma_success == 0.0

    warm = seed_health_record(cfg.seed_success_times, cfg.seed_min_score)
    assert warm.state is ProxyHealthState.WARM
    assert warm.ewma_success == cfg.fast_min_ewma


def test_fast_and_slow_eligibility():
    now = int(time.time())
    hot = ProxyHealthRecord(state=ProxyHealthState.HOT, ewma_success=0.95)
    low = ProxyHealthRecord(state=ProxyHealthState.HOT, ewma_success=0.5)
    cooling = ProxyHealthRecord(
        state=ProxyHealthState.HOT, ewma_success=1.0, cooldown_until=now + 600
    )
    assert _store.is_fast_eligible(hot, 0, now) is True
    assert _store.is_fast_eligible(hot, CONFIG.proxy_health.fast_max_in_flight, now) is False
    assert _store.is_fast_eligible(low, 0, now) is False
    assert _store.is_fast_eligible(cooling, 0, now) is False
    # slow 档只看是否冷却（dead 由取代理逻辑单独排除）
    assert _store.is_slow_eligible(cooling, now) is False
    assert _store.is_slow_eligible(hot, now) is True


# ---------------------------------------------------------------------------
# 重试预算
# ---------------------------------------------------------------------------


def test_fast_policy_is_two_rounds_of_three():
    policy = CONFIG.proxy_retry.policy_for(ProxyTier.FAST)
    assert policy.max_attempts == 6
    assert policy.is_exhausted(5) is False
    assert policy.is_exhausted(6) is True
    # 退避封顶在 fast 的小区间内
    assert policy.delay_for(1) <= CONFIG.proxy_retry.fast.backoff_max * 1.2
    assert policy.delay_for(6) <= CONFIG.proxy_retry.fast.backoff_max * 1.2


def test_slow_policy_is_unbounded_with_backoff_cap():
    policy = CONFIG.proxy_retry.policy_for(ProxyTier.SLOW)
    assert policy.max_attempts is None
    assert policy.is_exhausted(10**6, waited_seconds=0.0) is False
    # 指数退避封顶 60s（配置可覆盖）
    cap = CONFIG.proxy_retry.slow.backoff_max
    assert policy.delay_for(1) <= cap
    assert policy.delay_for(50) <= cap * 1.2
    assert policy.delay_for(50) >= cap * 0.8


def test_slow_budget_is_capped_by_total_wait():
    """slow 档次数不限，但累计等待超过 max_total_seconds 必须交回队列（不能挂几小时）。"""
    policy = CONFIG.proxy_retry.policy_for(ProxyTier.SLOW)
    assert policy.max_total_seconds is not None
    assert policy.is_exhausted(5, waited_seconds=0.0) is False
    assert (
        policy.is_exhausted(5, waited_seconds=policy.max_total_seconds) is True
    )


def test_policy_delay_is_monotonic_until_cap():
    policy = RequestRetryPolicy(
        tier=ProxyTier.SLOW,
        max_rounds=None,
        attempts_per_round=3,
        backoff_base=2.0,
        backoff_max=60.0,
        backoff_jitter=0.0,
    )
    delays = [policy.delay_for(n) for n in range(1, 10)]
    assert delays == sorted(delays)
    assert max(delays) == 60.0


# ---------------------------------------------------------------------------
# 失败任务延迟重入队
# ---------------------------------------------------------------------------


class _DummyCrawler(UnlimitedCrawler):
    """只用来验证 _requeue_delay 的纯计算逻辑。"""

    async def is_stop(self) -> bool:
        return False

    async def key_params_gen(self, params=None):
        yield params

    async def handle_fetch(self, params):
        return None

    async def main(self, *args, **kwargs):
        return None


def test_requeue_delay_grows_and_caps():
    crawler = _DummyCrawler()
    crawler.requeue_delay_base = 5.0
    crawler.requeue_delay_max = 300.0
    first = crawler._requeue_delay(1)
    second = crawler._requeue_delay(2)
    assert 4.0 <= first <= 5.75
    assert second > first
    for retry in (10, 20, 50):
        assert crawler._requeue_delay(retry) <= 300.0


def test_requeue_delay_zero_keeps_old_behaviour():
    crawler = _DummyCrawler()
    crawler.requeue_delay_base = 0
    assert crawler._requeue_delay(1) == 0.0
