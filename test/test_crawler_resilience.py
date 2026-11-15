# -*- coding: utf-8 -*-
"""爬虫「故障收敛」单测：重入队上限 / 代理池熔断 / 失败告警聚合 / 专线出口观测。

对应线上事故（2026-11-15）：专线被风控冷却后业务请求静默落到免费池，池子整体
不可用时同一批任务被无限打回队列，ERROR 稳定 26 条/分钟、有效数据为 0。

这里全部用内存桩 / 假时钟，不连 Redis / MySQL / RabbitMQ。
"""

import asyncio
import time
from datetime import datetime, timedelta
from typing import AsyncGenerator

import pytest

from CONFIG import CrawlerAlertConfig, CrawlerBreakerConfig
from Models.base.custom_pydantic import CustomBaseModelHashable
from Service.BaseCrawler.config import CrawlerConfig, PluginConfig
from Service.BaseCrawler.CrawlerType import UnlimitedCrawler
import Service.BaseCrawler.CrawlerType as crawler_type_module
from Service.BaseCrawler.model.base import WorkerModel, WorkerStatus
from Service.BaseCrawler.plugin.statusPlugin import StatsPlugin
from Service.BaseCrawler.resilience import (
    FailureAlertThrottle,
    ProxyCircuitBreaker,
    ProxyCircuitState,
)
from Utils.通用.Common import ConcurrencyLimit
from Utils.代理.mdoel.ProxyTier import ProxyTier
from Utils.代理.redisProxyRequest.RedisRequestProxy import RequestWithProxy
import Utils.代理.数据库操作.ProxyHealth as proxy_health_module
from Utils.代理.数据库操作.ProxyHealth import ProxyHealthStore


class _Params(CustomBaseModelHashable):
    rid: int

    def __hash__(self) -> int:
        return hash(self.rid)


class _FakeLog:
    """收集 WARNING / INFO 的假日志器（loguru 之外的鸭子类型）。"""

    def __init__(self) -> None:
        self.warnings: list[str] = []
        self.infos: list[str] = []

    def warning(self, msg: str, *args, **kwargs) -> None:
        self.warnings.append(str(msg))

    def info(self, msg: str, *args, **kwargs) -> None:
        self.infos.append(str(msg))

    def debug(self, msg: str, *args, **kwargs) -> None:
        pass

    def exception(self, msg: str, *args, **kwargs) -> None:
        self.warnings.append(str(msg))


class _FakeClock:
    """可控的单调时钟，避免测试真的 sleep。"""

    def __init__(self, now: float = 0.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


# ---------------------------------------------------------------------------
# 重入队上限
# ---------------------------------------------------------------------------


class _RequeueCapConfig(CrawlerConfig):
    crawler_name: str = "requeue-cap-test"
    concurrency_key: str = "reserve"
    requeue_on_fetch_fail: bool = True
    requeue_on_timeout: bool = True
    max_retries: int = -1
    max_requeue_per_task: int = 3
    requeue_max_task_seconds: float = 0
    requeue_delay_base: int | None = 0
    worker_error_delay: int = 0
    log_error: bool = False
    proxy_tier: ProxyTier = ProxyTier.FAST
    plugins: list[PluginConfig] = [PluginConfig("stats_plugin", StatsPlugin)]


class _DurationCapConfig(_RequeueCapConfig):
    max_requeue_per_task: int = 100
    requeue_max_task_seconds: float = 60.0


class _FailingCrawler(UnlimitedCrawler[_Params]):
    Config = _RequeueCapConfig

    def __init__(self) -> None:
        self.calls = 0
        super().__init__()

    async def is_stop(self) -> bool:
        return False

    async def key_params_gen(
        self, params: _Params | None = None
    ) -> AsyncGenerator[_Params, None]:
        if params is None:
            return
        yield params

    async def handle_fetch(self, params: _Params) -> WorkerStatus:
        self.calls += 1
        return WorkerStatus.fail

    async def main(self, *args, **kwargs) -> None:
        return None


class _DurationCrawler(_FailingCrawler):
    Config = _DurationCapConfig


def test_requeue_limit_reason_prefers_strictest_cap():
    """max_retries / 重入队次数 / 累计时长三道限取最先到的一条。"""
    crawler = _FailingCrawler()
    worker_model = WorkerModel(params=_Params(rid=1), seqId=0)
    worker_model.fetchStatus = WorkerStatus.fail

    assert crawler._requeue_limit_reason(worker_model) is None
    worker_model.retry_count = 3
    limit = crawler._requeue_limit_reason(worker_model)
    assert limit is not None and limit[0] == "requeue_cap"
    assert crawler._should_requeue(worker_model) is False
    assert crawler._final_fail_tally.requeue_cap == 1

    slow = _DurationCrawler()
    stale = WorkerModel(
        params=_Params(rid=2),
        seqId=0,
        created_at=datetime.now() - timedelta(seconds=120),
    )
    stale.fetchStatus = WorkerStatus.fail
    limit = slow._requeue_limit_reason(stale)
    assert limit is not None and limit[0] == "duration_cap"
    assert slow._should_requeue(stale) is False
    assert slow._final_fail_tally.duration_cap == 1


async def test_requeue_cap_terminates_task_and_keeps_stats():
    """达到重入队上限后任务终止：不再打回队列，且统计口径不变。"""
    crawler = _FailingCrawler()
    gate = ConcurrencyLimit(1)
    queue: asyncio.Queue[WorkerModel | None] = asyncio.Queue()
    worker_model = WorkerModel(params=_Params(rid=1), seqId=0)
    await queue.put(worker_model)
    workers = [asyncio.create_task(crawler.worker(queue, gate))]
    try:
        for _ in range(300):
            if crawler.stats_plugin.processed_items_count >= 4:
                break
            await asyncio.sleep(0.02)
    finally:
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

    assert crawler.calls == 4, "首次 + 3 次重入队后必须停止"
    assert worker_model.retry_count == 3
    assert worker_model.fetchStatus is WorkerStatus.fail
    # 统计口径保持：重排 3 次 + 最终失败 1 次（不再线性增长）
    assert crawler.stats_plugin.requeued_count == 3
    assert crawler.stats_plugin.fail_count == 1
    assert crawler.stats_plugin.attempt_count == 4


# ---------------------------------------------------------------------------
# 代理池熔断
# ---------------------------------------------------------------------------


def _breaker(clock: _FakeClock, **overrides) -> tuple[ProxyCircuitBreaker, _FakeLog]:
    config = CrawlerBreakerConfig(
        enabled=True,
        consecutive_proxy_failures=3,
        window_seconds=1000.0,
        min_samples=1000,
        min_success_rate=0.0,
        open_seconds_min=10.0,
        open_seconds_max=20.0,
        probe_limit=1,
        probe_successes=2,
        log_interval=1.0,
        **overrides,
    )
    log = _FakeLog()
    return ProxyCircuitBreaker("breaker-test", config, log, clock=clock), log


def test_breaker_opens_after_consecutive_pool_failures():
    clock = _FakeClock(100.0)
    breaker, log = _breaker(clock)
    for _ in range(3):
        breaker.record_failure("重试预算耗尽", pool_level=True)

    assert breaker.state is ProxyCircuitState.OPEN
    assert breaker.snapshot().retry_after_seconds == pytest.approx(10.0, abs=0.1)
    assert sum("熔断开启" in msg for msg in log.warnings) == 1
    # 非池级失败不累计连续计数
    breaker2, _ = _breaker(_FakeClock())
    for _ in range(10):
        breaker2.record_failure("单条代理网络错误", pool_level=False)
    assert breaker2.state is ProxyCircuitState.CLOSED


async def test_breaker_half_open_recovers_after_probe_successes():
    clock = _FakeClock(0.0)
    breaker, log = _breaker(clock)
    for _ in range(3):
        breaker.record_failure("没有可用代理", pool_level=True)
    assert breaker.state is ProxyCircuitState.OPEN

    clock.now += 10.5
    await breaker.wait_until_ready()
    assert breaker.state is ProxyCircuitState.HALF_OPEN
    assert sum("进入半开探测：" in msg for msg in log.warnings) == 1

    breaker.record_success()
    assert breaker.state is ProxyCircuitState.HALF_OPEN
    breaker.record_success()
    assert breaker.state is ProxyCircuitState.CLOSED
    assert sum("熔断解除" in msg for msg in log.warnings) == 1


async def test_half_open_probe_lease_releases_leaked_slot():
    """探测协程被取消时不回记账分支：名额必须靠租约强制归还，否则永久卡半开。"""
    clock = _FakeClock(0.0)
    breaker, _ = _breaker(clock, probe_lease_seconds=30.0)
    for _ in range(3):
        breaker.record_failure("没有可用代理", pool_level=True)

    clock.now += 10.5
    await breaker.wait_until_ready()  # 占用唯一探测名额
    assert breaker.snapshot().probes_in_flight == 1

    # 租约未到期：第二个请求拿不到名额（会持续等待）
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(breaker.wait_until_ready(), timeout=0.2)

    # 租约到期：按「协程被取消」处理，强制归还后可以继续探测
    clock.now += 31
    await asyncio.wait_for(breaker.wait_until_ready(), timeout=1.0)
    assert breaker.snapshot().probes_in_flight == 1


def test_breaker_opens_on_low_success_rate():
    config = CrawlerBreakerConfig(
        enabled=True,
        consecutive_proxy_failures=0,
        window_seconds=1000.0,
        min_samples=3,
        min_success_rate=0.6,
        open_seconds_min=5.0,
        open_seconds_max=5.0,
        probe_successes=1,
        log_interval=1.0,
    )
    clock = _FakeClock()
    breaker = ProxyCircuitBreaker("rate", config, _FakeLog(), clock=clock)
    breaker.record_success()
    breaker.record_success()
    breaker.record_failure("超时", pool_level=False)
    assert breaker.state is ProxyCircuitState.CLOSED
    breaker.record_failure("超时", pool_level=False)
    assert breaker.state is ProxyCircuitState.OPEN


def test_breaker_disabled_never_opens():
    config = CrawlerBreakerConfig(enabled=False, consecutive_proxy_failures=1)
    breaker = ProxyCircuitBreaker("disabled", config, _FakeLog(), clock=_FakeClock())
    for _ in range(5):
        breaker.record_failure("重试预算耗尽", pool_level=True)
    assert breaker.state is ProxyCircuitState.CLOSED


# ---------------------------------------------------------------------------
# 失败告警聚合
# ---------------------------------------------------------------------------


class _AlertConfig(_RequeueCapConfig):
    log_error: bool = True
    requeue_on_fetch_fail: bool = False
    max_requeue_per_task: int = 0


class _RaisingCrawler(UnlimitedCrawler[_Params]):
    Config = _AlertConfig

    def __init__(self, exc: BaseException) -> None:
        self.exc = exc
        super().__init__()

    async def is_stop(self) -> bool:
        return False

    async def key_params_gen(
        self, params: _Params | None = None
    ) -> AsyncGenerator[_Params, None]:
        if params is None:
            return
        yield params

    async def handle_fetch(self, params: _Params) -> WorkerStatus:
        raise self.exc

    async def main(self, *args, **kwargs) -> None:
        return None


def test_alert_throttle_pushes_first_then_aggregates():
    clock = _FakeClock(1000.0)
    config = CrawlerAlertConfig(
        enabled=True,
        window_seconds=100.0,
        min_interval_seconds=10.0,
        max_push_per_window=1,
    )
    throttle = FailureAlertThrottle(config, clock=clock)

    first = throttle.hit()
    assert first.push is True and first.aggregated is False and first.count == 1

    for _ in range(5):
        clock.now += 1
        decision = throttle.hit()
        assert decision.push is False
    assert decision.suppressed == 5
    assert decision.count == 6

    # 窗口滚动：把上一窗口积压合并成一条汇总
    clock.now = 1000.0 + 101
    summary = throttle.hit()
    assert summary.push is True and summary.aggregated is True
    assert summary.suppressed == 5
    assert summary.count == 7
    assert throttle.flush() is None


def test_alert_throttle_flush_pushes_pending():
    clock = _FakeClock()
    config = CrawlerAlertConfig(
        enabled=True,
        window_seconds=300.0,
        min_interval_seconds=60.0,
        max_push_per_window=1,
    )
    throttle = FailureAlertThrottle(config, clock=clock)
    throttle.hit()
    for _ in range(3):
        clock.now += 1
        throttle.hit()
    decision = throttle.flush()
    assert decision is not None and decision.push is True
    assert decision.suppressed == 3


def test_alert_throttle_disabled_pushes_every_time():
    config = CrawlerAlertConfig(enabled=False)
    throttle = FailureAlertThrottle(config, clock=_FakeClock())
    decisions = [throttle.hit() for _ in range(3)]
    assert all(item.push for item in decisions)


async def test_execute_fetch_aggregates_failure_alerts(monkeypatch):
    """_execute_fetch 的失败推送必须按窗口聚合，不能每个任务一条。"""
    pushed: list[str] = []

    async def _fake_push(*, subject: str, content: str) -> None:
        pushed.append(content)

    monkeypatch.setattr(crawler_type_module, "a_push_error", _fake_push)
    crawler = _RaisingCrawler(RuntimeError("boom"))
    worker_model = WorkerModel(params=_Params(rid=1), seqId=0)

    for _ in range(5):
        status = await crawler._execute_fetch(worker_model)
        assert status is WorkerStatus.fail

    assert len(pushed) == 1, f"失败风暴必须聚合成一条，实际 {len(pushed)} 条"
    assert "失败首次告警" in pushed[0]
    assert "RuntimeError" in pushed[0]
    # 窗口内其余 4 次只累加，不产生新的推送
    assert crawler._alert_throttle.snapshot().suppressed == 4


# ---------------------------------------------------------------------------
# 专线出口可观测性
# ---------------------------------------------------------------------------


def test_local_exit_gate_logs_cooldown_start_and_recovery():
    rwp = RequestWithProxy()
    log = _FakeLog()
    rwp.log = log

    rwp._note_local_exit_gate(True, "前缀 2001:db8::/64")
    rwp._note_local_exit_gate(True, "前缀 2001:db8::/64")  # 重复调用不刷屏
    assert rwp._local_exit_cooling is True
    rwp._note_local_exit_gate(False)
    assert rwp._local_exit_cooling is False

    assert sum("进入风控冷却" in msg for msg in log.warnings) == 1
    assert sum("风控冷却结束" in msg for msg in log.warnings) == 1
    # 冷却开始那条必须写清降级策略与出口占比
    start = next(msg for msg in log.warnings if "进入风控冷却" in msg)
    assert "降级策略" in start
    assert "出口占比" in start


class _FakeCooldownRedis:
    """只实现 HGET 的最小 Redis 桩。"""

    def __init__(self) -> None:
        self.cooldown_until = 0

    async def hget(self, key: str, field: str) -> str:
        return str(self.cooldown_until)


async def test_prefix_cooldown_transition_warns_once(monkeypatch):
    store = ProxyHealthStore()
    fake = _FakeCooldownRedis()
    monkeypatch.setattr(store, "_get_client", lambda: fake)
    log = _FakeLog()
    monkeypatch.setattr(proxy_health_module, "sql_log", log)

    fake.cooldown_until = int(time.time()) + 3600
    assert await store.is_prefix_cooling("2001:db8::") is True
    assert await store.is_prefix_cooling("2001:db8::") is True

    fake.cooldown_until = 0
    assert await store.is_prefix_cooling("2001:db8::") is False

    assert sum("处于风控冷却中" in msg for msg in log.warnings) == 1
    assert sum("风控冷却结束" in msg for msg in log.warnings) == 1
