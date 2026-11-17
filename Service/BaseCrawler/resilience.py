# -*- coding: utf-8 -*-
"""爬虫级「故障收敛」基础设施：代理池熔断 + 失败告警聚合。

为什么需要这两样东西（2026-11-15 线上事故）：

- 专线（自建出口）被 412 风控冷却 7200s 后，``local_ipv6_weight=10`` /
  ``pool_weight=1000`` 让几乎全部业务请求静默落到免费代理池；池子整体不可用时
  每个任务都会走满重试预算（fast 2 轮 × 3 次）再抛 ``ProxyRetryBudgetExhausted``，
  打回队列后立刻被下一个 worker 取走 —— ``attempts`` 与 ``requeued_count``
  线性增长、有效数据为 0、ERROR 稳定 26 条/分钟。
- 同时 ``_execute_fetch`` 对每个失败任务都 ``a_push_error``，失败风暴会把推送通道打爆。

这里只做**机制**（通用、可配置、与站点无关），业务差异仍由各爬虫 Config / 动作编排表达：

1. :class:`ProxyCircuitBreaker`：连续「代理池级」失败或窗口成功率过低 → 熔断，
   ``wait_until_ready`` 用 ``await asyncio.sleep`` 退避（不阻塞事件循环），
   到期进入半开探测，连续成功后关闭并打 WARNING；
2. :class:`FailureAlertThrottle`：按「爬虫 + 时间窗口」聚合失败告警，
   首条立即推送、窗口滚动时补一条汇总，避免逐条刷屏。

两个类都支持注入 ``clock``，便于单测在不 sleep 的情况下验证状态机。
"""

import asyncio
import time
from collections import deque
from collections.abc import Callable

from pydantic import BaseModel, Field

from bili_common.models import StrEnumAutoDoc
from CONFIG import CONFIG
from Service.GrpcModule.Models.CustomRequestErrorModel import (
    ProxyRetryBudgetExhausted,
    RequestProxyUnavailableError,
)

#: 「代理池级」失败：代表「换哪个代理都没用」，连续出现即说明池子整体不可用。
#: 网络层单点失败（RequestProxyNetworkError）与 412/352 不算在内：
#: 前者只说明某条代理坏了，后者是段级风控信号（已有前缀冷却，不能靠熔断绕行）。
POOL_LEVEL_ERRORS: tuple[type[BaseException], ...] = (
    ProxyRetryBudgetExhausted,
    RequestProxyUnavailableError,
)


def is_pool_level_error(err: BaseException) -> bool:
    """该异常是否属于「代理池整体不可用」类别。"""
    return isinstance(err, POOL_LEVEL_ERRORS)


class ProxyCircuitState(StrEnumAutoDoc):
    """熔断状态。"""

    CLOSED = "closed"  # 正常取数
    OPEN = "open"  # 熔断中：暂停取数与重试
    HALF_OPEN = "half_open"  # 半开：只放少量探测请求验证是否恢复


class BreakerSnapshot(BaseModel):
    """熔断状态快照（日志 / 接口观测用，不含任何业务对象）。"""

    state: ProxyCircuitState = Field(default=ProxyCircuitState.CLOSED)
    consecutive_pool_failures: int = Field(default=0, description="连续代理池级失败次数")
    samples: int = Field(default=0, description="成功率窗口内的样本数")
    success_rate: float = Field(default=0.0, description="成功率窗口内的成功率（0~1）")
    open_count: int = Field(default=0, description="本轮连续熔断次数（用于退避递增）")
    retry_after_seconds: float = Field(default=0.0, description="距离下次探测还需等待秒数")
    probes_in_flight: int = Field(default=0, description="半开状态下在途探测数")


class ProxyCircuitBreaker:
    """代理池整体不可用时的爬虫级熔断器。

    状态机：

    - ``CLOSED`` → ``OPEN``：连续 ``consecutive_proxy_failures`` 次池级失败，
      或成功率窗口样本足够且成功率低于 ``min_success_rate``；
    - ``OPEN`` → ``HALF_OPEN``：退避到期（``open_seconds_min × 2^open_count``，封顶
      ``open_seconds_max``）；
    - ``HALF_OPEN`` → ``CLOSED``：连续 ``probe_successes`` 次成功；
    - ``HALF_OPEN`` → ``OPEN``：任一探测失败，退避倍数递增。

    ``wait_until_ready`` 是唯一的阻塞点，All 用 ``await asyncio.sleep``，
    **禁止** ``time.sleep``（事件循环里阻塞会让 /docs 与监控一起卡死）。
    """

    def __init__(
        self,
        name: str | Callable[[], str],
        config=None,
        log=None,
        clock: Callable[[], float] = time.monotonic,
    ):
        self._name_provider = name
        self.cfg = config if config is not None else CONFIG.crawler_breaker
        self._log = log
        self._clock = clock
        self._state = ProxyCircuitState.CLOSED
        self._opened_until = 0.0
        self._open_count = 0
        self._consecutive_pool_failures = 0
        self._probe_successes = 0
        self._probes_in_flight = 0
        # 探测名额租约到期时间：协程被取消后不会回到 record_*，租约到期强制归还
        self._probe_lease_until = 0.0
        # 成功率窗口：(单调时钟, 是否成功)
        # 必须用 deque + popleft：list.pop(0) 是 O(n)，稳态上万样本时 snapshot 会退化成
        # O(n²)（2026-11-17 线上事件循环卡顿 15.17s）。
        self._window: deque[tuple[float, bool]] = deque()
        # 窗口内成功样本数（增量维护）：snapshot 只读计数器，O(1)，不再遍历窗口。
        # 注意不要改成 asyncio.Queue：它是任务协作原语，不能 O(1) 统计，也无法避免
        # CPU 计算占用事件循环（async 函数里的纯计算同样会阻塞 loop）。
        self._window_ok = 0
        self._last_open_log_ts = 0.0

    # region 观测
    @property
    def name(self) -> str:
        provider = self._name_provider
        return provider() if callable(provider) else provider

    @property
    def state(self) -> ProxyCircuitState:
        return self._state

    @property
    def is_open(self) -> bool:
        return self._state is ProxyCircuitState.OPEN

    def _logger(self):
        return self._log

    def _log_warning(self, msg: str) -> None:
        logger = self._logger()
        if logger is not None:
            logger.warning(f"[{self.name}] {msg}")

    def _log_info(self, msg: str) -> None:
        logger = self._logger()
        if logger is not None:
            logger.info(f"[{self.name}] {msg}")

    def _window_stats(self, now: float) -> tuple[int, float]:
        """裁剪并统计成功率窗口：``(样本数, 成功率)``。

        统计走增量计数器（``_window_ok``），整体 **O(1)**，不会在 snapshot 时遍历窗口。
        """
        window = max(0.0, float(self.cfg.window_seconds))
        if window <= 0:
            self._window.clear()
            self._window_ok = 0
            return 0, 0.0
        cutoff = now - window
        # deque.popleft 是 O(1)；`_window` 按时间递增，裁剪到第一条未过期即可
        while self._window and self._window[0][0] < cutoff:
            _, success = self._window.popleft()
            if success:
                self._window_ok = max(0, self._window_ok - 1)
        samples = len(self._window)
        if not samples:
            return 0, 0.0
        return samples, self._window_ok / samples

    def _prune_window(self, now: float) -> None:
        """记录新样本后裁剪窗口：时间窗口 + 样本数硬上限（防止高 QPS 下无限增长）。"""
        window = max(0.0, float(self.cfg.window_seconds))
        if window <= 0:
            self._window.clear()
            self._window_ok = 0
            return
        cutoff = now - window
        while self._window and self._window[0][0] < cutoff:
            _, success = self._window.popleft()
            if success:
                self._window_ok = max(0, self._window_ok - 1)
        max_samples = int(getattr(self.cfg, "window_max_samples", 2000) or 0)
        if max_samples > 0:
            while len(self._window) > max_samples:
                _, success = self._window.popleft()
                if success:
                    self._window_ok = max(0, self._window_ok - 1)

    async def snapshot(self) -> BreakerSnapshot:
        """熔断状态快照（异步接口：显式让出一次事件循环，避免调用方同步阻塞）。

        统计本身是 O(1)（增量计数器），``await asyncio.sleep(0)`` 只是让其它协程
        有机会插入，不代表把 CPU 计算移出了事件循环。
        """
        await asyncio.sleep(0)
        now = self._clock()
        samples, rate = self._window_stats(now)
        retry_after = (
            max(0.0, self._opened_until - now)
            if self._state is ProxyCircuitState.OPEN
            else 0.0
        )
        return BreakerSnapshot(
            state=self._state,
            consecutive_pool_failures=self._consecutive_pool_failures,
            samples=samples,
            success_rate=round(rate, 4),
            open_count=self._open_count,
            retry_after_seconds=round(retry_after, 1),
            probes_in_flight=self._probes_in_flight,
        )

    # endregion

    # region 状态迁移
    async def record_success(self) -> None:
        """一次成功取数：清空连续计数；半开状态下累计探测成功次数。

        异步接口：先 ``await asyncio.sleep(0)`` 让出一次事件循环。
        """
        if not self.cfg.enabled:
            return
        await asyncio.sleep(0)
        now = self._clock()
        self._release_probe()
        self._window.append((now, True))
        self._window_ok += 1
        # 成功也要参与裁剪：高 QPS 稳态下全是成功样本，只有失败分支裁剪会无限增长
        self._prune_window(now)
        self._consecutive_pool_failures = 0
        if self._state is ProxyCircuitState.HALF_OPEN:
            self._probe_successes += 1
            if self._probe_successes >= max(1, self.cfg.probe_successes):
                self._close()

    async def record_failure(self, reason: str, *, pool_level: bool = False) -> None:
        """一次失败取数（异步接口：先让出一次事件循环）。

        Args:
            reason: 失败原因（写进日志，便于区分超时 / 预算耗尽 / 网络错误）；
            pool_level: 是否属于「代理池整体不可用」类别（决定连续熔断计数）。
        """
        if not self.cfg.enabled:
            return
        await asyncio.sleep(0)
        now = self._clock()
        self._release_probe()
        self._window.append((now, False))
        self._prune_window(now)
        if pool_level:
            self._consecutive_pool_failures += 1
        if self._state is ProxyCircuitState.HALF_OPEN:
            self._open(f"半开探测失败：{reason}")
            return
        if self._state is ProxyCircuitState.CLOSED:
            samples, rate = self._window_stats(now)
            threshold = max(0, int(self.cfg.consecutive_proxy_failures))
            if threshold > 0 and self._consecutive_pool_failures >= threshold:
                self._open(
                    f"连续 {self._consecutive_pool_failures} 次代理池级失败"
                    f"（阈值 {threshold}），最后一次：{reason}"
                )
                return
            min_samples = max(1, int(self.cfg.min_samples))
            if samples >= min_samples and rate < float(self.cfg.min_success_rate):
                self._open(
                    f"成功率 {rate:.0%} 低于阈值 {self.cfg.min_success_rate:.0%}"
                    f"（窗口 {samples} 次样本，最后一次：{reason}）"
                )

    def _release_probe(self) -> None:
        if self._probes_in_flight > 0:
            self._probes_in_flight -= 1

    def _cooldown_seconds(self) -> float:
        low = max(1.0, float(self.cfg.open_seconds_min))
        high = max(low, float(self.cfg.open_seconds_max))
        return min(high, low * (2 ** min(self._open_count, 8)))

    def _open(self, reason: str) -> None:
        now = self._clock()
        cooldown = self._cooldown_seconds()
        self._state = ProxyCircuitState.OPEN
        self._opened_until = now + cooldown
        self._open_count += 1
        self._probe_successes = 0
        self._probes_in_flight = 0
        self._last_open_log_ts = now
        samples, rate = self._window_stats(now)
        self._log_warning(
            f"代理池熔断开启：{reason}；暂停取数与重试 {cooldown:.0f}s"
            f"（第 {self._open_count} 次，成功 {rate:.0%}/{samples} 次样本，"
            f"连续池级失败 {self._consecutive_pool_failures}）；"
            f"退避到期后进入半开探测，连续 {self.cfg.probe_successes} 次成功才恢复"
        )

    def _enter_half_open(self) -> None:
        self._state = ProxyCircuitState.HALF_OPEN
        self._probe_successes = 0
        self._probes_in_flight = 0
        self._log_warning(
            f"代理池熔断退避结束，进入半开探测："
            f"只放行 {max(1, self.cfg.probe_limit)} 个并发请求，"
            f"连续 {max(1, self.cfg.probe_successes)} 次成功即恢复；"
            f"任一失败则重新熔断 {self._cooldown_seconds():.0f}s"
        )

    def _close(self) -> None:
        self._state = ProxyCircuitState.CLOSED
        self._opened_until = 0.0
        self._open_count = 0
        self._probe_successes = 0
        self._probes_in_flight = 0
        self._consecutive_pool_failures = 0
        self._log_warning("代理池熔断解除：半开探测连续成功，恢复正常取数（失败计数已清零）")

    # endregion

    async def wait_until_ready(self) -> None:
        """熔断期间的退避等待（``await asyncio.sleep``，绝不阻塞事件循环）。

        ``CLOSED`` 直接返回；``OPEN`` 等到退避到期并切到半开；``HALF_OPEN``
        受 ``probe_limit`` 限制，探测名额满时短暂让出。
        """
        if not self.cfg.enabled:
            return
        while True:
            if self._state is ProxyCircuitState.CLOSED:
                return
            if self._state is ProxyCircuitState.HALF_OPEN:
                if await self._acquire_probe():
                    return
                # 等名额期间被判定再次熔断：回到外层继续退避，绝不在熔断中取数
                continue
            now = self._clock()
            remaining = self._opened_until - now
            if remaining <= 0:
                self._enter_half_open()
                continue
            # 分片 sleep：既能被取消，也能按 log_interval 留进度日志
            chunk = min(remaining, max(0.05, float(self.cfg.log_interval)))
            if now - self._last_open_log_ts >= max(1.0, float(self.cfg.log_interval)):
                self._last_open_log_ts = now
                self._log_info(
                    f"代理池熔断中：还需等待 {remaining:.0f}s 后进入半开探测"
                    f"（第 {self._open_count} 次熔断）"
                )
            await asyncio.sleep(chunk)

    async def _acquire_probe(self) -> bool:
        """半开状态下占用一个探测名额；名额满时每 0.5s 重试一次。

        Returns:
            True = 已占用名额，可以发探测请求；
            False = 状态已不是半开（再次熔断 / 已恢复），调用方必须回到外层循环。

        名额带租约：探测协程被「杀旧开新」取消时不会走到 ``record_success/failure``，
        没有租约的话 ``_probes_in_flight`` 会永久打满，熔断器永远卡在半开。
        """
        limit = max(1, int(self.cfg.probe_limit))
        lease = max(30.0, float(self.cfg.probe_lease_seconds))
        while self._state is ProxyCircuitState.HALF_OPEN:
            now = self._clock()
            if self._probes_in_flight >= limit and now >= self._probe_lease_until:
                self._log_info(
                    f"半开探测名额租约到期（{self._probes_in_flight} 个在途），"
                    f"按协程被取消处理并强制归还"
                )
                self._probes_in_flight = 0
            if self._probes_in_flight < limit:
                self._probes_in_flight += 1
                self._probe_lease_until = now + lease
                return True
            await asyncio.sleep(0.5)
        return False


class AlertDecision(BaseModel):
    """一次失败告警的处置决定。"""

    push: bool = Field(default=False, description="本次是否真的推送")
    aggregated: bool = Field(default=False, description="是否为窗口汇总推送")
    count: int = Field(default=0, description="当前窗口累计失败数（含已推送的）")
    suppressed: int = Field(default=0, description="本次推送覆盖的、之前被聚合的数量")
    window_seconds: float = Field(default=0.0, description="聚合窗口（秒）")


class AlertThrottleSnapshot(BaseModel):
    """告警节流状态快照（接口观测用）。"""

    window_count: int = Field(default=0)
    suppressed: int = Field(default=0)
    pushes_in_window: int = Field(default=0)
    total_failures: int = Field(default=0)


class FailureAlertThrottle:
    """按「爬虫 + 时间窗口」聚合失败告警。

    策略（与 ``CONFIG.crawler_alert`` 对应）：

    - 窗口内首条失败**立即推送**（保留「第一时间感知」）；
    - 之后的失败只累加，受 ``min_interval_seconds`` 与 ``max_push_per_window`` 双重限制；
    - 窗口滚动时，把上一窗口积压的数量作为**一条汇总**补推；
    - 运行结束时 ``flush`` 兜底把剩余积压推出去。

    这样失败风暴（26 条/分钟）下推送量是「每窗口 1 条」而不是「每分钟 26 条」。
    """

    def __init__(self, config=None, clock: Callable[[], float] = time.monotonic):
        self.cfg = config if config is not None else CONFIG.crawler_alert
        self._clock = clock
        # None = 尚未开始任何窗口（不能用 0 当哨兵：注入的假时钟可能就从 0 开始）
        self._window_start: float | None = None
        # -inf = 从未推送过，保证「窗口内首条」一定立刻推送
        self._last_push_ts = float("-inf")
        self._count = 0
        self._suppressed = 0
        self._pushes = 0
        self._total = 0

    def reset(self) -> None:
        """新一轮 run 开始时复位窗口（熔断状态不在这里复位）。"""
        self._window_start = None
        self._last_push_ts = float("-inf")
        self._count = 0
        self._suppressed = 0
        self._pushes = 0
        self._total = 0

    def snapshot(self) -> AlertThrottleSnapshot:
        return AlertThrottleSnapshot(
            window_count=self._count,
            suppressed=self._suppressed,
            pushes_in_window=self._pushes,
            total_failures=self._total,
        )

    def hit(self, now: float | None = None) -> AlertDecision:
        """登记一次失败，返回本次是否推送 / 是否为汇总。"""
        current = self._clock() if now is None else now
        window = max(1.0, float(self.cfg.window_seconds))
        if self._window_start is None:
            self._window_start = current
        if not self.cfg.enabled:
            self._total += 1
            return AlertDecision(push=True, count=1, window_seconds=window)

        rolled = current - self._window_start >= window
        prev_count = self._count
        pending = self._suppressed
        if rolled:
            self._window_start = current
            self._count = 0
            self._suppressed = 0
            self._pushes = 0
            if pending > 0:
                # 上一个窗口的积压：合并成一条汇总推送（不受 min_interval 限制）
                self._count = 1
                self._pushes = 1
                self._last_push_ts = current
                self._total += 1
                return AlertDecision(
                    push=True,
                    aggregated=True,
                    count=prev_count + 1,
                    suppressed=pending,
                    window_seconds=window,
                )

        self._count += 1
        self._total += 1
        can_push = (
            self._pushes < max(1, int(self.cfg.max_push_per_window))
            and current - self._last_push_ts >= max(0.0, float(self.cfg.min_interval_seconds))
        )
        if can_push:
            self._pushes += 1
            self._last_push_ts = current
            suppressed = self._suppressed
            self._suppressed = 0
            return AlertDecision(
                push=True,
                count=self._count,
                suppressed=suppressed,
                window_seconds=window,
            )
        self._suppressed += 1
        return AlertDecision(
            push=False,
            count=self._count,
            suppressed=self._suppressed,
            window_seconds=window,
        )

    def flush(self, now: float | None = None) -> AlertDecision | None:
        """运行结束时把窗口内积压的失败补一条汇总（没有积压返回 ``None``）。"""
        if not self.cfg.enabled or self._suppressed <= 0:
            return None
        current = self._clock() if now is None else now
        suppressed = self._suppressed
        count = self._count
        self._suppressed = 0
        self._count = 0
        self._pushes = 0
        self._window_start = current
        return AlertDecision(
            push=True,
            aggregated=True,
            count=count,
            suppressed=suppressed,
            window_seconds=max(1.0, float(self.cfg.window_seconds)),
        )


class FinalFailTally(BaseModel):
    """「达到重入队上限 / 时长上限」的最终失败计数（按原因区分，便于运行结束汇总）。"""

    max_retries: int = Field(default=0, description="达到 max_retries 的次数")
    requeue_cap: int = Field(default=0, description="达到 max_requeue_per_task 的次数")
    duration_cap: int = Field(default=0, description="达到 requeue_max_task_seconds 的次数")

    @property
    def total(self) -> int:
        return self.max_retries + self.requeue_cap + self.duration_cap

    def add(self, reason: str) -> int:
        """按原因 +1 并返回新的总数。"""
        if reason == "requeue_cap":
            self.requeue_cap += 1
        elif reason == "duration_cap":
            self.duration_cap += 1
        else:
            self.max_retries += 1
        return self.total

    def to_log_str(self) -> str:
        return (
            f"最终失败 {self.total} 个（重入队上限 {self.requeue_cap} / "
            f"时长上限 {self.duration_cap} / 最大重试 {self.max_retries}）"
        )
