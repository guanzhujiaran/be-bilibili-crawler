"""单条消息的「确认超时预算」：避免未确认消息超时被 RabbitMQ 强制 nack。

**为什么需要它**

RabbitMQ 有 ``consumer_timeout``（broker 端配置，默认 30 分钟 = 1800s）：
消费者收到消息后在超时时限内没有 ack/nack，broker 会**强制关闭 channel 并把消息重投**，
表现为「莫名重投 + 重复消费」；而且此时 channel 已关闭，再调 ``msg.ack()`` 也会失败。

而本服务的入库队列恰好是「刻意长时间不确认」的设计：没有可用槽位时本机等待、
槽位冷却（限流 / 今日额度 / 欠费）时按恢复时间等待 —— 一旦等待超过 30 分钟，
就会撞上 broker 的超时。因此需要一层**主动**机制：在 broker 超时**之前**把消息交还队列。

**做法**

每次投递建一个 :class:`ConsumeBudget`：记录**收到消息时的时间戳**，
所有可能长时间阻塞的等待都用 :meth:`~ConsumeBudget.bound` 按剩余预算截断；
预算耗尽时抛 :class:`ConsumeBudgetExceeded`，由消费流程在**释放完锁之后**
``nack(requeue=True)`` 把消息交还队列，下一个消费者以全新预算重新开始。

**为什么不用 ``asyncio.wait_for``**

硬超时会在协程 ``await`` 中间抛取消，可能打断「正在写库」的流程；
这里改为**协作式**：只在安全的等待点检查预算，绝不在写库中途中断。

重投的幂等性由「redis 去重锁 + 写库前 ``_already_stored`` 复查 + 写库 upsert」保证，
且 ``nack`` 发生在锁释放之后，不存在「重投副本撞上未释放的锁被 ack 丢弃」的窗口。
"""

import asyncio
import random
import time
from dataclasses import dataclass

#: 秒：主动交还消息前的随机抖动上限（避免同一批消息在同一刻集体重投形成热点）
CONSUME_REQUEUE_JITTER = 5.0


class ConsumeBudgetExceeded(RuntimeError):
    """本轮消费已用完「消息确认超时预算」，需要把消息交还队列重投。"""


@dataclass(slots=True)
class ConsumeBudget:
    """一次消息投递的时间预算（从收到消息开始算）。

    Attributes:
        total_seconds: 预算总时长（秒），应小于 broker 的 ``consumer_timeout``。
        started_at: 本轮开始处理的 ``time.monotonic()`` 时间戳。
        queue_name: 队列名（日志用）。
        label: 消息的业务标识（日志用，例如去重锁 key）。
    """

    total_seconds: float
    started_at: float
    queue_name: str
    label: str

    @classmethod
    def start(
        cls, *, total_seconds: float, queue_name: str, label: str
    ) -> "ConsumeBudget":
        """以「此刻」为起点创建预算（应在收到消息后立刻调用）。

        ``total_seconds`` 允许为 0（表示「没有预算」，立即过期），不允许为负。
        """
        if total_seconds < 0:
            raise ValueError("total_seconds 不得为负")
        return cls(
            total_seconds=total_seconds,
            started_at=time.monotonic(),
            queue_name=queue_name,
            label=label,
        )

    @property
    def deadline(self) -> float:
        """预算耗尽的单调时间戳"""
        return self.started_at + self.total_seconds

    def elapsed(self) -> float:
        """本轮已耗时（秒）"""
        return time.monotonic() - self.started_at

    def remaining(self) -> float:
        """剩余预算（秒），不会小于 0"""
        return max(0.0, self.deadline - time.monotonic())

    def expired(self, *, reserve: float = 0.0) -> bool:
        """剩余预算是否已不足以再做一次等待 / 收尾（``reserve`` 为预留的收尾时间）"""
        return self.remaining() <= reserve

    def bound(self, wait: float) -> float:
        """把一次等待截断到剩余预算内（返回 0 表示没有预算可用了）"""
        return max(0.0, min(wait, self.remaining()))

    def describe(self) -> str:
        """一行可读的预算状态，用于日志"""
        return (
            f"队列={self.queue_name} 消息={self.label} "
            f"已耗时={self.elapsed():.0f}s 剩余={self.remaining():.0f}s"
        )

    async def sleep_within(self, wait: float) -> None:
        """按剩余预算截断后等待（预算不足时不等待，由调用方检查 :meth:`expired`）"""
        bounded = self.bound(wait)
        if bounded > 0:
            await asyncio.sleep(bounded)

    async def requeue_jitter(self) -> None:
        """交还消息前的随机抖动：避免同一批消息在同一刻集体重投"""
        await asyncio.sleep(random.uniform(1.0, CONSUME_REQUEUE_JITTER))


__all__ = [
    "CONSUME_REQUEUE_JITTER",
    "ConsumeBudget",
    "ConsumeBudgetExceeded",
]
