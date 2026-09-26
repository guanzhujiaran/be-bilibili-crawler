"""MQ 消费者「等待回退（backoff）」重试策略。

背景（生产日志 ``be-bilibili-crawler-20260926093527.log``，2026-09-26 09:19~09:35）：

- 代理机不可用期间 ``get_lot_notice`` 全量失败，消费者异常兜底里直接 ``msg.nack()``，
  而 faststream 的 ``RabbitMessage.nack`` 默认 ``requeue=True``，消息被**立刻**重新投递，
  形成「失败 → 立即重投 → 再失败」的热循环，把下游一起拖垮
  （``curl`` 句柄泄漏 → ``OSError: [Errno 24] Too many open files``，
  日志自身写盘也失败；16 分钟内刷出 15.6 万行堆栈）；
- 缺少「等一等再试」的等待策略，重试节奏完全不受控。

可靠性设计（**不丢消息 + 最终一定处理成功**）：

1. 单条消息的处理逻辑失败后，按「指数等待 + 抖动」在进程内 sleep 后重试，
   期间**不 ack 也不 nack**（消息保持未确认，不会被 broker 立刻重投）；
2. 重试次数 / 总耗时上限由 ``settings.mq_consume_*`` 控制；
3. 一轮重试彻底耗尽后：先走「自定义错误回调」（默认：逐条写 MQ 错误日志 + 把告警投递给
   message-service），再 ``nack(requeue=True)`` 把消息**重新入队**；
4. RabbitMQ 会把它再次投递回来，消费者开始**新一轮**等待回退 —— 如此往复直到成功，
   因此既不丢消息，也不会因为「立即重投」而空转烧 CPU / 打爆下游。

也就是说：**持久化与重投完全交给 RabbitMQ（队列 durable + 消息 ``persist=True``，
进程崩溃时未确认消息也会被 broker 重投），Python 侧只负责执行 backoff 策略。**

等待与重试本身由 :mod:`bili_common.core.backoff` 提供（参考 ``backoff`` 库实现），
本模块只负责把它接到 MQ 消费语义上（ack / nack / 错误回调）。
"""

import inspect
import time
from collections.abc import Awaitable, Callable
from typing import Any

from bili_common.core.backoff import (
    BackoffConfig,
    BackoffContext,
    expo_wait,
    full_jitter,
    run_with_backoff,
)
from faststream.rabbit.fastapi import RabbitMessage

from CONFIG import settings
from log.base_log import MQ_logger
from Utils.推送.PushMe import a_push_error

#: 「重试耗尽」错误回调：接收回退上下文（含最后一次异常、重试次数、耗时）
ErrorCallback = Callable[[BackoffContext], Awaitable[None] | None]

#: 推送正文里异常文本的截断长度：保证「同队列 + 同类异常」的正文完全一致，
#: message-service 才能按内容去重计数（压成 ``×N`` 而不是一长串互不相同的条目）
_ALERT_EXCEPTION_LIMIT = 200


def format_consume_error(module_name: str, exc: BaseException, params: Any) -> str:
    """拼装统一格式的消费错误描述（沿用历史 ``handle_exception`` 的字段顺序）。"""
    return (
        f"[ERROR]队列:{module_name}\n"
        f"异常类型:{type(exc)}\n"
        f"异常:{exc}\n"
        f"时间:{time.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"参数:{params}"
    )


def build_alert_content(module_name: str, exc: BaseException) -> str:
    """推送用的告警正文（**稳定可去重**）。

    刻意**不带时间戳、不带单条消息参数**：只有「同因失败」的正文完全一致，
    message-service 才能把冷却期内的 347 条同因失败压成摘要里的「×347」。
    逐条明细（时间戳 + 消息参数 + 完整堆栈）只写 ``MQ_logger`` 日志，不进推送。
    """
    detail = " ".join(str(exc).split())
    if len(detail) > _ALERT_EXCEPTION_LIMIT:
        detail = f"{detail[:_ALERT_EXCEPTION_LIMIT]}…"
    return f"队列: {module_name}\n异常: {type(exc).__name__}: {detail}"


async def default_error_callback(
    module_name: str,
    params: Any,
    ctx: BackoffContext,
) -> None:
    """默认「本轮重试耗尽」错误回调：逐条写 MQ 错误日志 + 推送告警。

    对应需求里的「自定义错误回调」——调用方可通过 ``on_giveup`` 换掉本实现，
    把错误送去别的地方（推送服务 / 监控 / 工单系统）。

    分工（重要）：

    - **逐条明细**写 ``MQ_logger``（含时间戳、消息参数、完整堆栈），排查用；
    - **推送**只在 message-service 侧做聚合治理：按「接收人（渠道凭据）× 标题」分桶，
      首条立即推送，冷却期内的同类告警合并成一条摘要（见 be-message-service 的
      ``push_aggregator``）。因此这里的 content 由 :func:`build_alert_content` 生成，
      保持稳定可去重 —— 否则摘要会退化成「一长串互不相同的条目」。
    """
    exc = ctx.exception or RuntimeError("重试耗尽但未捕获到异常")
    error_msg = format_consume_error(module_name, exc, params)
    # opt(exception=...)：显式带上最后一次异常，保证日志里能看到完整堆栈
    MQ_logger.opt(exception=exc).error(
        f"【{module_name}】本轮重试 {ctx.attempt} 次后仍失败（耗时 {ctx.elapsed:.1f}s），"
        f"重新入队等待下一轮：\n{error_msg}"
    )
    # subject 固定 → 标题稳定（[f][服务@地址] MQ消费失败），
    # 与 message-service 的「按标题分桶」配合，同一类告警共用一个冷却窗口
    await a_push_error(
        content=build_alert_content(module_name, exc),
        subject="MQ消费失败",
    )


def build_consume_backoff_config(
    *,
    module_name: str,
    params: Any,
    on_giveup: ErrorCallback | None = None,
) -> BackoffConfig:
    """构造消费者用的等待回退配置（参数全部来自 ``settings``，可环境变量覆盖）。"""
    max_tries = settings.mq_consume_max_tries
    # max_tries<=0 视为「不限次数」，交给 max_time 兜底；两者都为 0 则退化为单次执行
    tries: int | None = max_tries if max_tries > 0 else None
    max_time: float | None = settings.mq_consume_max_time
    if max_time is not None and max_time <= 0:
        max_time = None

    async def _on_backoff(ctx: BackoffContext) -> None:
        MQ_logger.warning(
            f"【{module_name}】处理失败，等待 {ctx.wait:.1f}s 后重试"
            f"（第 {ctx.attempt} 次失败，已耗时 {ctx.elapsed:.1f}s）："
            f"{type(ctx.exception).__name__}: {ctx.exception}"
        )

    async def _on_giveup(ctx: BackoffContext) -> None:
        if on_giveup is not None:
            result = on_giveup(ctx)
            if inspect.isawaitable(result):
                await result
            return
        await default_error_callback(module_name, params, ctx)

    return BackoffConfig(
        max_tries=tries,
        max_time=max_time,
        wait_gen=expo_wait(
            base=settings.mq_consume_backoff_base,
            factor=settings.mq_consume_backoff_factor,
            max_value=settings.mq_consume_backoff_max_wait,
        ),
        jitter=full_jitter,
        on_backoff=_on_backoff,
        on_giveup=_on_giveup,
    )


async def _requeue(module_name: str, msg: RabbitMessage) -> None:
    """把消息重新入队（``requeue=True``），由 broker 稍后重投，开启下一轮等待回退。"""
    try:
        await msg.nack(requeue=True)
    except Exception as e:  # noqa: BLE001 - nack 失败不应再抛出掩盖原始错误
        # nack 失败时消息仍是 unacked 状态，broker 在 channel 关闭后会重投，仍然不丢
        MQ_logger.error(f"【{module_name}】nack 重新入队失败（消息仍未被确认）：{e}")


async def consume_with_backoff(
    *,
    module_name: str,
    params: Any,
    msg: RabbitMessage,
    handler: Callable[[], Awaitable[Any]],
    on_giveup: ErrorCallback | None = None,
) -> Any | None:
    """在「等待回退」保护下执行一条消息的处理逻辑。

    Args:
        module_name: 队列名（日志 / 告警用）。
        params: 消息体（出错时写进告警内容）。
        msg: faststream 的 RabbitMessage，负责最终的 ack / nack。
        handler: 真正的业务处理协程工厂；**内部不要再 ack/nack**，成功与否由本函数统一确认。
        on_giveup: 自定义「本轮重试耗尽」错误回调；缺省走
            :func:`default_error_callback`（日志 + 推送到 message-service）。

    Returns:
        ``handler`` 的返回值；本轮重试耗尽时返回 ``None``（消息已 nack 重新入队，
        会被再次投递并开始新一轮等待回退，直到成功为止）。
    """

    async def _run() -> Any:
        result = await handler()
        # ack 也纳入重试范围：channel 抖动导致 ack 失败时，退避后再确认，
        # 避免「业务已处理但未确认」被 broker 立刻重投造成重复消费。
        await msg.ack()
        return result

    try:
        return await run_with_backoff(
            _run,
            config=build_consume_backoff_config(
                module_name=module_name,
                params=params,
                on_giveup=on_giveup,
            ),
        )
    except Exception:  # noqa: BLE001 - 已由 on_giveup 完成日志与告警，这里只管重投
        await _requeue(module_name, msg)
        return None


__all__ = [
    "ErrorCallback",
    "build_consume_backoff_config",
    "consume_with_backoff",
    "default_error_callback",
    "format_consume_error",
]
