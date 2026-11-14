"""请求级上下文：代理档位、失败代理规避、重试让出并发额度。

这三件事都必须「跟着当前 worker 任务走」，而不是放在全局变量里：

- ``tier``：同一个进程里 fast / slow 两种爬虫同时在跑，请求内部要按自己所属的档位取代理；
- ``attempt``：一次请求重试时应换一个代理，需要记住本任务已经失败的代理串；
- ``retry_yield``：重试的 sleep 必须发生在 worker 并发额度**之外**（历史 bug：
  sleep 在 sem 内，20 个 worker 被坏代理长期占住）。

用 ``ContextVar`` 而非模块级单例，是因为 ``asyncio.create_task`` 会复制上下文，
既不会串任务，也不需要显式清理。本模块刻意不 import 任何业务模块，避免与 CONFIG /
爬虫基类形成循环依赖。
"""

import asyncio
from collections import deque
from contextvars import ContextVar, Token
from typing import Awaitable, Callable

from log.base_log import request_with_proxy_logger
from Utils.代理.mdoel.ProxyTier import ProxyTier
from Utils.代理.mdoel.RequestConf import RequestConf

#: 重试等待期间的「让出并发额度」钩子：参数是等待秒数
RetryYieldHook = Callable[[float], Awaitable[None]]

#: 单个任务内最多记住多少个失败代理（只用于规避，不做去重统计）
_PROXY_EXCLUDE_HISTORY = 16


class ProxyAttemptState:
    """单个 worker 任务内的代理尝试状态。

    仅记录「本任务最近失败过的代理串」，供下一次选代理时排除，
    这样 fast 档「换 2 轮代理」才是真的换，而不是反复命中同一个坏代理。
    """

    __slots__ = ("_excluded",)

    def __init__(self, history: int = _PROXY_EXCLUDE_HISTORY) -> None:
        self._excluded: deque[str] = deque(maxlen=max(1, history))

    @property
    def excluded(self) -> frozenset[str]:
        return frozenset(self._excluded)

    def note_failed(self, proxy_key: str | None) -> None:
        if proxy_key:
            self._excluded.append(proxy_key)

    def begin_round(self) -> None:
        """进入新一轮代理：清空排除名单（新一轮允许重试之前的代理）。"""
        self._excluded.clear()


_tier_ctx: ContextVar[ProxyTier] = ContextVar("proxy_tier_ctx", default=ProxyTier.FAST)
_attempt_ctx: ContextVar[ProxyAttemptState | None] = ContextVar(
    "proxy_attempt_ctx", default=None
)
_retry_yield_ctx: ContextVar[RetryYieldHook | None] = ContextVar(
    "proxy_retry_yield_ctx", default=None
)


# region tier
def current_proxy_tier() -> ProxyTier:
    return _tier_ctx.get()


def set_proxy_tier(tier: ProxyTier) -> Token[ProxyTier]:
    return _tier_ctx.set(tier)


def reset_proxy_tier(token: Token[ProxyTier]) -> None:
    _tier_ctx.reset(token)


def resolve_tier(request_conf: RequestConf | None = None) -> ProxyTier:
    """解析本次请求的代理档位。

    优先级：``RequestConf.tier`` > 旧参数 ``is_use_available_proxy``（探测档）
    > 当前爬虫上下文 > fast。
    """
    if request_conf is not None:
        if request_conf.tier is not None:
            return request_conf.tier
        if request_conf.is_use_available_proxy:
            return ProxyTier.SLOW
    return _tier_ctx.get()


# endregion


# region 失败代理规避
def current_attempt_state() -> ProxyAttemptState | None:
    return _attempt_ctx.get()


def bind_attempt_state(state: ProxyAttemptState) -> Token[ProxyAttemptState | None]:
    return _attempt_ctx.set(state)


def reset_attempt_state(token: Token[ProxyAttemptState | None]) -> None:
    _attempt_ctx.reset(token)


def note_failed_proxy(proxy_key: str | None) -> None:
    state = _attempt_ctx.get()
    if state is not None:
        state.note_failed(proxy_key)


def excluded_proxy_keys() -> frozenset[str]:
    state = _attempt_ctx.get()
    return state.excluded if state is not None else frozenset()


def begin_retry_round() -> None:
    state = _attempt_ctx.get()
    if state is not None:
        state.begin_round()


# endregion


# region 重试让出并发额度
def bind_retry_yield(hook: RetryYieldHook | None) -> Token[RetryYieldHook | None]:
    return _retry_yield_ctx.set(hook)


def reset_retry_yield(token: Token[RetryYieldHook | None]) -> None:
    _retry_yield_ctx.reset(token)


def has_retry_yield() -> bool:
    return _retry_yield_ctx.get() is not None


async def retry_sleep(delay: float) -> None:
    """重试等待：优先走「让出并发额度」的钩子，没有钩子时退化为普通 sleep。

    钩子由爬虫 worker 注入（释放本次任务的并发额度 → sleep → 重新取回），
    这样坏代理重试期间不会继续占着 worker 名额。
    """
    hook = _retry_yield_ctx.get()
    if hook is None:
        if delay > 0:
            request_with_proxy_logger.debug(
                f"重试等待 {delay:.2f}s（未绑定让出钩子，直接 sleep）"
            )
            await asyncio.sleep(delay)
        return
    await hook(delay)


# endregion
