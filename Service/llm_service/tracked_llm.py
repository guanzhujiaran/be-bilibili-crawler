"""带调用统计（含健康状态机）的 ChatOpenAI 子类

TrackedChatOpenAI 在 invoke / ainvoke 时自动记录：
- 调用次数（总数 / 成功 / 失败）
- 健康状态（见 ``Service/llm_service/health.py`` 的状态机：healthy / cooling /
  quota_wait / suspended / removed，由服务器返回的错误分类驱动迁移）
- 速率（最近 60 秒调用次数、平均耗时）
- 最后使用时间戳
- token 消耗量（输入 / 输出 / 总量）
- **失败调用**的耗时与 token（单独放「失败桶」``failed_*``，不污染上面这些
  成功口径的量；两者合起来才能算出真实的平均耗时 / 平均 token）

结构化输出的解析失败（模型返回的 JSON 不合规）会走到这里记成一次失败，但业务层
可能本地修好并采用这次输出 —— 那其实是成功的调用。因此提供
:meth:`LLMUsageStats.record_recovered`：把最近一次失败回滚为成功，否则
``consecutive_failures`` 只增不减，健康槽位会被自己的「可修复失败」推进冷却。

状态迁移后的副作用（例如「不可恢复 → 删除配置」）不在这里做，而是通过
``set_state_change_handler`` 注入的回调交给实例池处理 —— 统计层不认识池，
池也不需要知道统计口径，各管一件事。

请求节流不做任何加锁：每个实例构建时挂一个 langchain 的
``InMemoryRateLimiter``（见 ``pool.py``），``invoke`` / ``ainvoke`` 会自动先取令牌、
超速时阻塞等待，因此并发控制交给 langchain 自己的限流器。
"""

import hashlib
import time
from collections import deque
from collections.abc import Callable
from typing import Any

from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableConfig
from langchain_openai import ChatOpenAI
from loguru import logger
from pydantic import BaseModel, Field, PrivateAttr, computed_field

from .health import (
    MAX_CONSECUTIVE_FAILURES,
    LLMFailureKind,
    LLMHealth,
    LLMState,
    classify_llm_failure,
    describe_failure,
    extract_retry_after,
)

# 速率统计窗口（秒）
RATE_WINDOW_SECONDS = 60.0


def _secret_value(value: Any) -> str:
    """把 SecretStr / str / None 统一取成明文串（仅参与指纹计算，不落日志）。"""
    if value is None:
        return ""
    getter = getattr(value, "get_secret_value", None)
    if callable(getter):
        return str(getter())
    return str(value)


def slot_fingerprint(
    base_url: str | None, model_name: str | None, token: Any
) -> str:
    """槽位指纹：``sha256("base_url|model_name|token")`` 取前 16 位十六进制。

    槽位 = 一条云端 LLM 配置；指纹是它的运行时身份（删除配置、统计观测等按它定位）。
    刻意用 sha256 而不是明文拼接：apikey 不能落到日志或报错信息里。
    """
    raw = f"{base_url or ''}|{model_name or ''}|{_secret_value(token)}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


#: 健康状态迁移回调：入参为「实例、迁移前状态、迁移后状态」。
#: 统计层不负责「不可恢复 → 删除配置」这类副作用，统一交给实例池注入的处理函数。
StateChangeHandler = Callable[["TrackedChatOpenAI", LLMState, LLMState], None]


class LLMUsageStats(BaseModel):
    """单个 LLM 实例的使用统计 + 健康状态

    Pydantic 模型：可直接 model_dump() / 作为接口响应返回，
    computed_field（health / state / available / disabled / rate_per_minute /
    avg_latency_seconds）会一并包含在序列化结果中。
    """

    invoke_count: int = Field(default=0, description="总调用次数")
    success_count: int = Field(default=0, description="成功次数")
    failure_count: int = Field(default=0, description="失败次数")
    consecutive_failures: int = Field(default=0, description="连续失败次数")
    total_elapsed_seconds: float = Field(
        default=0.0, description="成功调用累计耗时（秒）"
    )
    last_used_at: float | None = Field(
        default=None, description="最后一次发起调用的 unix 时间戳"
    )
    last_error: str | None = Field(default=None, description="最近一次失败的错误信息")
    input_tokens: int = Field(default=0, description="累计输入（prompt）token 数")
    output_tokens: int = Field(default=0, description="累计输出（completion）token 数")
    total_tokens: int = Field(default=0, description="累计消耗 token 总数")
    failed_elapsed_seconds: float = Field(
        default=0.0, description="失败调用累计耗时（秒）"
    )
    failed_input_tokens: int = Field(
        default=0, description="失败调用累计输入（prompt）token 数"
    )
    failed_output_tokens: int = Field(
        default=0, description="失败调用累计输出（completion）token 数"
    )
    failed_total_tokens: int = Field(default=0, description="失败调用累计 token 总数")
    recovered_count: int = Field(
        default=0,
        description="业务层本地修复后采用的次数（已从失败回滚为成功）",
    )
    health: LLMHealth = Field(
        default_factory=LLMHealth,
        description="健康状态机：healthy / cooling / quota_wait / suspended / removed",
    )

    _recent_calls: deque[float] = PrivateAttr(default_factory=lambda: deque(maxlen=512))
    # 「最近一次失败」暂存的耗时 / token：供 record_recovered 原样挪到成功桶
    _pending_elapsed_seconds: float = PrivateAttr(default=0.0)
    _pending_input_tokens: int = PrivateAttr(default=0)
    _pending_output_tokens: int = PrivateAttr(default=0)
    _pending_total_tokens: int = PrivateAttr(default=0)
    _has_pending_failure: bool = PrivateAttr(default=False)

    @computed_field(description="当前健康状态（见 health.LLMState）")  # type: ignore[prop-decorator]
    @property
    def state(self) -> LLMState:
        return self.health.state

    @computed_field(description="是否可用：状态为 healthy")  # type: ignore[prop-decorator]
    @property
    def available(self) -> bool:
        return self.health.available

    @computed_field(description="是否已删除：遇到不可恢复错误（模型下线/鉴权失败等）")  # type: ignore[prop-decorator]
    @property
    def disabled(self) -> bool:
        return self.health.removed

    @computed_field(description="不可用原因（健康状态机的当前原因）")  # type: ignore[prop-decorator]
    @property
    def disabled_reason(self) -> str | None:
        return self.health.reason

    @computed_field(description="自动恢复时间戳（unix 秒）；null 表示不会自动恢复")  # type: ignore[prop-decorator]
    @property
    def resume_at(self) -> float | None:
        return self.health.resume_at

    @computed_field(description="最近一次失败的分类")  # type: ignore[prop-decorator]
    @property
    def failure_kind(self) -> LLMFailureKind | None:
        return self.health.failure_kind

    @computed_field(description="最近 60 秒内的调用次数")  # type: ignore[prop-decorator]
    @property
    def rate_per_minute(self) -> int:
        cutoff = time.time() - RATE_WINDOW_SECONDS
        return sum(1 for ts in self._recent_calls if ts >= cutoff)

    @computed_field(description="成功调用的平均耗时（秒）")  # type: ignore[prop-decorator]
    @property
    def avg_latency_seconds(self) -> float:
        if self.success_count <= 0:
            return 0.0
        return self.total_elapsed_seconds / self.success_count

    @computed_field(description="单次成功调用的平均 token 消耗")  # type: ignore[prop-decorator]
    @property
    def avg_tokens_per_call(self) -> float:
        if self.success_count <= 0:
            return 0.0
        return self.total_tokens / self.success_count

    @property
    def _all_calls(self) -> int:
        """成功 + 失败的调用总数（「全部口径」的平均值用它做分母）"""
        return self.success_count + self.failure_count

    @computed_field(description="全部调用（成功 + 失败）的平均耗时（秒）")  # type: ignore[prop-decorator]
    @property
    def avg_latency_all_seconds(self) -> float:
        if self._all_calls <= 0:
            return 0.0
        return (
            self.total_elapsed_seconds + self.failed_elapsed_seconds
        ) / self._all_calls

    @computed_field(description="全部调用（成功 + 失败）的平均 token 消耗")  # type: ignore[prop-decorator]
    @property
    def avg_tokens_per_call_all(self) -> float:
        if self._all_calls <= 0:
            return 0.0
        return (self.total_tokens + self.failed_total_tokens) / self._all_calls

    def record_start(self) -> None:
        now = time.time()
        self.invoke_count += 1
        self.last_used_at = now
        self._recent_calls.append(now)

    def record_success(
        self,
        elapsed_seconds: float,
        *,
        input_tokens: int = 0,
        output_tokens: int = 0,
        total_tokens: int = 0,
    ) -> None:
        self.success_count += 1
        self.consecutive_failures = 0
        self.total_elapsed_seconds += elapsed_seconds
        self.input_tokens += input_tokens
        self.output_tokens += output_tokens
        self.total_tokens += total_tokens
        self.last_error = None
        self._clear_pending_failure()
        self.health.record_success()

    def record_failure(
        self,
        error: BaseException,
        *,
        elapsed_seconds: float = 0.0,
        input_tokens: int = 0,
        output_tokens: int = 0,
        total_tokens: int = 0,
    ) -> LLMFailureKind:
        """记录一次失败，并按错误分类驱动健康状态机迁移，返回本次失败的分类。

        ``elapsed_seconds`` / token 落在**失败桶**（``failed_*`` 字段）：上游报错时
        通常拿不到 usage，但耗时总能拿到。这些量单独统计，不污染「成功口径」的
        ``total_elapsed_seconds`` / ``total_tokens`` / ``avg_latency_seconds``。

        这组值会被暂存，供 :meth:`record_recovered` 在「业务层本地修复成功」时
        原样挪进成功桶 —— 同一次调用不应该既算失败又缺耗时。
        """
        self.failure_count += 1
        self.consecutive_failures += 1
        self.last_error = repr(error)
        self.failed_elapsed_seconds += elapsed_seconds
        self.failed_input_tokens += input_tokens
        self.failed_output_tokens += output_tokens
        self.failed_total_tokens += total_tokens
        self._pending_elapsed_seconds = elapsed_seconds
        self._pending_input_tokens = input_tokens
        self._pending_output_tokens = output_tokens
        self._pending_total_tokens = total_tokens
        self._has_pending_failure = True
        kind = classify_llm_failure(error)
        self.health.record_failure(
            kind,
            reason=describe_failure(kind, error),
            consecutive_failures=self.consecutive_failures,
            retry_after=extract_retry_after(error),
        )
        return kind

    def record_recovered(self) -> bool:
        """把「最近一次失败」纠正为一次成功（业务层本地修复成功后调用）。

        背景：结构化输出的解析失败（模型返回的 JSON 不合规）在统计层被记成一次
        失败，但业务层可以本地修好并采用这次输出 —— 它其实是一次成功的调用。
        不回滚的话 ``consecutive_failures`` 只增不减：连续 3 次就能把一个健康槽位
        推进冷却，成功率还会永远停在 0（生产日志里 449 次本地修复被这样误计，
        某模型因此累积 380 次「失败」、0 次成功）。

        回滚内容：``failure_count -1``、``success_count +1``、连续失败清零、清空
        ``last_error``；该次调用暂存的耗时 / token 从失败桶挪进成功桶；健康状态按
        一次成功重置（``REMOVED`` 终态除外，由状态机自己保证）。

        Returns:
            是否真的回滚了一次已记录的失败。返回 ``False`` 表示统计层从没记过这次
            失败 —— 例如解析发生在统计埋点之外（某些 langchain 版本把解析放在模型
            调用之后、统计层之外），那次调用已经被正常记为成功了，这里不能重复计数。
        """
        self.recovered_count += 1
        if not self._has_pending_failure:
            return False
        self.failure_count -= 1
        self.failed_elapsed_seconds -= self._pending_elapsed_seconds
        self.failed_input_tokens -= self._pending_input_tokens
        self.failed_output_tokens -= self._pending_output_tokens
        self.failed_total_tokens -= self._pending_total_tokens
        self.success_count += 1
        self.consecutive_failures = 0
        self.last_error = None
        self.total_elapsed_seconds += self._pending_elapsed_seconds
        self.input_tokens += self._pending_input_tokens
        self.output_tokens += self._pending_output_tokens
        self.total_tokens += self._pending_total_tokens
        self._clear_pending_failure()
        self.health.record_success()
        return True

    def _clear_pending_failure(self) -> None:
        """清掉「最近一次失败」的暂存值（成功或回滚后调用）"""
        self._has_pending_failure = False
        self._pending_elapsed_seconds = 0.0
        self._pending_input_tokens = 0
        self._pending_output_tokens = 0
        self._pending_total_tokens = 0


def _extract_token_usage(result: Any) -> dict[str, int]:
    """从调用结果中提取 token 消耗（AIMessage.usage_metadata）

    结果不是 AIMessage 或无 usage 信息时返回全 0。
    """
    if isinstance(result, AIMessage) and result.usage_metadata:
        return _usage_metadata_to_counts(result)
    return {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}


def _usage_metadata_to_counts(message: AIMessage) -> dict[str, int]:
    """把 ``AIMessage.usage_metadata`` 压成统计用的三个计数"""
    usage = message.usage_metadata or {}
    return {
        "input_tokens": usage.get("input_tokens", 0),
        "output_tokens": usage.get("output_tokens", 0),
        "total_tokens": usage.get("total_tokens", 0),
    }


def _find_ai_message(value: Any) -> AIMessage | None:
    """在一层局部变量里找 ``AIMessage``：可能是它本身，也可能装在 Generation 列表里"""
    if isinstance(value, AIMessage):
        return value
    if isinstance(value, (list, tuple)) and value:
        message = getattr(value[0], "message", None)
        if isinstance(message, AIMessage):
            return message
    return None


def _extract_token_usage_from_exception(error: BaseException) -> dict[str, int]:
    """尽力从失败调用的异常回溯里取回 token 消耗。

    解析类失败（模型已经返回了内容，只是格式不合规）其实已经消耗了 token，
    但异常本身不带 usage —— 它只留在 langchain 解析器帧的局部变量里（那些帧
    仍在 traceback 上）。所以这里顺着回溯找 ``AIMessage``，取到就记下，
    取不到返回全 0。

    纯只读、全程吞异常：取不到只是少一个观测值，绝不能影响失败处理本身。
    """
    try:
        frame = error.__traceback__
        while frame is not None:
            for value in list(frame.tb_frame.f_locals.values()):
                message = _find_ai_message(value)
                if message is not None and message.usage_metadata:
                    return _usage_metadata_to_counts(message)
            frame = frame.tb_next
    except Exception:  # noqa: BLE001 - 观测性数据取不到就算了
        pass
    return {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}


class TrackedChatOpenAI(ChatOpenAI):
    """带调用统计与健康状态机的 ChatOpenAI

    通过 .stats 属性访问统计信息；bind() 返回的 RunnableBinding 最终仍会
    委托到本实例的 invoke/ainvoke，统计不会丢失。

    可用性语义：
    - ``available``：状态机处于 ``healthy``（冷却中的槽位不可用，到点自动恢复）；
    - ``disabled``：状态机处于 ``removed``，即已因不可恢复错误被彻底删除。
    """

    _stats: LLMUsageStats = PrivateAttr(default_factory=LLMUsageStats)
    _state_change_handler: StateChangeHandler | None = PrivateAttr(default=None)

    def set_state_change_handler(self, handler: StateChangeHandler) -> None:
        """注入状态迁移回调（实例池在构建实例时注册，用于删除配置 / 告警等副作用）"""
        self._state_change_handler = handler

    @property
    def stats(self) -> LLMUsageStats:
        return self._stats

    @property
    def state(self) -> LLMState:
        return self._stats.state

    @property
    def slot_fingerprint(self) -> str:
        """本实例对应槽位的指纹（base_url + model_name + token）。

        与 ``Service/llm_service/pool.py`` 计算槽位指纹的口径保持一致，
        用于「不可恢复 → 删除配置」时定位实例。
        """
        return slot_fingerprint(
            self.openai_api_base, self.model_name, self.openai_api_key
        )

    @property
    def available(self) -> bool:
        return self._stats.available

    @property
    def disabled(self) -> bool:
        return self._stats.disabled

    def _log_state_change(self, previous: LLMState, current: LLMState) -> None:
        """状态迁移时打印一次明确日志，便于定位到具体是哪条模型配置出了问题"""
        health = self._stats.health
        if current is LLMState.HEALTHY:
            logger.info(
                "LLM 槽位已恢复可用（{} → {}）：model={} base_url={}",
                previous.value,
                current.value,
                self.model_name,
                self.openai_api_base,
            )
            return
        logger.warning(
            "LLM 槽位状态迁移：{} → {}；model={} base_url={} 原因={} 自动恢复时间={}",
            previous.value,
            current.value,
            self.model_name,
            self.openai_api_base,
            health.reason,
            health.resume_at,
        )

    def _notify_state_change(self, previous: LLMState, current: LLMState) -> None:
        """把状态迁移通知给注入的处理函数（未注入或状态未变时什么也不做）"""
        handler = self._state_change_handler
        if handler is None or previous is current:
            return
        try:
            handler(self, previous, current)
        except Exception as e:  # noqa: BLE001 - 副作用失败不应影响调用结果与状态机
            logger.warning(
                "LLM 槽位状态迁移回调执行失败：{}: {}", type(e).__name__, e
            )

    def _failure_context(self) -> str:
        """把定位所需的少量统计压成一行，避免整份 stats 反复刷屏。"""
        stats = self._stats
        return (
            f"model={self.model_name} base_url={self.openai_api_base} "
            f"调用次数={stats.invoke_count} 失败次数={stats.failure_count} "
            f"连续失败={stats.consecutive_failures} 状态={stats.state.value}"
        )

    def _log_call_failure(self, error: BaseException, *, level: str) -> None:
        """统一的失败日志：异常类型 + 异常内容 + 定位上下文。

        注意：消息与参数必须作为独立参数传给 loguru（不能用 f-string 拼成一条），
        否则 {} 占位符既不会被替换，还会把多行信息粘成一行。
        """
        logger.log(
            level,
            "LLM 调用失败 | {}: {}\n  {}",
            type(error).__name__,
            error,
            self._failure_context(),
        )

    def _record_call_failure(
        self, error: Exception, *, elapsed_seconds: float, level: str
    ) -> None:
        """统一的失败记账：统计 → 迁移日志 → 迁移回调 → 失败日志。

        只接受 ``Exception``：``asyncio.CancelledError`` / ``KeyboardInterrupt`` /
        ``SystemExit`` 不是槽位的错，不能污染统计与健康状态 —— 生产里进程关闭时
        一次性把 6 个健康槽位记成了失败，把它们的连续失败次数推高。
        """
        previous = self._stats.state
        self._stats.record_failure(
            error,
            elapsed_seconds=elapsed_seconds,
            **_extract_token_usage_from_exception(error),
        )
        current = self._stats.state
        if current is not previous:
            self._log_state_change(previous, current)
            self._notify_state_change(previous, current)
        self._log_call_failure(error, level=level)

    def invoke(
        self,
        input: Any,
        config: RunnableConfig | None = None,
        *,
        stop: list[str] | None = None,
        **kwargs: Any,
    ) -> Any:
        self._stats.record_start()
        start = time.monotonic()
        try:
            # 不加锁：请求节流由实例自身的 langchain rate limiter 负责。
            result = super().invoke(input, config, stop=stop, **kwargs)
        except Exception as e:
            self._record_call_failure(
                e, elapsed_seconds=time.monotonic() - start, level="WARNING"
            )
            raise
        self._stats.record_success(
            time.monotonic() - start, **_extract_token_usage(result)
        )
        return result

    async def ainvoke(
        self,
        input: Any,
        config: RunnableConfig | None = None,
        *,
        stop: list[str] | None = None,
        **kwargs: Any,
    ) -> AIMessage:
        self._stats.record_start()
        start = time.monotonic()
        # 不加锁：请求节流由实例自身的 langchain rate limiter 负责，
        # 超速时它会在取令牌处自动阻塞等待。
        try:
            result: AIMessage = await super().ainvoke(
                input, config, stop=stop, **kwargs
            )
        except Exception as e:
            self._record_call_failure(
                e, elapsed_seconds=time.monotonic() - start, level="ERROR"
            )
            raise
        self._stats.record_success(
            time.monotonic() - start, **_extract_token_usage(result)
        )
        return result


__all__ = [
    "MAX_CONSECUTIVE_FAILURES",
    "LLMFailureKind",
    "LLMHealth",
    "LLMState",
    "LLMUsageStats",
    "RATE_WINDOW_SECONDS",
    "StateChangeHandler",
    "TrackedChatOpenAI",
    "classify_llm_failure",
    "describe_failure",
    "extract_retry_after",
    "slot_fingerprint",
]
