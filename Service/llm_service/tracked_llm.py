"""带调用统计的 ChatOpenAI 子类

TrackedChatOpenAI 在 invoke / ainvoke 时自动记录：
- 调用次数（总数 / 成功 / 失败）
- 是否可用（连续失败超过阈值判定为不可用，成功一次即恢复）
- 速率（最近 60 秒调用次数、平均耗时）
- 最后使用时间戳
- token 消耗量（输入 / 输出 / 总量）
"""

import time
from collections import deque
from typing import Any

from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableConfig
from langchain_openai import ChatOpenAI
from loguru import logger
from pydantic import BaseModel, Field, PrivateAttr, computed_field

# 连续失败达到该次数后判定为不可用
MAX_CONSECUTIVE_FAILURES = 3

# 速率统计窗口（秒）
RATE_WINDOW_SECONDS = 60.0


class LLMUsageStats(BaseModel):
    """单个 LLM 实例的使用统计

    Pydantic 模型：可直接 model_dump() / 作为接口响应返回，
    computed_field（available / rate_per_minute / avg_latency_seconds）
    会一并包含在序列化结果中。
    """

    invoke_count: int = Field(default=0, description="总调用次数")
    success_count: int = Field(default=0, description="成功次数")
    failure_count: int = Field(default=0, description="失败次数")
    consecutive_failures: int = Field(default=0, description="连续失败次数")
    total_elapsed_seconds: float = Field(default=0.0, description="成功调用累计耗时（秒）")
    last_used_at: float | None = Field(
        default=None, description="最后一次发起调用的 unix 时间戳"
    )
    last_error: str | None = Field(default=None, description="最近一次失败的错误信息")
    input_tokens: int = Field(default=0, description="累计输入（prompt）token 数")
    output_tokens: int = Field(default=0, description="累计输出（completion）token 数")
    total_tokens: int = Field(default=0, description="累计消耗 token 总数")

    _recent_calls: deque[float] = PrivateAttr(
        default_factory=lambda: deque(maxlen=512)
    )

    @computed_field(description="是否可用：连续失败未达到阈值即视为可用")  # type: ignore[prop-decorator]
    @property
    def available(self) -> bool:
        return self.consecutive_failures < MAX_CONSECUTIVE_FAILURES

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

    def record_failure(self, error: BaseException) -> None:
        self.failure_count += 1
        self.consecutive_failures += 1
        self.last_error = repr(error)


def _extract_token_usage(result: Any) -> dict[str, int]:
    """从调用结果中提取 token 消耗（AIMessage.usage_metadata）

    结果不是 AIMessage 或无 usage 信息时返回全 0。
    """
    if isinstance(result, AIMessage) and result.usage_metadata:
        usage = result.usage_metadata
        return {
            "input_tokens": usage.get("input_tokens", 0),
            "output_tokens": usage.get("output_tokens", 0),
            "total_tokens": usage.get("total_tokens", 0),
        }
    return {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}


class TrackedChatOpenAI(ChatOpenAI):
    """带调用统计的 ChatOpenAI

    通过 .stats 属性访问统计信息；bind() 返回的 RunnableBinding 最终仍会
    委托到本实例的 invoke/ainvoke，统计不会丢失。
    """

    _stats: LLMUsageStats = PrivateAttr(default_factory=LLMUsageStats)

    @property
    def stats(self) -> LLMUsageStats:
        return self._stats

    @property
    def available(self) -> bool:
        return self._stats.available

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
            result = super().invoke(input, config, stop=stop, **kwargs)
        except BaseException as e:
            self._stats.record_failure(e)
            logger.warning(
                "LLM 调用失败 model={} base_url={} stats={}",
                self.model_name,
                self.openai_api_base,
                self._stats.model_dump(),
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
    ) -> Any:
        self._stats.record_start()
        start = time.monotonic()
        try:
            result = await super().ainvoke(input, config, stop=stop, **kwargs)
        except BaseException as e:
            self._stats.record_failure(e)
            logger.warning(
                "LLM 调用失败 model={} base_url={} stats={}",
                self.model_name,
                self.openai_api_base,
                self._stats.model_dump(),
            )
            raise
        self._stats.record_success(
            time.monotonic() - start, **_extract_token_usage(result)
        )
        return result
