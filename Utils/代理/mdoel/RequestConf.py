import random

from pydantic import Field

from Models.base.custom_pydantic import CustomBaseModel
from Utils.代理.mdoel.ProxyTier import ProxyTier


class RequestRetryPolicy(CustomBaseModel):
    """单次业务请求的「已知错误」重试预算（与具体站点无关的通用机制）。

    语义：
    - ``max_rounds`` × ``attempts_per_round`` 是**已知可忽略错误**（代理失败 / 412 / 352 等）
      允许的总尝试次数；``max_rounds=None`` 表示不限次数（由调用方超时兜底）。
      每跨过一轮会清空「本轮已排除的代理」，即换一轮代理重新试（fast 档就是「换 2 轮代理」）；
    - 每次重试的等待时间按 ``backoff_base`` 指数增长、``backoff_max`` 封顶，
      等待期间**必须让出 worker 并发额度**（由调用方的 yield hook 实现，不在信号量内 sleep）。
    """

    tier: ProxyTier = Field(default=ProxyTier.FAST, description="该预算所属档位")
    max_rounds: int | None = Field(
        default=2, description="最多几轮代理；None 表示不限轮数"
    )
    attempts_per_round: int = Field(
        default=3, description="每轮（同一个代理轮次内）最多尝试几次"
    )
    backoff_base: float = Field(default=0.2, description="首次退避基准秒数")
    backoff_max: float = Field(default=1.0, description="单次退避上限秒数")
    backoff_jitter: float = Field(
        default=0.2, description="退避抖幅（±比例），避免同一时刻集中重试"
    )
    max_total_seconds: float | None = Field(
        default=None,
        description="单次请求的累计重试等待上限（秒），None = 不限。"
        "即使 max_rounds=None（slow 大预算），也不能让单个任务重试数小时",
    )
    log_interval: int = Field(
        default=50, description="每累计多少次重试打一条告警（0 表示不打）"
    )

    @property
    def max_attempts(self) -> int | None:
        """总尝试次数上限；``None`` 表示不限。"""
        if self.max_rounds is None:
            return None
        return max(1, self.max_rounds) * max(1, self.attempts_per_round)

    def is_exhausted(self, attempts: int, waited_seconds: float = 0.0) -> bool:
        total = self.max_attempts
        if total is not None and attempts >= total:
            return True
        return (
            self.max_total_seconds is not None
            and waited_seconds >= self.max_total_seconds
        )

    def delay_for(self, attempts: int) -> float:
        """第 ``attempts`` 次失败后的等待秒数（指数 + 抖动 + 封顶）。"""
        exponent = min(max(attempts - 1, 0), 16)
        base = max(self.backoff_base, 0.0) * (2**exponent)
        capped = min(base, max(self.backoff_max, self.backoff_base))
        if self.backoff_jitter <= 0:
            return capped
        low = capped * (1 - self.backoff_jitter)
        high = capped * (1 + self.backoff_jitter)
        return random.uniform(max(low, 0.0), max(high, capped))


class RequestConf(CustomBaseModel):
    is_use_cookie: bool = Field(False)
    is_use_available_proxy: bool = Field(
        False,
        description="历史参数：仅用于兼容旧调用。True 等价于「探测档」"
        "（tier=slow，可用未验证代理），实际取代理档位请用 tier",
    )
    is_use_custom_proxy: bool = Field(False)
    is_return_raw_response: bool = Field(False)
    tier: ProxyTier | None = Field(
        default=None,
        description="代理取用档位。None = 跟随当前爬虫上下文（默认 fast），"
        "显式赋值优先级最高",
    )
    retry_policy: RequestRetryPolicy | None = Field(
        default=None, description="显式覆盖重试预算；None = 按 tier 取 CONFIG.proxy_retry"
    )
