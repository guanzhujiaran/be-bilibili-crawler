"""LLM 槽位健康状态机：按服务器返回的错误分类决定「怎么恢复」。

改造前只有 ``disabled: bool`` + ``consecutive_failures: int`` 两个字段，
无法区分「限流要等一会儿」「今日额度要等明天」「欠费要等充值」「彻底坏掉要删除」，
于是只能一刀切：要么无限重试（白打请求），要么永久熔断（误伤可恢复的 key）。

本模块把这件事收敛成一个显式状态机：

- :class:`LLMFailureKind`：错误分类（由 :func:`classify_llm_failure` 判定）；
- :class:`LLMState`：槽位状态；
- :class:`LLMHealth`：状态 + 迁移规则（冷却到期用「读取时惰性刷新」实现，不依赖后台定时器）。

迁移关系::

    HEALTHY ── RATE_LIMITED ──────────► COOLING ──(冷却到点 / 成功)──► HEALTHY
    HEALTHY ── TRANSIENT/REQUEST ×N ──► COOLING ──(冷却到点 / 成功)──► HEALTHY
    HEALTHY ── OUTPUT_INVALID ×N ─────► COOLING ──(冷却到点 / 成功)──► HEALTHY
    HEALTHY ── QUOTA_EXHAUSTED ───────► QUOTA_WAIT ──(跨过次日 0 点)──► HEALTHY
    HEALTHY ── SUSPENDED ─────────────► SUSPENDED ──(冷却到点 / 热更新)──► HEALTHY
    HEALTHY ── PERMANENT ─────────────► REMOVED（终态，配置被删除）

设计要点：

1. **分类只看服务器返回的 status code + code + 文案 + Retry-After**，
   关键词集合全部是模块级常量，方便按上游厂商差异调整；
2. **REMOVED 是终态**：成功调用也不会把它拉回来（配置已经从配置列表里删掉了）；
3. **冷却到期不靠定时器**：``state`` / ``available`` 等属性读取时先做一次惰性刷新，
   因此没有需要关停的后台任务，也不会出现「到点了但没人触发」的问题。
"""

from __future__ import annotations

import json
import random
import time
from datetime import datetime, timedelta

from bili_common.models import StrEnumAutoDoc
from pydantic import BaseModel, PrivateAttr, computed_field

#: 连续失败（瞬时故障 / 本次请求问题）达到该次数后进入冷却
MAX_CONSECUTIVE_FAILURES = 3

#: 秒：限流（429）首次冷却时长
COOLING_BASE_SECONDS = 60.0
#: 秒：限流冷却上限（30 分钟）
COOLING_MAX_SECONDS = 1800.0
#: 秒：瞬时故障（5xx / 超时 / 连接错误）首次冷却时长
TRANSIENT_COOLING_SECONDS = 15.0
#: 秒：瞬时故障冷却上限（2 分钟）
TRANSIENT_COOLING_MAX_SECONDS = 120.0
#: 秒：欠费 / 余额不足首次停用时长（1 小时）
SUSPENDED_BASE_SECONDS = 3600.0
#: 秒：欠费 / 余额不足停用上限（24 小时）
SUSPENDED_MAX_SECONDS = 86400.0
#: 秒：每日额度次日恢复后的随机抖动，避免所有机器整点一起冲上去
QUOTA_RESET_JITTER_SECONDS = 60.0
#: 失败原因文案的长度上限（避免把整段响应体写进内存与接口响应）
REASON_MAX_LENGTH = 300


class LLMFailureKind(StrEnumAutoDoc):
    """一次 LLM 调用失败按「能否恢复 / 怎么恢复」分成的类别。"""

    #: 不可恢复：鉴权失败 / 模型下线 / 模型不存在 → 直接删除配置
    PERMANENT = "permanent"
    #: 欠费 / 余额不足 → 长时间停用，充值或人工热更新后恢复
    SUSPENDED = "suspended"
    #: 今日额度耗尽 → 等到次日 0 点自动恢复
    QUOTA_EXHAUSTED = "quota_exhausted"
    #: 频率限流 → 休息一会儿再试
    RATE_LIMITED = "rate_limited"
    #: 瞬时故障（5xx / 超时 / 连接错误）→ 连续若干次后短暂冷却
    TRANSIENT = "transient"
    #: 本次请求内容问题（超长 / 内容审核等）→ 只计数，不误伤槽位
    REQUEST = "request"
    #: 模型返回的文本不符合结果模型（JSON 非法 / 只回散文 / 无 tool call）
    #: → 只计数，连续若干次后短暂冷却；**不删配置**（是模型不擅长，不是配置坏了）
    OUTPUT_INVALID = "output_invalid"


class LLMState(StrEnumAutoDoc):
    """单个 LLM 槽位（一条 base_url + model_name + token 配置）的健康状态。"""

    #: 正常，可调用
    HEALTHY = "healthy"
    #: 短暂冷却（限流 / 连续瞬时失败），到点自动恢复
    COOLING = "cooling"
    #: 今日额度耗尽，等到次日 0 点自动恢复
    QUOTA_WAIT = "quota_wait"
    #: 欠费 / 余额不足，长时间冷却，充值或人工热更新可立即恢复
    SUSPENDED = "suspended"
    #: 不可恢复，配置已删除（终态）
    REMOVED = "removed"


#: 会自动恢复的状态（读取时惰性刷新用）
AUTO_RECOVER_STATES = frozenset(
    {LLMState.COOLING, LLMState.QUOTA_WAIT, LLMState.SUSPENDED}
)

#: HTTP 状态码：重试没有任何意义的「不可恢复」错误。
#: 401/403 鉴权或权限、404 模型/接口不存在、410 模型已下线（Gone，例如
#: ``The model '...' has reached its end of life ... and is no longer available``）。
#: 注意：429（限流）、5xx 与超时都是可恢复的，不能放进来。
_PERMANENT_STATUS_CODES = frozenset({401, 403, 404, 410})
#: 明确代表「模型 / 密钥 / 权限层面不可恢复」的 error code
_PERMANENT_CODES = frozenset(
    {
        "invalid_api_key",
        "invalid_authentication",
        "authentication_error",
        "model_not_found",
        "model_not_available",
        "model_decommissioned",
        "no_permission",
        "permission_denied",
        "account_deactivated",
        "unsupported_model",
    }
)
#: 明确代表「余额 / 额度耗尽，需要充值」的 error code
_SUSPENDED_CODES = frozenset(
    {
        "insufficient_quota",
        "quota_exceeded",
        "insufficient_balance",
        "credit_balance_too_low",
        "billing_hard_limit_reached",
        "account_suspended",
    }
)
#: 代表「本次请求内容的问题」（不是槽位的问题）的 error code
_REQUEST_CODES = frozenset(
    {
        "context_length_exceeded",
        "content_filter",
        "content_policy_violation",
        "string_above_max_length",
        "invalid_image_format",
    }
)
#: 429 文案里出现这些词 → 判为「今日额度耗尽」（等明天）
_DAILY_KEYWORDS = (
    "daily",
    "per day",
    "per-day",
    "today",
    "24 hours",
    "24h",
    "rpd",
    "每日",
    "今日",
    "当天",
    "一天",
)
#: 429 文案里出现这些词 → 判为「余额 / 额度耗尽」（等充值）
_SUSPENDED_KEYWORDS = (
    "balance",
    "credit",
    "billing",
    "insufficient",
    "余额",
    "欠费",
    "充值",
    "账户",
)
#: 文案里出现这些词 → 判为「模型不存在 / 已下线」（不可恢复，可删除配置）。
#: 不限状态码：上游把「模型已下线」报成 400 / 404 / 410 / 503 的情况都出现过，
#: 文案才是真正稳定的信号（例如 NVIDIA 的 410 会把 end of life 写在 detail 里）。
_MODEL_MISSING_KEYWORDS = (
    "model not found",
    "model_not_found",
    "does not exist",
    "unknown model",
    "unsupported model",
    "model is not available",
    "no provider supported",
    "end of life",
    "end-of-life",
    "no longer available",
    "decommissioned",
    "has been deprecated",
    "已下线",
    "不再提供",
)

#: 代表「模型返回的文本不符合结果模型」的异常类名
#: （pydantic 的校验失败，以及 langchain 自己的解析器异常）
_OUTPUT_INVALID_ERROR_NAMES = frozenset(
    {"ValidationError", "OutputParserException", "OutputParserError"}
)
#: 解析器在「模型没按格式返回」时会抛的通用异常。
#: 这些类型本身太宽泛，只有确认抛出点确实在 langchain 解析器里才算输出不合规
#: （典型例子：模型只回散文、没有 tool call，解析器对空结果做 ``result[0]`` 越界）。
_PARSER_SYMPTOM_ERROR_NAMES = frozenset({"IndexError", "KeyError", "ValueError"})


def _extract_status_code(error: BaseException) -> int | None:
    """从异常中提取 HTTP 状态码（openai 的 APIStatusError 及其子类均有该属性）"""
    status_code = getattr(error, "status_code", None)
    if isinstance(status_code, int):
        return status_code
    response = getattr(error, "response", None)
    response_status = getattr(response, "status_code", None)
    if isinstance(response_status, int):
        return response_status
    return None


def _extract_error_code(error: BaseException) -> str | None:
    """从异常中提取上游 error code（如 ``insufficient_quota`` / ``model_not_found``）"""
    code = getattr(error, "code", None)
    if isinstance(code, str) and code:
        return code
    body = getattr(error, "body", None)
    if isinstance(body, dict):
        err = body.get("error")
        if isinstance(err, dict):
            for key in ("code", "type"):
                value = err.get(key)
                if isinstance(value, str) and value:
                    return value
        for key in ("code", "type"):
            value = body.get(key)
            if isinstance(value, str) and value:
                return value
    return None


def _extract_error_text(error: BaseException) -> str:
    """把异常里所有可用于分类的文案压成一段小写文本（消息 + code + 响应体）"""
    parts: list[str] = [str(error)]
    for attr in ("message", "code"):
        value = getattr(error, attr, None)
        if isinstance(value, str) and value:
            parts.append(value)
    body = getattr(error, "body", None)
    if isinstance(body, dict):
        parts.append(json.dumps(body, ensure_ascii=False, default=str))
    return " ".join(parts).lower()


def extract_retry_after(error: BaseException) -> float | None:
    """从异常 / 响应头里提取 ``Retry-After``（秒）；没有则返回 None"""
    raw = getattr(error, "retry_after", None)
    if isinstance(raw, (int, float)) and raw > 0:
        return float(raw)
    response = getattr(error, "response", None)
    headers = getattr(response, "headers", None)
    if headers is None:
        return None
    try:
        value = headers.get("retry-after")
    except Exception:  # noqa: BLE001 - 头部对象可能是任意实现，取不到就当没有
        return None
    if value is None:
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    return seconds if seconds > 0 else None


def _is_output_invalid(error: BaseException) -> bool:
    """这次失败是不是「模型返回的内容不符合结果模型」。

    两个来源：

    1. pydantic 校验失败（``ValidationError``）——模型返回的 JSON 与结果模型对不上；
    2. langchain 解析器自己抛的通用异常——典型是模型只回散文、没有 tool call，
       解析器对空结果做 ``result[0]`` 下标访问抛 ``IndexError``。

    第 2 类只能靠异常回溯判断（这些异常类型本身太宽泛，不能只看类型），
    因此这里沿着 traceback 找「langchain 的解析相关帧」。
    本模块刻意不 import langchain：判定只用类型名 / 模块名 / 函数名。
    """
    name = type(error).__name__
    if name in _OUTPUT_INVALID_ERROR_NAMES:
        return True
    if name not in _PARSER_SYMPTOM_ERROR_NAMES:
        return False
    frame = error.__traceback__
    while frame is not None:
        module = frame.tb_frame.f_globals.get("__name__", "")
        if (
            isinstance(module, str)
            and module.startswith("langchain")
            and ("pars" in module or "pars" in frame.tb_frame.f_code.co_name)
        ):
            return True
        frame = frame.tb_next
    return False


def classify_llm_failure(error: BaseException) -> LLMFailureKind:
    """按服务器返回的状态码 / error code / 文案，判定这次失败该「怎么恢复」。

    优先级（自上而下短路）：
    ``OUTPUT_INVALID`` → ``PERMANENT`` → ``SUSPENDED`` → ``QUOTA_EXHAUSTED``
    → ``RATE_LIMITED`` → ``TRANSIENT`` → ``REQUEST``。

    分类不确定时保守落到 ``TRANSIENT`` / ``REQUEST``（只计数与短暂冷却，绝不删配置）。
    """
    status = _extract_status_code(error)
    code = _extract_error_code(error)
    lowered_code = code.lower() if code else ""
    text = _extract_error_text(error)

    # 0) 输出不合规：只跟「模型返回的文本」有关，与 HTTP 无关，必须最先判。
    #    否则这类无状态码的失败会被兜底成 TRANSIENT，把「模型不听话」
    #    误当成「上游 5xx 抖动」来熔断。
    if _is_output_invalid(error):
        return LLMFailureKind.OUTPUT_INVALID

    # 1) 不可恢复：鉴权失败 / 模型不存在 / 模型已下线 / 权限不足。
    #    文案判定不限状态码：上游把「模型已下线」报成 400 / 404 / 410 / 503 都出现过。
    if (
        status in _PERMANENT_STATUS_CODES
        or lowered_code in _PERMANENT_CODES
        or any(keyword in text for keyword in _MODEL_MISSING_KEYWORDS)
    ):
        return LLMFailureKind.PERMANENT

    # 2) 余额 / 额度耗尽，需要充值
    if status == 402 or lowered_code in _SUSPENDED_CODES:
        return LLMFailureKind.SUSPENDED

    # 3) 限流三兄弟：先看是不是「今日额度」，再看是不是「欠费」，最后才算频率限流
    if status == 429:
        if any(keyword in text for keyword in _DAILY_KEYWORDS):
            return LLMFailureKind.QUOTA_EXHAUSTED
        if any(keyword in text for keyword in _SUSPENDED_KEYWORDS):
            return LLMFailureKind.SUSPENDED
        return LLMFailureKind.RATE_LIMITED

    # 4) 服务端 5xx：上游抖动，短暂冷却即可
    if status is not None and 500 <= status < 600:
        return LLMFailureKind.TRANSIENT

    # 5) 400 / 413 / 422：本次请求内容的问题
    if status in (400, 413, 422):
        return LLMFailureKind.REQUEST

    # 6) 无状态码（超时 / 连接错误 / 其他）一律按瞬时故障处理
    return LLMFailureKind.TRANSIENT


def _next_quota_reset_at(now: float) -> float:
    """下一次「每日额度」重置时间：本地时间次日 0 点 + 随机抖动"""
    local = datetime.fromtimestamp(now)
    next_midnight = (local + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return next_midnight.timestamp() + random.uniform(0.0, QUOTA_RESET_JITTER_SECONDS)


def describe_failure(kind: LLMFailureKind, error: BaseException) -> str:
    """把一次失败压成一行可读原因（写入 stats，供日志与 ``GET /llm/stats`` 展示）"""
    parts = [f"分类={kind.value}"]
    status = _extract_status_code(error)
    if status is not None:
        parts.append(f"HTTP {status}")
    code = _extract_error_code(error)
    if code:
        parts.append(f"code={code}")
    parts.append(f"{type(error).__name__}: {error}")
    reason = "；".join(parts)
    if len(reason) > REASON_MAX_LENGTH:
        reason = reason[:REASON_MAX_LENGTH] + "…"
    return reason


class LLMHealth(BaseModel):
    """单个槽位的健康状态机（只负责状态与迁移，不含调用统计口径）。

    状态与恢复时间都存在 :class:`~pydantic.PrivateAttr` 里，对外通过
    ``computed_field`` 暴露；每次读取都会先做一次惰性刷新，
    因此冷却到期后**自动**回到 ``HEALTHY``，不需要任何后台定时器。
    """

    _state: LLMState = PrivateAttr(default=LLMState.HEALTHY)
    _reason: str | None = PrivateAttr(default=None)
    _resume_at: float | None = PrivateAttr(default=None)
    _since: float | None = PrivateAttr(default=None)
    _escalation: int = PrivateAttr(default=0)
    _escalation_kind: LLMFailureKind | None = PrivateAttr(default=None)
    _failure_kind: LLMFailureKind | None = PrivateAttr(default=None)

    # region 内部：惰性刷新与迁移
    def _refresh(self) -> None:
        """冷却到点就回 ``HEALTHY``（惰性：由读取方触发，无需定时器）"""
        if (
            self._state in AUTO_RECOVER_STATES
            and self._resume_at is not None
            and time.time() >= self._resume_at
        ):
            self._set_state(LLMState.HEALTHY, reason=None, resume_at=None)

    def _set_state(
        self, state: LLMState, *, reason: str | None, resume_at: float | None
    ) -> None:
        self._state = state
        self._reason = reason
        self._resume_at = resume_at
        self._since = time.time()

    def _bump(self, kind: LLMFailureKind) -> int:
        """累计「同类故障连续发生次数」，供冷却时间翻倍用（换类别则重新计数）"""
        if self._escalation_kind is not kind:
            self._escalation_kind = kind
            self._escalation = 0
        self._escalation += 1
        return self._escalation

    # endregion

    # region 对外只读状态
    @computed_field(description="当前健康状态（读取时惰性刷新：冷却到期即回 healthy）")  # type: ignore[prop-decorator]
    @property
    def state(self) -> LLMState:
        self._refresh()
        return self._state

    @computed_field(description="进入当前状态的原因")  # type: ignore[prop-decorator]
    @property
    def reason(self) -> str | None:
        self._refresh()
        return self._reason

    @computed_field(description="自动恢复时间戳（unix 秒）；null 表示不会自动恢复")  # type: ignore[prop-decorator]
    @property
    def resume_at(self) -> float | None:
        self._refresh()
        return self._resume_at

    @computed_field(description="进入当前状态的时间戳（unix 秒）")  # type: ignore[prop-decorator]
    @property
    def since(self) -> float | None:
        self._refresh()
        return self._since

    @computed_field(description="同类故障的连续升级次数（用于冷却时间翻倍）")  # type: ignore[prop-decorator]
    @property
    def escalation(self) -> int:
        return self._escalation

    @computed_field(description="最近一次失败的分类")  # type: ignore[prop-decorator]
    @property
    def failure_kind(self) -> LLMFailureKind | None:
        return self._failure_kind

    @computed_field(description="是否可用：状态为 healthy")  # type: ignore[prop-decorator]
    @property
    def available(self) -> bool:
        return self.state is LLMState.HEALTHY

    @computed_field(description="是否已彻底删除（终态）")  # type: ignore[prop-decorator]
    @property
    def removed(self) -> bool:
        return self.state is LLMState.REMOVED

    # endregion

    def resume_delay(self, now: float | None = None) -> float | None:
        """距自动恢复还有多少秒；不会自动恢复时返回 ``None``。"""
        self._refresh()
        if self._resume_at is None:
            return None
        return max(0.0, self._resume_at - (now if now is not None else time.time()))

    def record_success(self) -> LLMState:
        """一次成功调用：清空故障计数并回到 ``HEALTHY``（``REMOVED`` 除外，它是终态）"""
        self._failure_kind = None
        self._escalation = 0
        self._escalation_kind = None
        if self._state is not LLMState.REMOVED:
            self._set_state(LLMState.HEALTHY, reason=None, resume_at=None)
        return self._state

    def record_failure(
        self,
        kind: LLMFailureKind,
        *,
        reason: str,
        consecutive_failures: int,
        retry_after: float | None = None,
    ) -> LLMState:
        """一次失败调用：按分类迁移状态，返回迁移后的状态。

        Args:
            kind: 本次失败的分类（见 :func:`classify_llm_failure`）。
            reason: 可读原因（见 :func:`describe_failure`），写入 stats 供排查。
            consecutive_failures: 该槽位当前的连续失败次数（由统计侧维护），
                只有 ``TRANSIENT`` / ``REQUEST`` / ``OUTPUT_INVALID`` 三类需要它来决定何时冷却。
            retry_after: 上游 ``Retry-After``（秒），冷却时间至少听它的。
        """
        self._refresh()
        self._failure_kind = kind
        # REMOVED 是终态：已经删掉的配置不再因为任何失败而改变状态
        if self._state is LLMState.REMOVED:
            return self._state

        now = time.time()

        if kind is LLMFailureKind.PERMANENT:
            # 模型下线 / 鉴权失败 / 模型不存在：重试无意义，直接删除配置（终态）
            self._set_state(LLMState.REMOVED, reason=reason, resume_at=None)
            return self._state

        if kind is LLMFailureKind.SUSPENDED:
            # 欠费 / 余额不足：冷却逐次翻倍，充值或人工热更新可立即恢复
            escalation = self._bump(kind)
            cooldown = min(
                SUSPENDED_BASE_SECONDS * (2 ** (escalation - 1)),
                SUSPENDED_MAX_SECONDS,
            )
            self._set_state(
                LLMState.SUSPENDED, reason=reason, resume_at=now + cooldown
            )
            return self._state

        if kind is LLMFailureKind.QUOTA_EXHAUSTED:
            # 今日额度：等到本地时间次日 0 点
            self._bump(kind)
            self._set_state(
                LLMState.QUOTA_WAIT, reason=reason, resume_at=_next_quota_reset_at(now)
            )
            return self._state

        if kind is LLMFailureKind.RATE_LIMITED:
            # 频率限流：休息一会儿；上游给了 Retry-After 就完全听上游的
            # （自算的退避有上限，但显式的 Retry-After 是上游的明确指令，不截断）
            escalation = self._bump(kind)
            base = min(
                COOLING_BASE_SECONDS * (2 ** (escalation - 1)), COOLING_MAX_SECONDS
            )
            cooldown = max(retry_after or 0.0, base)
            self._set_state(LLMState.COOLING, reason=reason, resume_at=now + cooldown)
            return self._state

        # TRANSIENT / REQUEST / OUTPUT_INVALID：单次失败只计数（不误伤槽位），
        # 连续达到阈值才冷却。上游给了 Retry-After 时冷却至少听它的
        # （自算的退避有上限，但显式的 Retry-After 不截断）。
        if consecutive_failures < MAX_CONSECUTIVE_FAILURES:
            return self._state
        escalation = self._bump(kind)
        base = min(
            TRANSIENT_COOLING_SECONDS * (2 ** (escalation - 1)),
            TRANSIENT_COOLING_MAX_SECONDS,
        )
        cooldown = max(retry_after or 0.0, base)
        self._set_state(LLMState.COOLING, reason=reason, resume_at=now + cooldown)
        return self._state


__all__ = [
    "AUTO_RECOVER_STATES",
    "COOLING_BASE_SECONDS",
    "COOLING_MAX_SECONDS",
    "LLMFailureKind",
    "LLMHealth",
    "LLMState",
    "MAX_CONSECUTIVE_FAILURES",
    "QUOTA_RESET_JITTER_SECONDS",
    "SUSPENDED_BASE_SECONDS",
    "SUSPENDED_MAX_SECONDS",
    "TRANSIENT_COOLING_MAX_SECONDS",
    "TRANSIENT_COOLING_SECONDS",
    "classify_llm_failure",
    "describe_failure",
    "extract_retry_after",
]
