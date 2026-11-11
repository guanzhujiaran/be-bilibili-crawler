"""云端 LLM 单实例连通性探测（管理端「测试」按钮用，一次性诊断）。

与实例池的关系：**完全独立**。

- 池里的 ``TrackedChatOpenAI`` 带限流器与统计埋点，借用它探测会污染统计口径、
  还可能因为限流器排队把「测试」拖成几十秒；
- 本模块用**裸的** ``ChatOpenAI``（与生产同一套 SDK、同一条请求链路）真实问一句
  ``1+1=?``，拿到模型回复才算通。相比 ``httpx`` 裸打 ``/chat/completions`` 的最小
  completion，这样连 SDK 版本兼容性、``base_url`` 拼接、鉴权头、响应解析都会被一起测到；
- **不写任何状态**：不参与健康状态机、不改统计、不落盘、不影响冷却 / 摘除。

密钥处理：请求里的 ``token`` 为空或含脱敏标记 ``****`` 时，按 ``index`` 回填
``settings.llm_apis`` 里的原密钥（管理页永远拿不到明文，这是唯一正确的做法）。
``settings.llm_apis`` 是**完整人工配置列表**：状态机 REMOVED 只标记实例、
不再删条目，因此 index 与 ``GET /llm/config`` 的列表始终对齐。
回填失败（索引越界 / 索引与配置不匹配 / 原密钥本身是脱敏值 / 该实例本来就没有密钥）
时不发请求，直接返回失败，避免拿错密钥或把脱敏串发给上游。
"""

import time

import httpx
from langchain_openai import ChatOpenAI
from loguru import logger
from openai import APIStatusError, APITimeoutError
from pydantic import BaseModel, Field, SecretStr

from CONFIG import LLMApiConfig, settings

#: 脱敏标记：``_mask_token`` 生成的串一定含它（见 pool.py）
_MASK_FLAG = "****"

#: 错误文案截断长度：上游 400/401 的响应体可能很长，避免把整段塞进接口响应
_ERROR_MAX_CHARS = 200

#: 探测提问：足够简单，只用来确认「SDK 真的把请求发出去并拿到了回复」
_PROBE_PROMPT = "1+1=?"

#: 模型回复只进日志（不进响应，避免变更对外 schema），日志里截断到这个长度
_ANSWER_LOG_MAX_CHARS = 100


def _scrub(text: str, token: str) -> str:
    """把上游错误文案里的密钥替换成 ``****`` 并截断。

    部分厂商会在 401 响应体里回显收到的 key，而本接口的结果会一路透传到管理端页面，
    所以**必须在源头抹掉**（宁可少看到一点错误细节，也不能把明文密钥带回前端）。
    """
    if token:
        text = text.replace(token, _MASK_FLAG)
    return text[:_ERROR_MAX_CHARS]


def _failure_detail(exc: Exception, *, token: str, budget: float) -> tuple[int | None, str]:
    """把 SDK 抛出的异常归一成 ``(HTTP 状态码, 可读文案)``（文案绝不含密钥）。

    ``ChatOpenAI`` 成功时不暴露底层 HTTP 状态码，所以只有失败侧能取到
    ``APIStatusError.status_code``；超时单独给一句可执行的提示，别让运维
    把「模型慢」误判成「配置错」。
    """
    status_code = exc.status_code if isinstance(exc, APIStatusError) else None
    if (
        isinstance(exc, (APITimeoutError, httpx.TimeoutException))
        or "timeout" in type(exc).__name__.lower()
    ):
        return status_code, f"请求超时（>{budget:.0f}s），该模型可能过慢或不可达"
    detail = _scrub(str(exc) or type(exc).__name__, token)
    if status_code is not None:
        return status_code, f"HTTP {status_code}：{detail}"
    return status_code, f"请求失败：{detail}"


def _reply_text(reply: object) -> str:
    """把 ``AIMessage.content``（str 或分片列表）压成一行短文本，仅供日志。"""
    content = getattr(reply, "content", "")
    text = content if isinstance(content, str) else str(content)
    return " ".join(text.split())[:_ANSWER_LOG_MAX_CHARS]


class LLMApiTestRequest(BaseModel):
    """管理端「测试」请求：一条实例的可编辑字段 + 可选的索引。"""

    index: int | None = Field(
        default=None,
        description="该实例在 crawler llm_apis 中的索引；token 为空/脱敏时用它回填原密钥，新增行传 null",
    )
    base_url: str = Field(default="", description="OpenAI 兼容接口地址")
    model_name: str = Field(default="", description="模型名")
    token: str = Field(
        default="", description="明文密钥；空串或含 **** 时按 index 回填原密钥"
    )


class LLMProbeResult(BaseModel):
    """一次探测的结果（诊断用，不含 token）。"""

    ok: bool = Field(description="是否探测成功（ChatOpenAI 真实调用拿到模型回复）")
    base_url: str = Field(description="被测接口地址")
    model_name: str = Field(description="被测模型名")
    status_code: int | None = Field(
        default=None,
        description="上游 HTTP 状态码；SDK 成功调用拿不到状态码时记为 200，未发出请求时为 null",
    )
    latency_ms: int = Field(default=0, description="耗时（毫秒）；未发出请求时为 0")
    error: str | None = Field(default=None, description="失败原因（截断，绝不含 token）")


def resolve_api_token(
    *, index: int | None, base_url: str, model_name: str, token: str
) -> tuple[str, str | None]:
    """把请求里的 ``token`` 归一成可用于发请求的明文，返回 ``(token, 失败原因)``。

    失败原因非 None 表示**不应发请求**（索引越界 / 索引与配置不匹配 / 原密钥是脱敏值），
    调用方据此直接返回失败。``token`` 传明文时原样返回，不做任何校验。

    管理页永远拿不到明文，所以「留空 / 回传脱敏值」都必须按 ``index`` 回填
    ``settings.llm_apis`` 里的原密钥（探测与模型列表共用这一口径）。
    ``settings.llm_apis`` 是完整人工配置列表：状态机 REMOVED 只标记实例、不删条目，
    因此 index 与 ``GET /llm/config`` 的列表始终对齐。
    """
    token = token.strip()
    if token and _MASK_FLAG not in token:
        return token, None
    if index is None:
        return "", None
    if not 0 <= index < len(settings.llm_apis):
        return (
            "",
            f"索引越界：当前共 {len(settings.llm_apis)} 条配置，"
            f"index={index}（新实例请填写明文密钥）",
        )
    current = settings.llm_apis[index]
    # 允许「改了模型名 / 改了地址」后仍沿用本行密钥；两个字段都对不上
    # 说明前端行序与服务端列表已经错位，此时按 index 取密钥会串到别的实例。
    matched = current.base_url == base_url.strip() or current.model_name == model_name.strip()
    if not matched:
        return "", "索引与配置不匹配，请刷新页面或重新填写明文密钥"
    if _MASK_FLAG in current.token:
        return "", "该实例保存的密钥是脱敏值，请重新填写明文密钥"
    return current.token, None


def resolve_probe_config(req: LLMApiTestRequest) -> tuple[LLMApiConfig, str | None]:
    """把请求归一成「可直接发请求的配置」，返回 ``(配置, 失败原因)``。

    失败原因非 None 表示**不应发请求**（未提供密钥 / 索引越界 / 索引与配置不匹配 /
    原密钥是脱敏值），调用方据此直接返回失败。
    """
    base_url = req.base_url.strip()
    model_name = req.model_name.strip()
    token, resolve_error = resolve_api_token(
        index=req.index,
        base_url=base_url,
        model_name=model_name,
        token=req.token,
    )
    # requests_per_second 只用于实例池限流，探测是单次请求，取默认值即可
    cfg = LLMApiConfig(base_url=base_url, model_name=model_name, token=token)
    if resolve_error is not None:
        return cfg, resolve_error
    if not base_url.startswith(("http://", "https://")):
        return cfg, "接口地址必须以 http:// 或 https:// 开头"
    if not model_name:
        return cfg, "缺少模型名"
    if not token:
        return cfg, "该实例没有可用密钥（新实例请先填写密钥再测试）"
    return cfg, None


async def probe_llm_api(
    cfg: LLMApiConfig, *, timeout: float | None = None
) -> LLMProbeResult:
    """用裸 ``ChatOpenAI`` 真发一次 ``1+1=?``，拿到模型回复即认为该实例可用。

    ``max_retries=0`` 保证「一次探测 = 一次上游请求」；只用裸类、不碰实例池，
    因此不改统计、不参与健康状态机、不落盘。任何 SDK 异常都归一成
    ``ok=false`` 的诊断结果，不向接口抛异常。
    """
    budget = float(timeout if timeout is not None else settings.llm_test_timeout)
    started = time.perf_counter()

    def _elapsed_ms() -> int:
        return int((time.perf_counter() - started) * 1000)

    try:
        llm = ChatOpenAI(
            model=cfg.model_name,
            base_url=cfg.base_url,
            api_key=SecretStr(cfg.token),
            timeout=budget,
            max_retries=0,
        )
        reply = await llm.ainvoke(_PROBE_PROMPT)
    except Exception as exc:  # noqa: BLE001 - 上游 SDK 异常种类多，诊断接口统一转换
        status_code, error = _failure_detail(exc, token=cfg.token, budget=budget)
        logger.warning(
            "LLM 探测失败：model={} base_url={} status={} 异常={} 原因={}",
            cfg.model_name,
            cfg.base_url,
            status_code,
            type(exc).__name__,
            error,
        )
        return LLMProbeResult(
            ok=False,
            base_url=cfg.base_url,
            model_name=cfg.model_name,
            status_code=status_code,
            latency_ms=_elapsed_ms(),
            error=error,
        )

    latency_ms = _elapsed_ms()
    logger.info(
        "LLM 探测成功：model={} base_url={} 耗时={}ms 回答={}",
        cfg.model_name,
        cfg.base_url,
        latency_ms,
        _reply_text(reply) or "（空回复）",
    )
    return LLMProbeResult(
        ok=True,
        base_url=cfg.base_url,
        model_name=cfg.model_name,
        status_code=200,
        latency_ms=latency_ms,
    )


__all__ = [
    "LLMApiTestRequest",
    "LLMProbeResult",
    "probe_llm_api",
    "resolve_probe_config",
]
