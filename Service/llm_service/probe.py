"""云端 LLM 单实例连通性探测（管理端「测试」按钮用，一次性诊断）。

与实例池的关系：**完全独立**。

- 池里的 ``TrackedChatOpenAI`` 带限流器与统计埋点，借用它探测会污染统计口径、
  还可能因为限流器排队把「测试」拖成几十秒；
- 本模块直接 ``httpx`` 打一次最小 completion（``max_tokens=1``），
  **不写任何状态**：不参与健康状态机、不改统计、不落盘、不影响冷却 / 摘除。

密钥处理：请求里的 ``token`` 为空或含脱敏标记 ``****`` 时，按 ``index`` 回填
``settings.llm_apis`` 里的原密钥（管理页永远拿不到明文，这是唯一正确的做法）。
回填失败（索引越界 / 该实例本来就没有密钥）时不发请求，直接返回失败。
"""

import time

import httpx
from loguru import logger
from pydantic import BaseModel, Field

from CONFIG import LLMApiConfig, settings

#: 脱敏标记：``_mask_token`` 生成的串一定含它（见 pool.py）
_MASK_FLAG = "****"

#: 错误文案截断长度：上游 400/401 的响应体可能很长，避免把整段塞进接口响应
_ERROR_MAX_CHARS = 200


def _scrub(text: str, token: str) -> str:
    """把上游错误文案里的密钥替换成 ``****`` 并截断。

    部分厂商会在 401 响应体里回显收到的 key，而本接口的结果会一路透传到管理端页面，
    所以**必须在源头抹掉**（宁可少看到一点错误细节，也不能把明文密钥带回前端）。
    """
    if token:
        text = text.replace(token, _MASK_FLAG)
    return text[:_ERROR_MAX_CHARS]


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

    ok: bool = Field(description="是否探测成功（HTTP 2xx 且有响应体）")
    base_url: str = Field(description="被测接口地址")
    model_name: str = Field(description="被测模型名")
    status_code: int | None = Field(default=None, description="上游 HTTP 状态码；未发出请求时为 null")
    latency_ms: int = Field(default=0, description="耗时（毫秒）；未发出请求时为 0")
    error: str | None = Field(default=None, description="失败原因（截断，绝不含 token）")


def resolve_probe_config(req: LLMApiTestRequest) -> tuple[LLMApiConfig, str | None]:
    """把请求归一成「可直接发请求的配置」，返回 ``(配置, 失败原因)``。

    失败原因非 None 表示**不应发请求**（未提供密钥 / 索引越界），调用方据此直接返回失败。
    """
    base_url = req.base_url.strip()
    model_name = req.model_name.strip()
    token = req.token.strip()
    if not token or _MASK_FLAG in token:
        if req.index is not None and 0 <= req.index < len(settings.llm_apis):
            token = settings.llm_apis[req.index].token
        else:
            token = ""
    # requests_per_second 只用于实例池限流，探测是单次请求，取默认值即可
    cfg = LLMApiConfig(base_url=base_url, model_name=model_name, token=token)
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
    """对一个实例做一次最小 completion 探测（不重试、不写状态）。"""
    url = cfg.base_url.rstrip("/") + "/chat/completions"
    budget = float(timeout if timeout is not None else settings.llm_test_timeout)
    started = time.perf_counter()

    def _elapsed_ms() -> int:
        return int((time.perf_counter() - started) * 1000)

    try:
        async with httpx.AsyncClient(timeout=budget) as client:
            resp = await client.post(
                url,
                headers={"Authorization": f"Bearer {cfg.token}"},
                json={
                    "model": cfg.model_name,
                    "messages": [{"role": "user", "content": "ping"}],
                    "max_tokens": 1,
                    "stream": False,
                },
            )
    except httpx.TimeoutException:
        error = f"请求超时（>{budget:.0f}s），该模型可能过慢或不可达"
        logger.warning("LLM 探测超时：model={} base_url={}", cfg.model_name, cfg.base_url)
        return LLMProbeResult(
            ok=False,
            base_url=cfg.base_url,
            model_name=cfg.model_name,
            latency_ms=_elapsed_ms(),
            error=error,
        )
    except httpx.HTTPError as e:
        # 连接层错误（DNS / 拒绝连接 / TLS 等）：异常信息里不含密钥
        logger.warning(
            "LLM 探测失败：model={} base_url={} 原因={!r}",
            cfg.model_name,
            cfg.base_url,
            e,
        )
        return LLMProbeResult(
            ok=False,
            base_url=cfg.base_url,
            model_name=cfg.model_name,
            latency_ms=_elapsed_ms(),
            error=_scrub(f"请求失败：{e}", cfg.token),
        )

    latency_ms = _elapsed_ms()
    if resp.status_code >= 400:
        detail = _scrub((resp.text or "").strip().replace("\n", " "), cfg.token)
        logger.warning(
            "LLM 探测返回错误：model={} status={} detail={}",
            cfg.model_name,
            resp.status_code,
            detail,
        )
        return LLMProbeResult(
            ok=False,
            base_url=cfg.base_url,
            model_name=cfg.model_name,
            status_code=resp.status_code,
            latency_ms=latency_ms,
            error=f"HTTP {resp.status_code}：{detail}",
        )

    logger.info(
        "LLM 探测成功：model={} base_url={} 耗时={}ms",
        cfg.model_name,
        cfg.base_url,
        latency_ms,
    )
    return LLMProbeResult(
        ok=True,
        base_url=cfg.base_url,
        model_name=cfg.model_name,
        status_code=resp.status_code,
        latency_ms=latency_ms,
    )


__all__ = [
    "LLMApiTestRequest",
    "LLMProbeResult",
    "probe_llm_api",
    "resolve_probe_config",
]
