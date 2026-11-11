"""云端 LLM 上游模型列表拉取（管理端「获取模型列表」按钮用，一次性查询）。

与实例池 / 探测的关系：**完全独立**。只拿 ``base_url + token`` 打一次 OpenAI 兼容的
``GET {base_url}/models``，把可用的模型 id 列表回给管理页做下拉候选；
不参与健康状态机、不改统计、不落盘、不影响冷却 / 摘除。

密钥处理与 :mod:`Service.llm_service.probe` 同一口径：请求里的 ``token`` 为空或含脱敏
标记 ``****`` 时按 ``index`` 回填 ``settings.llm_apis`` 里的原密钥（管理页拿不到明文）。
因此管理页可以在「地址 / 密钥都填好但模型名还没选」的状态下直接拉列表。

响应解析兼容两种常见形状：OpenAI 的 ``{"data": [{"id": ...}]}`` 与
Ollama 的 ``{"models": [{"name": ...}]}``，也接受裸数组 ``["model-a", ...]``。
"""

import time

import httpx
from loguru import logger
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from CONFIG import LLMApiConfig, settings
from Service.llm_service.probe import resolve_api_token

#: 脱敏标记：``_mask_token`` 生成的串一定含它（见 pool.py）
_MASK_FLAG = "****"

#: 错误文案截断长度：上游 4xx 的响应体常是完整 JSON（含 message / faq 链接），
#: 200 字符会把关键那句切掉；放到 2000 既能看全一条 JSON 错误，
#: 也不至于把整页 HTML 塞进接口响应与日志。
_ERROR_MAX_CHARS = 2000


def _scrub(text: str, token: str) -> str:
    """把上游错误文案里的密钥替换成 ``****`` 并截断（结果会透传到管理端页面）。"""
    if token:
        text = text.replace(token, _MASK_FLAG)
    return text.strip()[:_ERROR_MAX_CHARS]


class LLMApiModelsRequest(BaseModel):
    """管理端「获取模型列表」请求：一条实例的地址 / 密钥 + 可选索引。"""

    index: int | None = Field(
        default=None,
        description="该实例在 crawler llm_apis 中的索引；token 为空/脱敏时用它回填原密钥，新增行传 null",
    )
    base_url: str = Field(default="", description="OpenAI 兼容接口地址")
    model_name: str = Field(
        default="",
        description="该实例当前的模型名；仅用于「索引与配置是否匹配」的校验，可为空",
    )
    token: str = Field(
        default="", description="明文密钥；空串或含 **** 时按 index 回填原密钥"
    )


class LLMApiModelsResult(BaseModel):
    """一次模型列表查询的结果（诊断口径，不含 token）。"""

    ok: bool = Field(description="是否查询成功（上游 /models 返回了可解析的列表）")
    base_url: str = Field(description="被查询的接口地址")
    status_code: int | None = Field(
        default=None, description="上游 HTTP 状态码；未发出请求时为 null"
    )
    latency_ms: int = Field(default=0, description="耗时（毫秒）；未发出请求时为 0")
    models: list[str] = Field(
        default_factory=list, description="上游可用的模型 id 列表（去重 + 升序）；失败时为空"
    )
    error: str | None = Field(default=None, description="失败或提示文案（截断，绝不含 token）")


class _ModelEntry(BaseModel):
    """上游 ``/models`` 里的一条模型条目（OpenAI 用 ``id``，Ollama 用 ``name``）。"""

    model_config = ConfigDict(extra="ignore")

    id: str = Field(default="", description="OpenAI 风格模型 id")
    name: str = Field(default="", description="Ollama 风格模型名")


class _ModelsEnvelope(BaseModel):
    """``{"data": [...]}`` / ``{"models": [...]}`` 两种外层字段的兼容解析。"""

    model_config = ConfigDict(extra="ignore")

    data: list[_ModelEntry] = Field(default_factory=list)
    models: list[_ModelEntry] = Field(default_factory=list)


#: 裸数组形态（``["model-a", {"id": "model-b"}]``）
_ARRAY_ADAPTER: TypeAdapter[list[_ModelEntry | str]] = TypeAdapter(
    list[_ModelEntry | str]
)


def _entry_id(entry: _ModelEntry | str) -> str:
    """取一条模型条目的 id（``id`` 优先，其次 ``name``；字符串直接用）。"""
    if isinstance(entry, str):
        return entry.strip()
    return (entry.id or entry.name).strip()


def _extract_model_ids(payload: object) -> list[str]:
    """从上游响应里抽出模型 id 列表（去重 + 升序），无法解析时抛 ``ValueError``。"""
    entries: list[_ModelEntry | str]
    if isinstance(payload, list):
        entries = _ARRAY_ADAPTER.validate_python(payload)
    else:
        envelope = _ModelsEnvelope.model_validate(payload)
        entries = [*envelope.data, *envelope.models]
    ids: set[str] = set()
    for entry in entries:
        model = _entry_id(entry)
        if model:
            ids.add(model)
    return sorted(ids)


async def list_llm_models(
    cfg: LLMApiConfig, *, timeout: float | None = None
) -> LLMApiModelsResult:
    """打一次上游 ``GET {base_url}/models``，返回可用模型列表。

    任何失败（不可达 / 超时 / 非 2xx / 响应不可解析）都归一成 ``ok=false`` 的诊断结果，
    不向接口抛异常 —— 管理页要的是一条能就地展示的结论，而不是 500。
    """
    budget = float(timeout if timeout is not None else settings.llm_models_timeout)
    started = time.perf_counter()

    def _elapsed_ms() -> int:
        return int((time.perf_counter() - started) * 1000)

    def _fail(status_code: int | None, error: str) -> LLMApiModelsResult:
        logger.warning(
            "LLM 模型列表查询失败：base_url={} status={} 原因={}",
            cfg.base_url,
            status_code,
            error,
        )
        return LLMApiModelsResult(
            ok=False,
            base_url=cfg.base_url,
            status_code=status_code,
            latency_ms=_elapsed_ms(),
            error=error,
        )

    url = f"{cfg.base_url.rstrip('/')}/models"
    headers = {"Authorization": f"Bearer {cfg.token}"} if cfg.token else {}
    try:
        async with httpx.AsyncClient(timeout=budget) as client:
            resp = await client.get(url, headers=headers)
    except httpx.TimeoutException:
        return _fail(None, f"请求超时（>{budget:.0f}s），该地址可能过慢或不可达")
    except httpx.HTTPError as exc:
        detail = _scrub(str(exc) or type(exc).__name__, cfg.token)
        return _fail(None, f"请求失败：{detail}")

    if resp.status_code >= 400:
        detail = _scrub(resp.text or "", cfg.token)
        return _fail(resp.status_code, f"HTTP {resp.status_code}：{detail or '上游未返回内容'}")

    try:
        payload = resp.json()
    except ValueError:
        return _fail(resp.status_code, "响应不是合法 JSON")

    try:
        models = _extract_model_ids(payload)
    except ValueError:
        return _fail(resp.status_code, "响应格式无法解析出模型列表（期望 data/models 数组）")

    logger.info(
        "LLM 模型列表查询成功：base_url={} 共 {} 个模型 耗时={}ms",
        cfg.base_url,
        len(models),
        _elapsed_ms(),
    )
    return LLMApiModelsResult(
        ok=True,
        base_url=cfg.base_url,
        status_code=resp.status_code,
        latency_ms=_elapsed_ms(),
        models=models,
        error=None if models else "上游返回了空列表（该地址/密钥下没有可用模型）",
    )


def resolve_models_config(req: LLMApiModelsRequest) -> tuple[LLMApiConfig, str | None]:
    """把请求归一成「可直接发请求的配置」，返回 ``(配置, 失败原因)``。

    与探测不同，这里**不要求模型名**（拉列表就是为了选模型）。
    """
    base_url = req.base_url.strip()
    token, resolve_error = resolve_api_token(
        index=req.index,
        base_url=base_url,
        model_name=req.model_name,
        token=req.token,
    )
    cfg = LLMApiConfig(base_url=base_url, model_name=req.model_name.strip(), token=token)
    if resolve_error is not None:
        return cfg, resolve_error
    if not base_url.startswith(("http://", "https://")):
        return cfg, "接口地址必须以 http:// 或 https:// 开头"
    if not token:
        return cfg, "该实例没有可用密钥（请先填写密钥再获取模型列表）"
    return cfg, None


__all__ = [
    "LLMApiModelsRequest",
    "LLMApiModelsResult",
    "list_llm_models",
    "resolve_models_config",
]
