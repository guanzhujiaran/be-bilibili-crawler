"""云端 LLM 单实例连通性探测（`Service/llm_service/probe.py`）的单测

纯本地、不联网：用假 httpx 客户端覆盖
1. 密钥回填（脱敏 / 空串按 index 取原密钥，索引越界不发请求）；
2. 成功（2xx → ok=True，带状态码与耗时，请求体是最小 completion）；
3. 非 2xx（错误摘要不含 token）；
4. 超时 / 连接错误；
5. 探测不写任何状态（不改 `settings.llm_apis`、不动统计）。

运行:
    uv run python -m pytest test/test_llm_probe.py -v
"""

import sys
from pathlib import Path

import httpx
import pytest

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from CONFIG import LLMApiConfig, settings
from Service.llm_service import probe as probe_mod
from Service.llm_service.probe import (
    LLMApiTestRequest,
    probe_llm_api,
    resolve_probe_config,
)

_TOKEN = "nvapi-SECRET-token"


def _cfg(**kwargs) -> LLMApiConfig:
    defaults = {
        "base_url": "https://api.example.com/v1",
        "model_name": "example-model",
        "token": _TOKEN,
    }
    return LLMApiConfig(**{**defaults, **kwargs})


@pytest.fixture()
def with_configs(monkeypatch):
    """在 settings.llm_apis 里放两条配置，测试结束后还原。"""
    original = list(settings.llm_apis)
    settings.llm_apis = [_cfg(model_name="first"), _cfg(model_name="second")]
    yield settings.llm_apis
    settings.llm_apis = original


# ================================================================
# 1. 密钥回填
# ================================================================


def test_masked_token_filled_from_index(with_configs):
    cfg, invalid = resolve_probe_config(
        LLMApiTestRequest(
            index=1,
            base_url="https://api.example.com/v1",
            model_name="second",
            token="nvap****uVY9",
        )
    )
    assert invalid is None
    assert cfg.token == _TOKEN  # 用的是第 1 条的原密钥


def test_empty_token_filled_from_index(with_configs):
    cfg, invalid = resolve_probe_config(
        LLMApiTestRequest(
            index=0, base_url="https://api.example.com/v1", model_name="first"
        )
    )
    assert invalid is None
    assert cfg.token == _TOKEN


def test_plain_token_is_used_as_is(with_configs):
    cfg, invalid = resolve_probe_config(
        LLMApiTestRequest(
            index=0,
            base_url="https://api.example.com/v1",
            model_name="first",
            token="sk-new-plain",
        )
    )
    assert invalid is None
    assert cfg.token == "sk-new-plain"


def test_new_row_without_token_is_rejected(with_configs):
    cfg, invalid = resolve_probe_config(
        LLMApiTestRequest(base_url="https://api.example.com/v1", model_name="new")
    )
    assert invalid is not None
    assert "密钥" in invalid
    assert cfg.token == ""


def test_index_out_of_range_is_rejected(with_configs):
    _, invalid = resolve_probe_config(
        LLMApiTestRequest(
            index=99, base_url="https://api.example.com/v1", model_name="ghost"
        )
    )
    assert invalid is not None
    assert "密钥" in invalid


@pytest.mark.parametrize(
    ("base_url", "model_name", "expected"),
    [
        ("api.example.com", "m", "http:// 或 https://"),
        ("https://api.example.com/v1", "  ", "缺少模型名"),
    ],
)
def test_bad_fields_rejected(with_configs, base_url, model_name, expected):
    cfg, invalid = resolve_probe_config(
        LLMApiTestRequest(index=0, base_url=base_url, model_name=model_name, token=_TOKEN)
    )
    assert invalid is not None
    assert expected in invalid


# ================================================================
# 2~4. 探测本身
# ================================================================


class _FakeResponse:
    def __init__(self, status_code: int, text: str = "") -> None:
        self.status_code = status_code
        self.text = text


class _FakeAsyncClient:
    """记录请求参数，并按预设返回 / 抛错。"""

    def __init__(self, *, response=None, exc=None, **_kwargs) -> None:
        self._response = response
        self._exc = exc
        self.calls: list[dict] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args) -> bool:
        return False

    async def post(self, url, *, headers=None, json=None):
        self.calls.append({"url": url, "headers": headers or {}, "json": json or {}})
        if self._exc is not None:
            raise self._exc
        return self._response


def _install_client(monkeypatch, **kwargs) -> _FakeAsyncClient:
    fake = _FakeAsyncClient(**kwargs)
    monkeypatch.setattr(probe_mod.httpx, "AsyncClient", lambda **_kw: fake)
    return fake


@pytest.mark.asyncio
async def test_probe_success(monkeypatch):
    fake = _install_client(monkeypatch, response=_FakeResponse(200, "{}"))

    result = await probe_llm_api(_cfg(), timeout=10)

    assert result.ok is True
    assert result.status_code == 200
    assert result.latency_ms >= 0
    assert result.error is None
    # 请求是最小 completion：1 个 token、非流式、带 Bearer
    call = fake.calls[0]
    assert call["url"] == "https://api.example.com/v1/chat/completions"
    assert call["json"]["max_tokens"] == 1
    assert call["json"]["stream"] is False
    assert call["headers"]["Authorization"] == f"Bearer {_TOKEN}"


@pytest.mark.asyncio
async def test_probe_http_error(monkeypatch):
    _install_client(
        monkeypatch, response=_FakeResponse(401, "invalid api key: nvapi-SECRET-token")
    )

    result = await probe_llm_api(_cfg(), timeout=10)

    assert result.ok is False
    assert result.status_code == 401
    assert "HTTP 401" in (result.error or "")
    # 上游回显密钥时必须被抹掉，不能透传到管理端
    assert _TOKEN not in (result.error or "")


@pytest.mark.asyncio
async def test_probe_timeout(monkeypatch):
    _install_client(monkeypatch, exc=httpx.ReadTimeout("too slow"))

    result = await probe_llm_api(_cfg(), timeout=5)

    assert result.ok is False
    assert result.status_code is None
    assert "超时" in (result.error or "")


@pytest.mark.asyncio
async def test_probe_connection_error(monkeypatch):
    _install_client(monkeypatch, exc=httpx.ConnectError("connection refused"))

    result = await probe_llm_api(_cfg(), timeout=5)

    assert result.ok is False
    assert "请求失败" in (result.error or "")


@pytest.mark.asyncio
async def test_probe_does_not_touch_settings(with_configs, monkeypatch):
    _install_client(monkeypatch, response=_FakeResponse(200, "{}"))
    before = [(c.model_name, c.token) for c in settings.llm_apis]

    await probe_llm_api(_cfg(model_name="first"), timeout=10)

    assert [(c.model_name, c.token) for c in settings.llm_apis] == before
