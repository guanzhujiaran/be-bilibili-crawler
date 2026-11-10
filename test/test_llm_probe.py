"""云端 LLM 单实例连通性探测（`Service/llm_service/probe.py`）的单测

纯本地、不联网：用假 ``ChatOpenAI`` 覆盖 SDK，验证
1. 密钥回填（脱敏 / 空串按 index 取原密钥，索引越界不发请求）；
2. 成功（真实提问 `1+1=?` → ok=True，裸类参数 / 不重试 / 状态码 200）；
3. SDK 错误（401 摘要不含 token）、超时、连接错误、其它异常；
4. 探测不写任何状态（不改 `settings.llm_apis`、不动统计）。

运行:
    uv run python -m pytest test/test_llm_probe.py -v
"""

import sys
from pathlib import Path

import httpx
import pytest
from langchain_core.messages import AIMessage
from openai import APIConnectionError, APIStatusError, APITimeoutError

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from CONFIG import LLMApiConfig, settings
from Service.llm_service import pool
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
    assert "索引越界" in invalid
    assert "密钥" in invalid


def test_index_mismatch_does_not_borrow_other_key(with_configs):
    """行序错位时不能把别的实例的密钥拿来测试（会串密钥）。"""
    cfg, invalid = resolve_probe_config(
        LLMApiTestRequest(
            index=0,
            base_url="https://another.example.com/v1",
            model_name="totally-other-model",
            token="abcd****wxyz",
        )
    )
    assert invalid is not None
    assert "索引与配置不匹配" in invalid
    assert cfg.token == ""


def test_masked_original_token_is_not_sent_upstream(monkeypatch):
    """历史脏配置里的脱敏 token 不能被当真密钥拿去发请求。"""
    monkeypatch.setattr(
        settings,
        "llm_apis",
        [_cfg(model_name="first", token="nvap****wxyz")],
    )
    cfg, invalid = resolve_probe_config(
        LLMApiTestRequest(
            index=0,
            base_url="https://api.example.com/v1",
            model_name="first",
            token="nvap****wxyz",
        )
    )
    assert invalid is not None
    assert "脱敏值" in invalid
    assert cfg.token == ""


def test_full_config_view_after_removal_keeps_index(monkeypatch):
    """REMOVED 不再删条目：index 仍然指向原来的那一行。"""
    configs = [_cfg(model_name="first"), _cfg(model_name="second")]
    monkeypatch.setattr(settings, "llm_apis", list(configs))
    monkeypatch.setattr(pool, "_free_llm_cache", [])
    monkeypatch.setattr(pool, "_free_llm_cache_key", "")

    fingerprint = pool._fingerprint_of(configs[0])
    assert pool.remove_llm_by_fingerprint(fingerprint) is True
    assert len(settings.llm_apis) == 2  # 列表没被缩短，index 不漂移

    cfg, invalid = resolve_probe_config(
        LLMApiTestRequest(
            index=1,
            base_url="https://api.example.com/v1",
            model_name="second",
            token="nvap****wxyz",
        )
    )
    assert invalid is None
    assert cfg.token == _TOKEN


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
# 2~4. 探测本身（假 ChatOpenAI，不联网）
# ================================================================


def _request() -> httpx.Request:
    return httpx.Request("POST", "https://api.example.com/v1/chat/completions")


def _status_error(status_code: int, message: str) -> APIStatusError:
    return APIStatusError(
        message, response=httpx.Response(status_code, request=_request()), body=None
    )


class _FakeChatOpenAI:
    """记录构造参数与提问内容，并按预设返回 AIMessage 或抛 SDK 异常。"""

    def __init__(
        self, *, reply: AIMessage | None = None, exc: Exception | None = None
    ) -> None:
        self.reply = reply if reply is not None else AIMessage(content="2")
        self.exc = exc
        self.prompts: list[str] = []
        self.model: str = ""
        self.base_url: str = ""
        self.api_key_plain: str = ""
        self.timeout: float = 0.0
        self.max_retries: int = -1

    async def ainvoke(self, prompt: str, **_kwargs) -> AIMessage:
        self.prompts.append(prompt)
        if self.exc is not None:
            raise self.exc
        return self.reply


def _install_llm(
    monkeypatch, *, reply: AIMessage | None = None, exc: Exception | None = None
) -> _FakeChatOpenAI:
    fake = _FakeChatOpenAI(reply=reply, exc=exc)

    def _factory(**kwargs):
        fake.model = kwargs.get("model", "")
        fake.base_url = kwargs.get("base_url", "")
        api_key = kwargs.get("api_key")
        fake.api_key_plain = api_key.get_secret_value() if api_key is not None else ""
        fake.timeout = kwargs.get("timeout", 0.0)
        fake.max_retries = kwargs.get("max_retries", -1)
        return fake

    monkeypatch.setattr(probe_mod, "ChatOpenAI", _factory)
    return fake


@pytest.mark.asyncio
async def test_probe_success(monkeypatch):
    fake = _install_llm(monkeypatch)

    result = await probe_llm_api(_cfg(), timeout=10)

    assert result.ok is True
    assert result.status_code == 200
    assert result.latency_ms >= 0
    assert result.error is None
    # 真的用裸 ChatOpenAI 问了一句 1+1=?，且「不重试 + 超时受控 + 密钥正确」
    assert fake.prompts == ["1+1=?"]
    assert fake.model == "example-model"
    assert fake.base_url == "https://api.example.com/v1"
    assert fake.api_key_plain == _TOKEN
    assert fake.timeout == 10
    assert fake.max_retries == 0


@pytest.mark.asyncio
async def test_probe_empty_reply_still_ok(monkeypatch):
    """部分推理模型把内容放 reasoning、content 为空：SDK 调用通了就算通。"""
    _install_llm(monkeypatch, reply=AIMessage(content=""))

    assert (await probe_llm_api(_cfg(), timeout=10)).ok is True


@pytest.mark.asyncio
async def test_probe_sdk_http_error(monkeypatch):
    _install_llm(monkeypatch, exc=_status_error(401, f"invalid api key: {_TOKEN}"))

    result = await probe_llm_api(_cfg(), timeout=10)

    assert result.ok is False
    assert result.status_code == 401
    assert "HTTP 401" in (result.error or "")
    # 上游回显密钥时必须被抹掉，不能透传到管理端
    assert _TOKEN not in (result.error or "")


@pytest.mark.asyncio
async def test_probe_timeout(monkeypatch):
    _install_llm(monkeypatch, exc=APITimeoutError(request=_request()))

    result = await probe_llm_api(_cfg(), timeout=5)

    assert result.ok is False
    assert result.status_code is None
    assert "超时" in (result.error or "")


@pytest.mark.asyncio
async def test_probe_connection_error(monkeypatch):
    _install_llm(
        monkeypatch,
        exc=APIConnectionError(message="connection refused", request=_request()),
    )

    result = await probe_llm_api(_cfg(), timeout=5)

    assert result.ok is False
    assert "请求失败" in (result.error or "")


@pytest.mark.asyncio
async def test_probe_unexpected_error_is_reported(monkeypatch):
    """SDK 之外的异常也要转成诊断结果，不能让管理端点「测试」直接 500。"""
    _install_llm(monkeypatch, exc=RuntimeError(f"bad response {_TOKEN}"))

    result = await probe_llm_api(_cfg(), timeout=5)

    assert result.ok is False
    assert _TOKEN not in (result.error or "")


@pytest.mark.asyncio
async def test_probe_does_not_touch_settings(with_configs, monkeypatch):
    _install_llm(monkeypatch)
    before = [(c.model_name, c.token) for c in settings.llm_apis]

    await probe_llm_api(_cfg(model_name="first"), timeout=10)

    assert [(c.model_name, c.token) for c in settings.llm_apis] == before
