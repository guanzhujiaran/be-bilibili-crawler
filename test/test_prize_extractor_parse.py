"""LLM 输出「解析前置清洗」回归用例。

背景：thinking 模型返回 ``<think>…</think> ```json {…} ``` 时，
``with_structured_output`` 的原始解析会抛 JSONDecodeError（不是 ValidationError），
旧的后置修复救不回来，一条消息会串行试完全部 LLM 再退避重试。
"""

from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from Models.MQ.PrizeExtractResult import (
    OfficialPrizeExtractResult,
    PrizeExtractResult,
)
from Service.GetOthersLotDyn.parser import prize_extractor as pe
from Service.GetOthersLotDyn.parser.prize_extractor import (
    _LogThrottle,
    _parse_llm_result_text,
    _raw_text_candidates,
    _repair_structured_output,
    _strip_llm_wrappers,
)
from Service.llm_service import AllLLMsCoolingError


def test_strip_think_block_and_json_fence() -> None:
    raw = (
        "<think>用户要抽奖，我先想想……\n这里可能有 { 干扰括号 }</think>\n"
        '```json\n{"is_lot": true, "prize_names": ["耳机"], "lottery_time": "2026-11-20"}\n```'
    )
    cleaned = _strip_llm_wrappers(raw)
    assert "<think" not in cleaned
    assert "```" not in cleaned
    assert cleaned.startswith("{") and cleaned.endswith("}")

    result = _parse_llm_result_text(raw, PrizeExtractResult)
    assert isinstance(result, PrizeExtractResult)
    assert result.is_lot is True
    assert result.prize_names == ["耳机"]
    assert result.lottery_time == "2026-11-20"


def test_strip_truncated_think_block() -> None:
    raw = (
        "嗯，前面是模型思考过程 {不是 JSON} </think>\n"
        "```json\n{\"is_grand_prize\": true}\n```"
    )
    cleaned = _strip_llm_wrappers(raw)
    assert "嗯，前面" not in cleaned
    result = _parse_llm_result_text(raw, OfficialPrizeExtractResult)
    assert isinstance(result, OfficialPrizeExtractResult)
    assert result.is_grand_prize is True


def test_plain_json_without_wrappers() -> None:
    result = _parse_llm_result_text(
        '{"is_lot": false, "need_repost": false}', PrizeExtractResult
    )
    assert isinstance(result, PrizeExtractResult)
    assert result.is_lot is False


def test_embedded_json_object_in_prose() -> None:
    raw = '模型回答如下：{"is_grand_prize": false} 以上。'
    result = _parse_llm_result_text(raw, OfficialPrizeExtractResult)
    assert isinstance(result, OfficialPrizeExtractResult)
    assert result.is_grand_prize is False


def test_bare_bool_for_single_bool_model() -> None:
    result = _parse_llm_result_text("**false**", OfficialPrizeExtractResult)
    assert isinstance(result, OfficialPrizeExtractResult)
    assert result.is_grand_prize is False
    # 多字段模型不允许把裸布尔猜成某个字段
    assert _parse_llm_result_text("true", PrizeExtractResult) is None


def test_unparseable_text_returns_none() -> None:
    assert _parse_llm_result_text("抱歉，我无法完成该请求", PrizeExtractResult) is None
    assert _parse_llm_result_text("", PrizeExtractResult) is None


def test_raw_text_candidates_from_message_content_and_tool_calls() -> None:
    raw_msg = SimpleNamespace(
        content="```json\n{\"is_lot\": true}\n```",
        additional_kwargs={
            "tool_calls": [
                {
                    "function": {
                        "arguments": "<think>xxx</think>```json{\"is_lot\": true}```"
                    }
                }
            ]
        },
    )
    candidates = _raw_text_candidates(raw_msg, RuntimeError("解析失败"))
    assert len(candidates) == 3
    assert any(_parse_llm_result_text(c, PrizeExtractResult) for c in candidates)


def test_log_throttle_suppresses_multi_consumer_spam() -> None:
    """N 个消费者同一秒撞上冷却时，全局节流器只放行第一条日志。"""
    throttle = _LogThrottle(30.0)
    assert throttle.allow() is True
    assert [throttle.allow() for _ in range(20)] == [False] * 20
    assert _LogThrottle(0.0).allow() is True


class _AlwaysFailLLM:
    """所有调用都失败的假实例，用于测「最大重试轮数」。"""

    def bind(self, **_kwargs):
        return self

    def with_structured_output(self, _schema, include_raw=False):
        assert include_raw is True
        return self

    async def ainvoke(self, _messages, extra_body=None):
        raise RuntimeError("上游不可用")


@pytest.mark.asyncio
async def test_extract_gives_up_after_max_retry_rounds(monkeypatch) -> None:
    """全部实例失败时不能 while True 无限重试：超过上限交还消息。"""
    calls = 0

    def _fake_free_llms():
        nonlocal calls
        calls += 1
        return [_AlwaysFailLLM()]

    monkeypatch.setattr(pe, "_RETRY_BASE_DELAY", 0)
    monkeypatch.setattr(pe, "get_all_free_llms", _fake_free_llms)
    monkeypatch.setattr(pe, "_all_failed_log_throttle", _LogThrottle(0.0))

    with pytest.raises(AllLLMsCoolingError, match="轮未取得可用 LLM"):
        await pe.extract_prize_info_for_biliopusdb(
            dyn_content="关注+转发抽3人送耳机", max_wait=None
        )
    assert calls == pe._MAX_RETRY_ROUNDS


def test_repair_structured_output_reuses_pre_clean_parser() -> None:
    raw = "<think>想一下</think>```json\n{\"is_grand_prize\": true}\n```"
    try:
        OfficialPrizeExtractResult.model_validate(raw)
    except ValidationError as exc:
        repaired = _repair_structured_output(exc, OfficialPrizeExtractResult)
        assert isinstance(repaired, OfficialPrizeExtractResult)
        assert repaired.is_grand_prize is True
    else:
        raise AssertionError("原始文本本应触发 ValidationError")
