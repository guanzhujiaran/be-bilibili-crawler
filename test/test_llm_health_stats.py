"""LLM 槽位健康状态机与调用统计口径的单测

纯内存、不联网、不连库：只覆盖
1. 错误分类（``classify_llm_failure``）；
2. 状态迁移（冷却 / 额度 / 欠费 / 删除）；
3. 调用统计口径（失败桶、本地修复回滚）。

运行:
    uv run python -m pytest test/test_llm_health_stats.py -v
"""
import sys
from pathlib import Path

import pytest
from pydantic import BaseModel, ValidationError

# 确保项目根目录在 sys.path 中
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from langchain_core.output_parsers.openai_tools import PydanticToolsParser

from Service.llm_service.health import (
    MAX_CONSECUTIVE_FAILURES,
    LLMFailureKind,
    LLMState,
    classify_llm_failure,
)
from Service.llm_service.tracked_llm import LLMUsageStats


class _FakeError(Exception):
    """带 status_code / code / retry_after 的假异常（模拟 openai 的错误对象）"""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        code: str | None = None,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message)
        if status_code is not None:
            self.status_code = status_code
        if code is not None:
            self.code = code
        if retry_after is not None:
            self.retry_after = retry_after


class _BoolModel(BaseModel):
    """只为构造 / 解析测试用的最小结果模型"""

    flag: bool


def _validation_error() -> ValidationError:
    """造一个真·pydantic 校验失败（模型返回的 JSON 不合规）"""
    with pytest.raises(ValidationError) as exc_info:
        _BoolModel.model_validate_json("不是 JSON")
    return exc_info.value


def _parser_index_error() -> IndexError:
    """造一个 langchain 解析器里的下标越界（模型只回散文、没有 tool call）"""
    with pytest.raises(IndexError) as exc_info:
        PydanticToolsParser(tools=[_BoolModel]).parse_result([])
    return exc_info.value


# ================================================================
# 1. 错误分类
# ================================================================


def test_status_code_permanent_set():
    """401 / 403 / 404 / 410 都是不可恢复：鉴权、权限、模型不存在、模型已下线"""
    for status in (401, 403, 404, 410):
        kind = classify_llm_failure(_FakeError("nope", status_code=status))
        assert kind is LLMFailureKind.PERMANENT, status


def test_410_end_of_life_is_permanent():
    """生产实例：NVIDIA 对已下线模型回 410 Gone（此前漏判，白跑了 278 次）"""
    error = _FakeError(
        "Error code: 410 - {'title': 'Gone', 'status': 410, 'detail': "
        "\"The model 'deepseek-ai/deepseek-v4-flash-0731' has reached its "
        "end of life on 2026-09-21T08:00:00Z and is no longer available.\"}",
        status_code=410,
    )
    assert classify_llm_failure(error) is LLMFailureKind.PERMANENT


def test_model_missing_keywords_are_permanent_regardless_of_status():
    """「模型没 provider / 已下线」写在文案里时，不限状态码也判不可恢复"""
    cases = (
        ("Model id : x , has no provider supported", 400),
        ("model not found", 503),
        ("the model is no longer available", None),
        ("模型已下线", 400),
    )
    for message, status in cases:
        kind = classify_llm_failure(_FakeError(message, status_code=status))
        assert kind is LLMFailureKind.PERMANENT, message


def test_rate_limit_three_way_split():
    """429 按文案拆成「今日额度 / 欠费 / 频率限流」"""
    assert (
        classify_llm_failure(_FakeError("daily quota exceeded", status_code=429))
        is LLMFailureKind.QUOTA_EXHAUSTED
    )
    assert (
        classify_llm_failure(_FakeError("欠费，请充值", status_code=429))
        is LLMFailureKind.SUSPENDED
    )
    assert (
        classify_llm_failure(_FakeError("Too Many Requests", status_code=429))
        is LLMFailureKind.RATE_LIMITED
    )


def test_402_is_suspended_and_5xx_timeout_are_transient():
    assert (
        classify_llm_failure(_FakeError("insufficient balance", status_code=402))
        is LLMFailureKind.SUSPENDED
    )
    assert (
        classify_llm_failure(_FakeError("overloaded", status_code=503))
        is LLMFailureKind.TRANSIENT
    )
    assert classify_llm_failure(_FakeError("Request timed out")) is (
        LLMFailureKind.TRANSIENT
    )


def test_content_problem_is_request_not_permanent():
    """400 里的「本次请求内容问题」不能删配置"""
    error = _FakeError(
        "context_length_exceeded", status_code=400, code="context_length_exceeded"
    )
    assert classify_llm_failure(error) is LLMFailureKind.REQUEST


def test_validation_error_is_output_invalid():
    """模型返回的 JSON 不合规 → 输出不合规，不能当成 5xx 抖动"""
    assert classify_llm_failure(_validation_error()) is LLMFailureKind.OUTPUT_INVALID


def test_parser_index_error_is_output_invalid():
    """模型只回散文、解析器下标越界 → 输出不合规"""
    assert classify_llm_failure(_parser_index_error()) is LLMFailureKind.OUTPUT_INVALID


def test_bare_index_error_is_not_output_invalid():
    """与 langchain 无关的 IndexError 不能被误判成输出不合规"""
    with pytest.raises(IndexError) as exc_info:
        [][0]
    assert classify_llm_failure(exc_info.value) is LLMFailureKind.TRANSIENT


# ================================================================
# 2. 状态迁移
# ================================================================


def test_transient_cools_down_after_threshold_and_recovers():
    """瞬时故障：前 N-1 次只计数，第 N 次进冷却，到点自动回 healthy"""
    stats = LLMUsageStats()
    for _ in range(MAX_CONSECUTIVE_FAILURES - 1):
        stats.record_failure(_FakeError("boom", status_code=503))
    assert stats.state is LLMState.HEALTHY
    assert stats.available is True

    stats.record_failure(_FakeError("boom", status_code=503))
    assert stats.state is LLMState.COOLING
    assert stats.available is False

    # 冷却到点（把恢复时间拨到过去）后读取即回 healthy
    stats.health._resume_at = 0.0
    assert stats.state is LLMState.HEALTHY


def test_output_invalid_cools_down_but_never_removes_config():
    """输出不合规连续发生 → 只冷却，绝不删配置（是模型不擅长，不是配置坏了）"""
    stats = LLMUsageStats()
    for _ in range(10):
        stats.record_failure(_validation_error())
        stats.health._resume_at = 0.0  # 跳过冷却，模拟「冷却结束后又失败」
    assert stats.state is not LLMState.REMOVED
    assert stats.disabled is False


def test_permanent_removes_slot():
    stats = LLMUsageStats()
    stats.record_failure(_FakeError("gone", status_code=410))
    assert stats.state is LLMState.REMOVED
    assert stats.disabled is True
    # REMOVED 是终态：成功也不会把它拉回来
    stats.record_success(1.0)
    assert stats.state is LLMState.REMOVED


def test_quota_exhausted_waits_until_next_midnight():
    stats = LLMUsageStats()
    stats.record_failure(_FakeError("daily quota", status_code=429))
    assert stats.state is LLMState.QUOTA_WAIT
    assert stats.resume_at is not None


def test_retry_after_enlarges_cooldown():
    """上游给了 Retry-After 时，冷却时间至少听它的"""
    stats = LLMUsageStats()
    for _ in range(MAX_CONSECUTIVE_FAILURES):
        stats.record_failure(
            _FakeError("overloaded", status_code=503, retry_after=300.0)
        )
    assert stats.state is LLMState.COOLING
    delay = stats.health.resume_delay()
    assert delay is not None and delay > 290


def test_suspended_cooldown_doubles():
    stats = LLMUsageStats()
    stats.record_failure(_FakeError("insufficient balance", status_code=402))
    first = stats.health.resume_delay()
    stats.record_failure(_FakeError("insufficient balance", status_code=402))
    second = stats.health.resume_delay()
    assert first is not None and second is not None
    assert second > first


# ================================================================
# 3. 统计口径
# ================================================================


def test_failure_goes_to_failed_bucket():
    """失败调用的耗时 / token 落在失败桶，不污染成功口径"""
    stats = LLMUsageStats()
    stats.record_start()
    stats.record_failure(
        _FakeError("overloaded", status_code=503),
        elapsed_seconds=2.5,
        input_tokens=100,
        output_tokens=10,
        total_tokens=110,
    )
    assert stats.failure_count == 1
    assert stats.failed_elapsed_seconds == pytest.approx(2.5)
    assert stats.failed_total_tokens == 110
    # 成功口径不受影响
    assert stats.total_elapsed_seconds == 0.0
    assert stats.total_tokens == 0
    assert stats.avg_latency_seconds == 0.0
    # 全部口径能反映这次失败
    assert stats.avg_latency_all_seconds == pytest.approx(2.5)
    assert stats.avg_tokens_per_call_all == pytest.approx(110)


def test_record_recovered_rolls_back_failure():
    """本地修复成功 = 一次成功调用：失败计数回滚、耗时/token 挪进成功桶"""
    stats = LLMUsageStats()
    stats.record_start()
    stats.record_failure(_validation_error(), elapsed_seconds=3.0, total_tokens=220)
    assert stats.failure_count == 1

    assert stats.record_recovered() is True
    assert stats.recovered_count == 1
    assert stats.failure_count == 0
    assert stats.success_count == 1
    assert stats.consecutive_failures == 0
    assert stats.last_error is None
    assert stats.total_elapsed_seconds == pytest.approx(3.0)
    assert stats.total_tokens == 220
    assert stats.failed_elapsed_seconds == 0.0
    assert stats.failed_total_tokens == 0
    assert stats.state is LLMState.HEALTHY


def test_repeated_repairs_never_knock_slot_out():
    """生产问题回归：连续 N 次「可修复的格式失败」不应把健康槽位推进冷却"""
    stats = LLMUsageStats()
    for _ in range(20):
        stats.record_start()
        stats.record_failure(_validation_error())
        stats.record_recovered()
    assert stats.state is LLMState.HEALTHY
    assert stats.available is True
    assert stats.failure_count == 0
    assert stats.success_count == 20
    assert stats.recovered_count == 20


def test_record_recovered_without_pending_failure_is_noop():
    """解析发生在统计埋点之外时，那次调用已被记成成功，不能再补记一次"""
    stats = LLMUsageStats()
    stats.record_start()
    stats.record_success(1.0)
    assert stats.record_recovered() is False
    assert stats.success_count == 1
    assert stats.recovered_count == 1
