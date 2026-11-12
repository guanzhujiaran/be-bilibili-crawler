"""`get_lot_notice` 对上游 -22（服务系统超时）的降级行为单测。

背景（2026-11-12）：
B 站抽奖 notice 接口对「抽奖已结束 / 上游瞬时抖动」返回 `{'code': -22, 'message': '服务系统超时'}`，
旧实现把所有非 0 非 -9999 的响应都当错误：ERROR + 推「运行异常」+ sleep(100) 无限重试，
6 小时内刷了 3 次告警，且每次都卡 100 秒。

约定：
1. -22 → 只打 WARNING，不推「运行异常」；按 5s / 15s / 45s 退避重试 3 次；
2. 3 次仍 -22 → 返回 dict 且 `data` 为 `{}`（调用方按「没拿到抽奖信息」跳过）；
3. -9999 → 原样返回（代理响应，不是 B 站响应），不重试、不推送；
4. 其余非 0 code → 保持原行为（ERROR + 推送 + 100s 重试）。

运行:
    uv run python -m pytest test/test_lot_notice_timeout_code.py -v
"""

import sys
from pathlib import Path
from unittest import mock

import pytest

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from Service.GrpcModule.Grpc.Bapi import BiliApi


def _patch_dependencies(monkeypatch, responses: list[dict]):
    """把网络层、推送、日志全部替换成 mock，返回 (请求 mock, 推送 mock, 日志 mock)。"""
    request_mock = mock.AsyncMock(side_effect=responses)
    monkeypatch.setattr(
        BiliApi.request_with_proxy_internal, "request_with_proxy", request_mock
    )
    monkeypatch.setattr(
        BiliApi, "prepare_request_data", mock.AsyncMock(return_value=({}, {}))
    )
    # 退避归零：不真的等 5/15/45 秒
    monkeypatch.setattr(
        BiliApi, "LOT_NOTICE_TIMEOUT_BACKOFF", (0.0, 0.0, 0.0)
    )

    async def _no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(BiliApi.asyncio, "sleep", _no_sleep)

    push_error = mock.AsyncMock()
    monkeypatch.setattr(BiliApi, "a_push_error", push_error)
    bapi_log = mock.Mock()
    monkeypatch.setattr(BiliApi, "bapi_log", bapi_log)
    return request_mock, push_error, bapi_log


@pytest.mark.asyncio
async def test_timeout_code_retries_then_returns_empty_data(monkeypatch) -> None:
    response = {"code": -22, "message": "服务系统超时"}
    request_mock, push_error, bapi_log = _patch_dependencies(
        monkeypatch, [response] * 20
    )

    result = await BiliApi.get_lot_notice(
        business_type=2, business_id=123456, origin_dynamic_id=789012
    )

    # 首次请求 + 3 次退避重试，之后放弃（不再无限 100s 循环）
    assert request_mock.await_count == 1 + len(BiliApi.LOT_NOTICE_TIMEOUT_BACKOFF)
    assert isinstance(result, dict)
    assert result["code"] == -22
    assert result["data"] == {}
    # 不刷 ERROR、不推「运行异常」
    push_error.assert_not_awaited()
    bapi_log.error.assert_not_called()
    # 3 次重试 + 1 次「放弃」告警
    assert bapi_log.warning.call_count == len(BiliApi.LOT_NOTICE_TIMEOUT_BACKOFF) + 1


@pytest.mark.asyncio
async def test_timeout_code_recovers_on_retry(monkeypatch) -> None:
    responses = [
        {"code": -22, "message": "服务系统超时"},
        {"code": -22, "message": "服务系统超时"},
        {"code": 0, "data": {"lottery_id": 42}},
    ]
    request_mock, push_error, bapi_log = _patch_dependencies(monkeypatch, responses)

    result = await BiliApi.get_lot_notice(business_type=10, business_id=1)

    assert request_mock.await_count == 3
    assert result["code"] == 0
    assert result["data"] == {"lottery_id": 42}
    push_error.assert_not_awaited()
    bapi_log.error.assert_not_called()


@pytest.mark.asyncio
async def test_proxy_response_code_is_returned_as_is(monkeypatch) -> None:
    response = {"code": -9999, "message": "代理响应"}
    request_mock, push_error, bapi_log = _patch_dependencies(monkeypatch, [response])

    result = await BiliApi.get_lot_notice(business_type=12, business_id=1)

    assert request_mock.await_count == 1
    assert result is response
    push_error.assert_not_awaited()
    bapi_log.error.assert_not_called()


@pytest.mark.asyncio
async def test_other_error_code_keeps_old_behaviour(monkeypatch) -> None:
    responses = [{"code": -500, "message": "其他错误"}, {"code": 0, "data": {}}]
    request_mock, push_error, bapi_log = _patch_dependencies(monkeypatch, responses)

    result = await BiliApi.get_lot_notice(business_type=2, business_id=1)

    assert request_mock.await_count == 2
    assert result["code"] == 0
    bapi_log.error.assert_called_once()
    assert "get_lot_notice响应代码错误" in bapi_log.error.call_args.args[0]
    push_error.assert_awaited_once()
