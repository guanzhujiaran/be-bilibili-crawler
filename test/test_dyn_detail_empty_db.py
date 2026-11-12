"""`DynDetailScrapy` 断点解析（空库 / 断点过旧 / 断点过小）的行为单测。

背景（2026-11-12 dev 环境）：
`dyndetail.bilidyndetail` 被清空后，旧代码在空库时直接 `int(None)` 抛 TypeError，
后来改成「打 WARNING 跳过」——但表一旦真的是空的，GET_DYN 就每天跳过、永远不工作，
即使数据补回来也要人工确认。现在：

1. 空库 / 断点早于保留窗口（`COLD_ARCHIVE_KEEP_DAYS_DYNDETAIL`）：
   从**窗口起点**那条抽奖动态（lotdata.business_id → gRPC → extend.businessId）
   反查 rid，把整个保留窗口重新回补，而不是只补最近几天；
2. 连锚点都拿不到：打 ERROR + 推送「运行异常」并跳过，明确提示先同步数据；
3. 断点还在窗口内：直接沿用，保证增量不断档；
4. 断点过小（< settings.DYN_DETAIL_START_RID）：用配置的起始 rid 兜底并补缺动态。

运行:
    uv run python -m pytest test/test_dyn_detail_empty_db.py -v
"""

import sys
import time
from pathlib import Path
from unittest import mock

import pytest

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from CONFIG import settings
from Service.GrpcModule.GrpcSrc import getDynDetail as dyn_detail_module
from Service.GrpcModule.GrpcSrc.SQLObject.DynDetailSqlHelperMysqlVer import (
    DynDetailBreakpoint,
)

_ANCHOR_DYNAMIC_ID = 1051919215480537096
_ANCHOR_RID = 360_000_000


def _make_scrapy(
    breakpoint: DynDetailBreakpoint | None,
    *,
    anchor_ids: list[int] | None = None,
    anchor_resp: dict | None = None,
) -> dyn_detail_module.DynDetailScrapy:
    """构造一个只替换掉外部依赖的 DynDetailScrapy（不走 __init__ 里的重依赖）。"""
    scrapy = object.__new__(dyn_detail_module.DynDetailScrapy)
    scrapy.Sqlhelper = mock.Mock()
    scrapy.Sqlhelper.get_latest_rid_breakpoint = mock.AsyncMock(
        return_value=breakpoint
    )
    scrapy.Sqlhelper.get_earliest_lot_dynamic_ids_since = mock.AsyncMock(
        return_value=anchor_ids or []
    )
    scrapy.BiliGrpc = mock.Mock()
    scrapy.BiliGrpc.grpc_get_dynamic_detail_by_dynamic_id = mock.AsyncMock(
        return_value=anchor_resp or {}
    )
    scrapy.run = mock.AsyncMock(return_value=None)
    scrapy.log = mock.Mock()
    scrapy.succ_counter = mock.Mock()
    return scrapy


@pytest.mark.asyncio
async def test_empty_db_without_anchor_reports_and_skips(monkeypatch) -> None:
    """空库且拿不到锚点：不能静默跳过，要打 ERROR + 推送，并复位运行标志。"""
    push_error = mock.AsyncMock()
    monkeypatch.setattr(dyn_detail_module, "a_push_error", push_error)
    scrapy = _make_scrapy(None)

    await scrapy.main()

    scrapy.run.assert_not_awaited()
    push_error.assert_awaited_once()
    error_text = scrapy.log.error.call_args.args[0]
    assert "空" in error_text
    assert "sync_lottery_from_prod" in error_text
    scrapy.log.exception.assert_not_called()

    # 审计：finally 仍会复位运行标志，提前 return 不能漏掉
    assert scrapy.succ_counter.is_running is False


@pytest.mark.asyncio
async def test_empty_db_uses_window_start_dynamic_as_anchor(monkeypatch) -> None:
    """空库（本地库被清过）：从保留窗口起点那条抽奖动态反查 rid，回补整窗口。"""
    push_error = mock.AsyncMock()
    monkeypatch.setattr(dyn_detail_module, "a_push_error", push_error)
    scrapy = _make_scrapy(
        None,
        anchor_ids=[_ANCHOR_DYNAMIC_ID],
        anchor_resp={"item": {"extend": {"businessId": _ANCHOR_RID}}},
    )

    await scrapy.main()

    push_error.assert_not_awaited()
    scrapy.run.assert_awaited_once()
    assert scrapy.run.call_args.args[0].rid == _ANCHOR_RID
    # 锚点窗口 = 冷归档保留窗口（默认 180 天），不是「最近几天」
    since_ts = scrapy.Sqlhelper.get_earliest_lot_dynamic_ids_since.call_args.kwargs[
        "since_ts"
    ]
    assert since_ts == pytest.approx(
        int(time.time()) - settings.COLD_ARCHIVE_KEEP_DAYS_DYNDETAIL * 86400, abs=60
    )
    # 走了「自动重定位」分支，不应再打「跳过」类错误
    assert not any(
        "跳过" in str(call.args[0]) for call in scrapy.log.error.call_args_list
    )


@pytest.mark.asyncio
async def test_backfill_days_can_shrink_anchor_window(monkeypatch) -> None:
    """`DYN_DETAIL_BACKFILL_DAYS` 可单独调小回补窗口，不影响冷归档删库窗口。"""
    push_error = mock.AsyncMock()
    monkeypatch.setattr(dyn_detail_module, "a_push_error", push_error)
    monkeypatch.setattr(settings, "DYN_DETAIL_BACKFILL_DAYS", 30)
    scrapy = _make_scrapy(
        None,
        anchor_ids=[_ANCHOR_DYNAMIC_ID],
        anchor_resp={"item": {"extend": {"businessId": _ANCHOR_RID}}},
    )

    await scrapy.main()

    since_ts = scrapy.Sqlhelper.get_earliest_lot_dynamic_ids_since.call_args.kwargs[
        "since_ts"
    ]
    assert since_ts == pytest.approx(int(time.time()) - 30 * 86400, abs=60)


@pytest.mark.asyncio
async def test_empty_db_skips_when_anchor_is_deleted(monkeypatch) -> None:
    """锚点动态已被删除（gRPC 无 item）：本轮仍要跳过并告警，而不是带着坏断点开跑。"""
    push_error = mock.AsyncMock()
    monkeypatch.setattr(dyn_detail_module, "a_push_error", push_error)
    scrapy = _make_scrapy(None, anchor_ids=[_ANCHOR_DYNAMIC_ID], anchor_resp={})

    await scrapy.main()

    scrapy.run.assert_not_awaited()
    push_error.assert_awaited_once()


@pytest.mark.asyncio
async def test_fresh_breakpoint_is_reused(monkeypatch) -> None:
    """断点新鲜：直接用库里的断点，不去请求锚点。"""
    push_error = mock.AsyncMock()
    monkeypatch.setattr(dyn_detail_module, "a_push_error", push_error)
    breakpoint = DynDetailBreakpoint(
        rid=_ANCHOR_RID + 1, latest_dynamic_ts=int(time.time()) - 3600
    )
    scrapy = _make_scrapy(breakpoint, anchor_ids=[_ANCHOR_DYNAMIC_ID])

    await scrapy.main()

    push_error.assert_not_awaited()
    scrapy.run.assert_awaited_once()
    assert scrapy.run.call_args.args[0].rid == breakpoint.rid
    scrapy.Sqlhelper.get_earliest_lot_dynamic_ids_since.assert_not_awaited()


@pytest.mark.asyncio
async def test_stale_breakpoint_relocates_to_anchor(monkeypatch) -> None:
    """断点早于保留窗口（本地库停在很久以前）：回到窗口起点回补，而不是追全部历史。"""
    push_error = mock.AsyncMock()
    monkeypatch.setattr(dyn_detail_module, "a_push_error", push_error)
    breakpoint = DynDetailBreakpoint(
        rid=346_837_980,
        latest_dynamic_ts=int(time.time())
        - (settings.COLD_ARCHIVE_KEEP_DAYS_DYNDETAIL + 30) * 86400,
    )
    scrapy = _make_scrapy(
        breakpoint,
        anchor_ids=[_ANCHOR_DYNAMIC_ID],
        anchor_resp={"item": {"extend": {"businessId": _ANCHOR_RID}}},
    )

    await scrapy.main()

    push_error.assert_not_awaited()
    scrapy.run.assert_awaited_once()
    assert scrapy.run.call_args.args[0].rid == _ANCHOR_RID
    warning_text = scrapy.log.warning.call_args.args[0]
    assert "断点过旧" in warning_text


@pytest.mark.asyncio
async def test_small_breakpoint_falls_back_to_configured_start_rid(
    monkeypatch,
) -> None:
    """断点很小但时间新鲜：沿用原有分支，用配置的起始 rid 兜底并补缺动态。"""
    push_error = mock.AsyncMock()
    monkeypatch.setattr(dyn_detail_module, "a_push_error", push_error)

    async def _no_sleep(_seconds: float) -> None:
        return None

    # main() 在「断点过小」分支里会 sleep(30) 等待补缺任务先跑起来
    monkeypatch.setattr(dyn_detail_module.asyncio, "sleep", _no_sleep)

    scrapy = _make_scrapy(
        DynDetailBreakpoint(rid=1, latest_dynamic_ts=int(time.time()))
    )
    scrapy.get_discontious_dynamics_by_single_detail = mock.AsyncMock(return_value=None)
    scrapy.get_lost_lottery_notice = mock.AsyncMock(return_value=None)

    await scrapy.main()

    push_error.assert_not_awaited()
    scrapy.run.assert_awaited_once()
    started_params = scrapy.run.call_args.args[0]
    assert started_params.rid == settings.DYN_DETAIL_START_RID
    assert settings.DYN_DETAIL_START_RID > 1
