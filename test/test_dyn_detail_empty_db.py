"""`DynDetailScrapy.main()` 在「空库 / 断点过小」时的行为单测。

背景（2026-11-12 dev 环境）：
`dyndetail.bilidyndetail` 是空表时 `get_latest_rid()` 返回 None，
旧代码直接 `int(None)` 抛 TypeError → 打「爬取动态任务出错」并推「运行异常」，
本轮动态详情任务从未真正启动（日志里连「开始执行获取动态详情」都没有）。

约定：
1. 空库（None）→ 打 WARNING「跳过本次动态详情爬取」，不调用 run()、不推送；
2. 断点过小（< settings.DYN_DETAIL_START_RID）→ 用配置的起始 rid 兜底并正常启动。

运行:
    uv run python -m pytest test/test_dyn_detail_empty_db.py -v
"""

import sys
from pathlib import Path
from unittest import mock

import pytest

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from CONFIG import settings
from Service.GrpcModule.GrpcSrc import getDynDetail as dyn_detail_module


def _make_scrapy(latest_rid: int | None) -> dyn_detail_module.DynDetailScrapy:
    """构造一个只替换掉外部依赖的 DynDetailScrapy（不走 __init__ 里的重依赖）。"""
    scrapy = object.__new__(dyn_detail_module.DynDetailScrapy)
    scrapy.Sqlhelper = mock.Mock()
    scrapy.Sqlhelper.get_latest_rid = mock.AsyncMock(return_value=latest_rid)
    scrapy.run = mock.AsyncMock(return_value=None)
    scrapy.log = mock.Mock()
    scrapy.succ_counter = mock.Mock()
    return scrapy


@pytest.mark.asyncio
async def test_main_skips_when_table_is_empty(monkeypatch) -> None:
    push_error = mock.AsyncMock()
    monkeypatch.setattr(dyn_detail_module, "a_push_error", push_error)
    scrapy = _make_scrapy(None)

    # 空库不应抛异常
    await scrapy.main()

    scrapy.run.assert_not_awaited()
    scrapy.log.warning.assert_called_once()
    warning_text = scrapy.log.warning.call_args.args[0]
    assert "空库" in warning_text
    assert "跳过本次动态详情爬取" in warning_text
    # 不推「运行异常」，也不要按错误处理
    push_error.assert_not_awaited()
    scrapy.log.error.assert_not_called()
    scrapy.log.exception.assert_not_called()

    # 审计：finally 仍会复位运行标志，提前 return 不能漏掉
    assert scrapy.succ_counter.is_running is False


@pytest.mark.asyncio
async def test_main_falls_back_to_configured_start_rid(monkeypatch) -> None:
    push_error = mock.AsyncMock()
    monkeypatch.setattr(dyn_detail_module, "a_push_error", push_error)

    async def _no_sleep(_seconds: float) -> None:
        return None

    # main() 在「断点过小」分支里会 sleep(30) 等待补缺任务先跑起来
    monkeypatch.setattr(dyn_detail_module.asyncio, "sleep", _no_sleep)

    scrapy = _make_scrapy(1)  # 远小于起始 rid
    scrapy.get_discontious_dynamics_by_single_detail = mock.AsyncMock(return_value=None)
    scrapy.get_lost_lottery_notice = mock.AsyncMock(return_value=None)

    await scrapy.main()

    push_error.assert_not_awaited()
    scrapy.run.assert_awaited_once()
    started_params = scrapy.run.call_args.args[0]
    assert started_params.rid == settings.DYN_DETAIL_START_RID
    assert settings.DYN_DETAIL_START_RID > 1
