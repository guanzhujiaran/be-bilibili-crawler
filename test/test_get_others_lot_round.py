"""第三方抽奖轮次收尾 / 轮次 id 复用回归用例。

覆盖：
- ``_after_scrapy`` 只等本次 run 产生/更新的 dynId，不再按整轮查库；
- ``_after_scrapy`` 异常时轮次仍一定被关闭；
- 陈旧的未完成轮次强制收尾并新开一轮，新鲜的未完成轮次仍可复用。
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from Models.get_other_lot_dyn.dyn_robot_model import RobotScrapyInfo
from Service.GetOthersLotDyn.Sql.models import TLotmaininfo, TLotdyninfo
from Service.GetOthersLotDyn.Sql.sql_helper import (
    SqlHelper,
    get_other_lot_redis_manager,
)
from Service.GetOthersLotDyn.core.bili_dynamic_item import (
    BiliDynamicItemJudgeLotteryResult,
)
from Service.GetOthersLotDyn.core.robot import GetOthersLotDynRobot


@dataclass
class _ExtraInfo:
    """测试用 t_lot_extra_info 最小投影"""

    is_lot: int


def _dyn(dyn_id: int, content: str = "关注+转发抽3人送耳机") -> TLotdyninfo:
    return TLotdyninfo(dynId=dyn_id, dynContent=content, officialLotType=None)


@pytest.mark.asyncio
async def test_after_scrapy_only_waits_for_run_dyn_ids(monkeypatch) -> None:
    robot = GetOthersLotDynRobot()
    robot.nowRound = TLotmaininfo(lotRound_id=370, isRoundFinished=0)
    robot.scrapy_info = RobotScrapyInfo()
    # 模拟「本轮 3 条」与「整轮 2913 条」共存：整轮查询一旦被调用就失败
    robot._run_dyn_ids = {101, 102, 103}

    get_by_ids = AsyncMock(return_value=[_dyn(101), _dyn(102), _dyn(103)])
    get_by_round = AsyncMock(
        side_effect=AssertionError("收尾不应再按整轮查询 getAllLotDynInfoByRoundNum")
    )
    get_extra = AsyncMock(
        return_value={101: _ExtraInfo(1), 102: _ExtraInfo(0), 103: _ExtraInfo(1)}
    )
    set_pending = AsyncMock()

    monkeypatch.setattr(robot, "_retry_pending_extra_refs", AsyncMock())
    monkeypatch.setattr(SqlHelper, "getDynInfoByIds", get_by_ids)
    monkeypatch.setattr(SqlHelper, "getAllLotDynInfoByRoundNum", get_by_round)
    monkeypatch.setattr(SqlHelper, "get_extra_info_map_by_ref_ids", get_extra)
    monkeypatch.setattr(
        get_other_lot_redis_manager, "set_pending_extra_refs", set_pending
    )

    await robot._after_scrapy()

    get_by_round.assert_not_called()
    get_by_ids.assert_awaited_once_with([101, 102, 103])
    get_extra.assert_awaited_once_with([101, 102, 103], lot_type="common")
    assert robot.nowRound.allNum == 3
    assert robot.nowRound.lotNum == 2
    assert robot.nowRound.uselessNum == 1
    # 全部等到 extra_info → 待补偿列表被清空，不产生「本轮有 N 条未等到」
    set_pending.assert_awaited_once_with([])


@pytest.mark.asyncio
async def test_collect_run_dyn_ids_skips_empty_content(monkeypatch) -> None:
    robot = GetOthersLotDynRobot()
    result = BiliDynamicItemJudgeLotteryResult(
        cur_dynamic=_dyn(201),
        orig_dynamic=_dyn(202, content="源动态"),
        attached_card=_dyn(203, content=""),  # 失效/空内容不会发 MQ
    )
    robot._collect_run_dyn_ids(result)
    assert robot._run_dyn_ids == {201, 202}


@pytest.mark.asyncio
async def test_round_is_closed_even_if_after_scrapy_fails(monkeypatch) -> None:
    robot = GetOthersLotDynRobot()
    robot.nowRound = TLotmaininfo(lotRound_id=370, isRoundFinished=0)
    add_round = AsyncMock()

    monkeypatch.setattr(robot, "_GetOthersLotDynRobot__init", AsyncMock())
    monkeypatch.setattr(robot, "run", AsyncMock())
    monkeypatch.setattr(
        robot, "_after_scrapy", AsyncMock(side_effect=RuntimeError("收尾超时"))
    )
    monkeypatch.setattr(SqlHelper, "addLotMainInfo", add_round)

    with pytest.raises(RuntimeError, match="收尾超时"):
        await robot.main()

    assert robot.nowRound.isRoundFinished == 1
    add_round.assert_awaited_once()


@pytest.mark.asyncio
async def test_stale_unfinished_round_is_force_closed(monkeypatch) -> None:
    robot = GetOthersLotDynRobot()
    stale_round = TLotmaininfo(
        lotRound_id=370,
        isRoundFinished=0,
        created_at=datetime.now() - timedelta(hours=7),
    )
    get_latest = AsyncMock(return_value=stale_round)
    add_round = AsyncMock()
    monkeypatch.setattr(SqlHelper, "getLatestRound", get_latest)
    monkeypatch.setattr(SqlHelper, "addLotMainInfo", add_round)
    monkeypatch.setattr(
        get_other_lot_redis_manager, "get_target_uid_list", AsyncMock(return_value=[])
    )

    await robot._GetOthersLotDynRobot__init()

    assert robot.nowRound.lotRound_id == 371
    assert robot.nowRound.isRoundFinished == 0 or robot.nowRound.isRoundFinished is False
    assert add_round.await_count == 2, "一次强制收尾旧轮 + 一次新建轮次"
    assert add_round.await_args_list[0].args[0].isRoundFinished == 1


@pytest.mark.asyncio
async def test_fresh_unfinished_round_is_reused(monkeypatch) -> None:
    robot = GetOthersLotDynRobot()
    fresh_round = TLotmaininfo(
        lotRound_id=370,
        isRoundFinished=0,
        created_at=datetime.now() - timedelta(minutes=5),
    )
    monkeypatch.setattr(
        SqlHelper, "getLatestRound", AsyncMock(return_value=fresh_round)
    )
    add_round = AsyncMock()
    monkeypatch.setattr(SqlHelper, "addLotMainInfo", add_round)
    monkeypatch.setattr(
        get_other_lot_redis_manager, "get_target_uid_list", AsyncMock(return_value=[])
    )

    await robot._GetOthersLotDynRobot__init()

    assert robot.nowRound.lotRound_id == 370
    add_round.assert_awaited_once()
