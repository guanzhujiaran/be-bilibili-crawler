"""空间动态发布时间戳为空时的回归用例。

``__add_space_card_to_db`` 改为返回 ``(ret_list, time_list)`` 后，
空 items / 全置顶 / 解析异常都会返回空 ``time_list``；
此前循环结束后的 ``time_list[-1]`` 会抛 ``IndexError``。
这里覆盖上述三条路径：不抛异常、正常收尾、不误报「动态数量过少」。
"""

import datetime
from unittest.mock import AsyncMock

import pytest

import Service.GetOthersLotDyn.fetcher.space_dynamic_fetcher as fetcher_module
from Service.GetOthersLotDyn.Sql.models import TLotuserinfo
from Service.GetOthersLotDyn.Sql.sql_helper import SqlHelper
from Service.GetOthersLotDyn.fetcher.space_dynamic_fetcher import BiliSpaceUserItem
from log.base_log import get_others_lot_logger

_UID = 123456


@pytest.fixture
def log_messages():
    """收集 get_others_lot_logger 的日志文本"""
    messages: list[str] = []
    sink_id = get_others_lot_logger.add(
        lambda message: messages.append(message.record["message"]),
        level="DEBUG",
    )
    yield messages
    get_others_lot_logger.remove(sink_id)


def _make_item(
    dynamic_id: int,
    *,
    pub_ts: int = 1700000000,
    pinned: bool = False,
    dynamic_type: str = "DYNAMIC_TYPE_FORWARD",
) -> dict:
    modules: dict = {"module_author": {"name": "tester", "pub_ts": pub_ts}}
    if pinned:
        modules["module_tag"] = {"text": "置顶"}
    item: dict = {
        "id_str": str(dynamic_id),
        "type": dynamic_type,
        "modules": modules,
    }
    if dynamic_type == "DYNAMIC_TYPE_FORWARD":
        item["orig"] = {"id_str": str(dynamic_id + 1), "type": "DYNAMIC_TYPE_DRAW"}
    return item


def _make_response(items: list[dict]) -> dict:
    return {
        "code": 0,
        "data": {"items": items, "has_more": False, "offset": "100"},
        "message": "0",
        "ttl": 1,
    }


async def _run_fetch(monkeypatch, response: dict, *, batch_side_effect=None):
    """带 mock 执行一轮空间动态获取，返回 (user, addLotUserInfo mock)"""
    user = BiliSpaceUserItem(lot_round_id=7, uid=_UID)

    async def _get_lot_user_info(_uid):
        return TLotuserinfo(
            uid=_UID,
            uname="tester",
            updateNum=0,
            updatetime=datetime.datetime.now(),
            isUserSpaceFinished=0,
            offset=0,
            latestFinishedOffset=0,
            isPubLotUser=0,
        )

    add_lot_user_info = AsyncMock()
    monkeypatch.setattr(SqlHelper, "getLotUserInfoByUid", _get_lot_user_info)
    monkeypatch.setattr(SqlHelper, "addLotUserInfo", add_lot_user_info)
    monkeypatch.setattr(
        SqlHelper,
        "addSpaceRespBatch",
        AsyncMock(side_effect=batch_side_effect),
    )
    monkeypatch.setattr(
        fetcher_module,
        "get_space_dynamic_req_with_proxy",
        AsyncMock(return_value=response),
    )
    # 内部会 await create_task 触发第二轮获取，这里用 AsyncMock 顶掉，
    # 避免单测真的进入第二轮逻辑（仍然保留 create_task + await 的调用链）。
    user.get_user_space_dynamic_id = AsyncMock(return_value=None)

    await BiliSpaceUserItem.get_user_space_dynamic_id(user, SpareTime=5 * 86400)
    return user, add_lot_user_info


def _assert_finished_normally(
    user, add_lot_user_info, expected_count: int, expected_info_calls: int = 2
) -> None:
    """收尾状态仍按正常流程更新，且本轮动态计数语义不变"""
    assert user.fetched_dyn_count == expected_count
    assert add_lot_user_info.await_count == expected_info_calls
    final_info = add_lot_user_info.await_args_list[-1].args[0]
    assert final_info.isUserSpaceFinished == 1


@pytest.mark.asyncio
async def test_empty_items_does_not_crash(monkeypatch, log_messages) -> None:
    """items 为空：time_list=[]，不抛 IndexError 且正常结束"""
    user, add_lot_user_info = await _run_fetch(monkeypatch, _make_response([]))

    _assert_finished_normally(user, add_lot_user_info, expected_count=0)
    assert any(
        "空间动态响应中未提取到任何发布时间戳" in message for message in log_messages
    )


@pytest.mark.asyncio
async def test_all_items_pinned_does_not_crash(monkeypatch, log_messages) -> None:
    """items 全为置顶：置顶被剔除后 time_list=[]，不抛 IndexError"""
    user, add_lot_user_info = await _run_fetch(
        monkeypatch, _make_response([_make_item(3001, pinned=True)])
    )

    _assert_finished_normally(user, add_lot_user_info, expected_count=0)
    assert any(
        "空间动态响应中未提取到任何发布时间戳" in message for message in log_messages
    )


@pytest.mark.asyncio
async def test_batch_insert_exception_skips_warning(monkeypatch, log_messages) -> None:
    """批量入库解析异常返回 []：跳过「动态过少」告警，n 仍按解析结果计数"""
    user, add_lot_user_info = await _run_fetch(
        monkeypatch,
        _make_response([_make_item(4001)]),
        batch_side_effect=RuntimeError("db down"),
    )

    # 发布时间戳缺失，但 _solve_space_dynamic 仍解析出 1 条动态；
    # 该路径会走到循环内的 lot_user_info 更新，因此共 3 次入库。
    _assert_finished_normally(
        user, add_lot_user_info, expected_count=1, expected_info_calls=3
    )
    assert any(
        "空间动态响应中未提取到任何发布时间戳" in message for message in log_messages
    )
    assert not any(
        "本轮获取到的动态数量过少" in message for message in log_messages
    )
