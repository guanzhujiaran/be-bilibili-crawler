# -*- coding: utf-8 -*-
"""话题爬虫的应答分类单测（不连库、不发请求）。

背景：``save_resp`` 以前把**所有** ``code != 0`` 都记成 ``nullData``，等于把
-352 / -412（风控）/ -500（服务端）全部当成"话题不存在"，既污染空数据统计，
又让这些 id 永远进不了重试队列（2026-11-16 丢数据的同类问题）。

现在的口径：
- ``code == 0`` → 正常写入，``complete``；
- ``code`` 属于终态集合（话题不存在 / 已删除）→ ``nullData``；
- 其它非 0 码 / 非法码（None、字符串、风控、5xx）→ ``fail``，由重试队列兜底。
"""

from Service.BaseCrawler.model.base import WorkerStatus
from Service.opus新版官方抽奖.活动抽奖.话题抽奖.robot import (
    TOPIC_TERMINAL_CODES,
    topic_robot,
)


async def test_missing_topic_is_null_data():
    """话题确实不存在 → 终态空数据（否则会永久 pending 在重试队列里）。"""
    for code in sorted(TOPIC_TERMINAL_CODES):
        status = await topic_robot.save_resp(123456, {"code": code, "message": "啥都木有"})
        assert status is WorkerStatus.nullData


async def test_topic_not_exist_4401031_is_null_data():
    """4401031（话题不存在）是线上真实码：必须一次记终态，不能走 20 次重试。"""
    status = await topic_robot.save_resp(
        1342346, {"code": 4401031, "message": "话题不存在"}
    )
    assert status is WorkerStatus.nullData


async def test_terminal_message_keyword_is_null_data():
    """非标准 code 但文案是「话题不存在 / 已删除」时同样按终态处理。"""
    for message in ("话题不存在", "该话题已删除", "活动已删除"):
        status = await topic_robot.save_resp(
            1342346, {"code": -400, "message": message}
        )
        assert status is WorkerStatus.nullData


async def test_retry_round_generator_is_bounded():
    """补跑轮（init_params=None）不产出任何自增 id，只跑 pre_params。"""
    params = [param async for param in topic_robot.key_params_gen(None)]
    assert params == []


async def test_is_stop_when_beyond_known_head():
    """自增越过「已知 head + buffer」直接停止（不依赖 ctime）。"""
    origin = (
        topic_robot._known_head,
        topic_robot._head_buffer,
        topic_robot._generated_topic_id,
        topic_robot._cur_stop_times,
    )
    try:
        topic_robot._known_head = 1351803
        topic_robot._head_buffer = 5000
        topic_robot._cur_stop_times = 0
        topic_robot._generated_topic_id = 1351803 + 5000
        assert await topic_robot.is_stop() is False
        topic_robot._generated_topic_id += 1
        assert await topic_robot.is_stop() is True
    finally:
        (
            topic_robot._known_head,
            topic_robot._head_buffer,
            topic_robot._generated_topic_id,
            topic_robot._cur_stop_times,
        ) = origin


async def test_risk_control_code_is_retryable_failure():
    """-412 / -352 是风控，不是"话题不存在"：必须 fail，交给重试队列。"""
    for code in (-412, -352):
        status = await topic_robot.save_resp(123456, {"code": code, "message": "风控"})
        assert status is WorkerStatus.fail


async def test_server_error_and_bad_code_are_retryable():
    """服务端 5xx / 非法 code（None、空串）也不能当空数据丢掉。"""
    for code in (-500, -503, None, "", "not-a-code"):
        status = await topic_robot.save_resp(123456, {"code": code})
        assert status is WorkerStatus.fail
