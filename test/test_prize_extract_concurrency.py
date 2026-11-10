"""提取队列并发消费 / 前置解析回归用例。

约束：并发改造不得改变 ack/nack 语义 —— 只有「库里已有」与「成功写库」才 ack。
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from Models.MQ.PrizeExtractMQModel import (
    PrizeExtractParams,
    PrizeExtractTargetEnum,
)
from Service.GetOthersLotDyn.parser import prize_extractor as pe
from Service.MQ.base.MQClient import PrizeExtract as module
from Service.MQ.base.MQClient.PrizeExtract import (
    _consume_once,
    register_parallel_consumers,
)
from Service.MQ.base.MQClient.consume_budget import ConsumeBudget


def test_register_parallel_consumers_adds_competing_consumers() -> None:
    fake_router = MagicMock()

    added = register_parallel_consumers(fake_router, total_consumers=3)

    # 2 条队列 × 2 个额外 subscriber
    assert added == 4
    assert fake_router.subscriber.call_count == 4
    assert fake_router.subscriber.return_value.call_count == 4
    handlers = [
        call.args[0]
        for call in fake_router.subscriber.return_value.call_args_list
    ]
    assert len({handler.__name__ for handler in handlers}) == 4
    # 每个 subscriber 都带自己的 channel / prefetch，才能叠加在途未确认数
    for call in fake_router.subscriber.call_args_list:
        assert call.kwargs["channel"].prefetch_count >= 32

    # 同一 router 不重复注册
    assert register_parallel_consumers(fake_router, total_consumers=3) == 0
    assert fake_router.subscriber.call_count == 4


def test_single_consumer_config_registers_nothing() -> None:
    fake_router = MagicMock()
    assert register_parallel_consumers(fake_router, total_consumers=1) == 0
    fake_router.subscriber.assert_not_called()


@pytest.mark.asyncio
async def test_extract_gate_bounds_concurrency_and_keeps_ack(monkeypatch) -> None:
    monkeypatch.setattr(module, "_EXTRACT_MAX_CONCURRENCY", 2)
    monkeypatch.setattr(module, "_extract_gate", None)
    monkeypatch.setattr(module, "_extract_gate_loop", None)

    active = 0
    peak = 0

    async def _fake_extract(params, budget):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.02)
        active -= 1
        return "ok"

    async def _fake_run_with_backoff(func, config=None):
        return await func()

    monkeypatch.setattr(module, "_do_extract_and_store", _fake_extract)
    monkeypatch.setattr(module, "run_with_backoff", _fake_run_with_backoff)
    monkeypatch.setattr(module, "_wait_own_lock", AsyncMock())
    monkeypatch.setattr(module, "_already_stored", AsyncMock(return_value=False))
    monkeypatch.setattr(module.prize_extract_redis, "release_lock", AsyncMock())

    messages = []
    tasks = []
    for idx in range(6):
        params = PrizeExtractParams(
            target_db=PrizeExtractTargetEnum.BILIOPUSDB,
            ref_id=900000 + idx,
            lot_type="common",
            dyn_content="关注+转发抽3人送耳机",
        )
        msg = AsyncMock()
        msg.ack = AsyncMock()
        msg.nack = AsyncMock()
        messages.append(msg)
        budget = ConsumeBudget.start(
            total_seconds=60.0, queue_name="test-queue", label=str(idx)
        )
        tasks.append(
            _consume_once(
                module.prize_extract_biliopus.mq_props, params, msg, budget
            )
        )

    results = await asyncio.gather(*tasks)

    assert peak <= 2, "在途 LLM 提取数不得超过闸上限"
    assert results == ["ok"] * 6
    for msg in messages:
        msg.ack.assert_awaited_once()
        msg.nack.assert_not_called()


class _FakeStructuredLLM:
    async def ainvoke(self, messages, extra_body=None):
        # 模拟 thinking 模型：结构化解析失败，raw 里带 <think> 与代码围栏
        return {
            "raw": SimpleNamespace(
                content=(
                    "<think>让我判断一下是不是抽奖……</think>\n"
                    '```json\n{"is_lot": true, "prize_names": ["耳机"]}\n```'
                ),
                additional_kwargs={},
            ),
            "parsed": None,
            "parsing_error": RuntimeError("JSONDecodeError: Expecting value"),
        }


class _FakeLLM:
    def bind(self, **kwargs):
        return self

    def with_structured_output(self, schema, include_raw=False):
        assert include_raw is True, "必须拿到 raw 才能做解析前置清洗"
        return _FakeStructuredLLM()


@pytest.mark.asyncio
async def test_do_extract_recovers_thinking_output_without_retry(monkeypatch) -> None:
    monkeypatch.setattr(pe, "get_all_free_llms", lambda: [_FakeLLM()])

    resp = await pe.extract_prize_info_for_biliopusdb(
        dyn_content="关注+转发，抽3人送蓝牙耳机，10月30日开奖"
    )

    assert resp.result.is_lot is True
    assert resp.result.prize_names == ["耳机"]
