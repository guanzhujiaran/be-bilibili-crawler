"""测试从 MQ Handler → Consumer → LLM 提取 → 入库的完整流程

使用 SQLite 内存数据库模拟，无需 MySQL。
使用 FastStream TestRabbitBroker 内存路由消息。

运行:
    uv run python -m pytest test/test_prize_extract_flow.py -v
    uv run python -m pytest test/test_prize_extract_flow.py -v -k "test_do_extract"
    uv run python -m pytest test/test_prize_extract_flow.py -v -k "test_handler_inmemory" -s
"""
import asyncio
import sys
from contextlib import AsyncExitStack
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from bili_common.core.backoff import BackoffConfig
from sqlalchemy import select, and_, text, or_, exists, case
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler

# SQLite 不支持 MySQL TINYINT 和 on update CURRENT_TIMESTAMP，运行时补丁
if not hasattr(SQLiteTypeCompiler, "visit_TINYINT"):
    SQLiteTypeCompiler.visit_TINYINT = lambda self, type_, **kw: "INTEGER"
    SQLiteTypeCompiler.visit_TIMESTAMP = lambda self, type_, **kw: "TIMESTAMP"

# 确保项目根目录在 sys.path 中
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from Models.MQ.PrizeExtractResult import PrizeExtractResult
from Models.MQ.PrizeExtractMQModel import (
    PrizeExtractParams, PrizeExtractTargetEnum,
)
from Models.lottery_database.bili.LotteryDataModels import OfficialLotType

# ---- 导入 ORM 模型（用于建表） ----
from Service.GetOthersLotDyn.Sql.models import (
    Base,
    TLotmaininfo, TLotuserinfo, TLotdyninfo,
    TLotuserspaceresp, TRiddynid, TLotExtraInfo,
)
from Service.GetOthersLotDyn.Sql.sql_helper import __SqlHelper
from Service.MQ.base.MQClient import PrizeExtract as prize_extract_module
from Service.MQ.base.MQClient.PrizeExtract import (
    _consume_once, _do_extract_and_store, process_prize_extract,
    prize_extract_biliopus, prize_extract_dyndetail,
)
from Service.GetOthersLotDyn.parser.prize_extractor import PrizeExtractResp
from Service.llm_service import LLMSlotLease

TEST_REF_ID_BASE = 999990000
#: 「不会真的落库」的用例用的 ref_id（走的是错误/跳过分支，不写数据库）
TEST_REF_ID_ASYNC_ERROR = 999991000


# ========================================================================
# SQLite 测试用 SqlHelper
# ========================================================================

class TestSqlHelper(__SqlHelper):
    """SQLite 内存数据库版 SqlHelper，覆盖 MySQL 专用 upsert 为 SQLite 语法"""
    __test__ = False  # 不让 pytest 误认为这是测试类

    async def save_extra_info(
        self, ref_id: int, lot_type: str, is_grand_prize: int = 0,
        is_lot: int | None = None, need_comment: int | None = None,
        need_repost: int | None = None, required_topic_text: str | None = None,
        prize_names: list[str] | None = None, lottery_time: str | None = None,
    ) -> None:
        async with self.async_session() as session:
            insert_values = {
                "ref_id": ref_id, "lot_type": lot_type,
                "is_grand_prize": is_grand_prize,
            }
            update_values = {
                "is_grand_prize": is_grand_prize,
                "predicted_at": text("CURRENT_TIMESTAMP"),
            }
            for key, val in (("is_lot", is_lot), ("need_comment", need_comment),
                             ("need_repost", need_repost), ("required_topic_text", required_topic_text),
                             ("prize_names", prize_names), ("lottery_time", lottery_time)):
                if val is not None:
                    insert_values[key] = val
                    update_values[key] = val

            ins = sqlite_insert(TLotExtraInfo).values(**insert_values)
            stmt = ins.on_conflict_do_update(
                index_elements=[TLotExtraInfo.ref_id, TLotExtraInfo.lot_type],
                set_=update_values,
            )
            await session.execute(stmt)
            await session.commit()


# ========================================================================
# Fixtures
# ========================================================================

@pytest.fixture
async def test_sqlhelper():
    """创建 SQLite 内存数据库，建表，返回 TestSqlHelper 实例"""
    engine = create_async_engine("sqlite+aiosqlite://", echo=False)

    # 手动用 SQLite 兼容 SQL 创建测试需要的表
    ddls = [
        """CREATE TABLE IF NOT EXISTS t_lotdyninfo (
            dynId INTEGER PRIMARY KEY, dynamicUrl TEXT, authorName TEXT,
            up_uid INTEGER, pubTime TIMESTAMP, dynContent TEXT,
            commentCount INTEGER, repostCount INTEGER, likeCount INTEGER,
            officialLotType TEXT, officialLotId TEXT, isOfficialAccount INTEGER,
            dynLotRound_id INTEGER, rawJsonStr TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS t_lot_extra_info (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ref_id INTEGER NOT NULL, lot_type TEXT NOT NULL,
            is_lot INTEGER NOT NULL DEFAULT 0,
            is_grand_prize INTEGER NOT NULL DEFAULT 0,
            need_comment INTEGER NOT NULL DEFAULT 0,
            need_repost INTEGER NOT NULL DEFAULT 0,
            required_topic_text TEXT,
            prize_names TEXT,
            lottery_time TEXT,
            predicted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ref_id, lot_type)
        )""",
    ]

    async with engine.begin() as conn:
        for ddl in ddls:
            await conn.execute(text(ddl))

    # 创建索引
    async with engine.begin() as conn:
        await conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_ref_id_type ON t_lot_extra_info(ref_id, lot_type)"))

    helper = TestSqlHelper.__new__(TestSqlHelper)
    helper.log = __import__("logging").getLogger("test")
    helper.add_dyn_info_lock = asyncio.Lock()
    helper.async_engine = engine
    helper.async_session = async_sessionmaker(engine, expire_on_commit=False)

    yield helper

    await engine.dispose()


@pytest.fixture(autouse=True)
def _patch_sql_helper(test_sqlhelper):
    """自动 patch 所有模块中对 SqlHelper 的引用为 SQLite 版本"""
    targets = [
        "Service.GetOthersLotDyn.Sql.sql_helper.SqlHelper",
        "Service.MQ.base.MQClient.PrizeExtract.SqlHelper",
    ]
    patches = [patch(target, test_sqlhelper) for target in targets]
    for p in patches:
        p.start()
    yield
    for p in patches:
        p.stop()


# ========================================================================
# 测试数据
# ========================================================================

class MockCase:
    def __init__(self, name, target_db, lot_type="common", dyn_content="",
                 lottery_text="", mock_result=None):
        self.name = name
        self.target_db = target_db
        self.lot_type = lot_type
        self.dyn_content = dyn_content
        self.lottery_text = lottery_text
        self.mock_result = mock_result

    @property
    def text_content(self):
        return self.dyn_content or self.lottery_text


_CASES = [
    MockCase(name="普通抽奖 - 有奖品名和开奖时间", target_db="普通抽奖动态", lot_type="common",
             dyn_content="【转发抽奖】关注+转发，抽3位送《原神》648元充值卡！5月1日开奖~ #原神抽奖#",
             mock_result=PrizeExtractResult(
                 prize_names=["原神648元充值卡"], lottery_time="2026-05-01",
                 is_lot=True, need_repost=True, required_topic_text="#原神抽奖#",
                 is_grand_prize=True)),
    MockCase(name="普通动态 - 不是抽奖", target_db="普通抽奖动态", lot_type="common",
             dyn_content="今天天气真好，出去散步了~",
             mock_result=PrizeExtractResult(prize_names=[], lottery_time=None,
                                            is_lot=False, need_repost=False,
                                            required_topic_text="", is_grand_prize=False)),
    MockCase(name="预约抽奖", target_db="普通抽奖动态", lot_type="reserve",
             dyn_content="预约《黑神话：悟空》DLC，抽10位送游戏激活码！",
             mock_result=PrizeExtractResult(prize_names=["游戏激活码"], lottery_time=None,
                                            is_lot=True, need_repost=False,
                                            required_topic_text="", is_grand_prize=True)),
    MockCase(name="官方抽奖 - dyndetail", target_db="官方充电抽奖",
             lottery_text="一等奖 iPhone 15 Pro Max，二等奖 AirPods Pro，三等奖 大会员月卡",
             mock_result=PrizeExtractResult(
                 prize_names=["iPhone 15 Pro Max", "AirPods Pro", "大会员月卡"],
                 lottery_time=None, is_lot=True, need_repost=False,
                 required_topic_text="", is_grand_prize=True)),
    MockCase(name="普通抽奖 - 仅奖品名无开奖时间", target_db="普通抽奖动态", lot_type="common",
             dyn_content="评论抽一位粉丝送手写祝福卡片~评论即可参与！",
             mock_result=PrizeExtractResult(prize_names=["手写祝福卡片"], lottery_time=None,
                                            is_lot=True, need_repost=False,
                                            required_topic_text="", is_grand_prize=False)),
]

BILIOPUS_CASES = [c for c in _CASES if c.target_db == "普通抽奖动态"]


# ========================================================================
# 辅助
# ========================================================================

def _case_to_req(case: MockCase, ref_id: int) -> PrizeExtractParams:
    target_db = PrizeExtractTargetEnum(case.target_db)
    if target_db == PrizeExtractTargetEnum.DYNDETAIL:
        return PrizeExtractParams(
            target_db=target_db, lottery_id=ref_id, lottery_text=case.lottery_text)
    return PrizeExtractParams(
        target_db=target_db, ref_id=ref_id, lot_type=case.lot_type,
        dyn_content=case.dyn_content, comment_count=case.comment_count if hasattr(case, 'comment_count') else None,
        forward_count=case.forward_count if hasattr(case, 'forward_count') else None,
    )


def _fake_lease() -> LLMSlotLease:
    """测试用的槽位租约桩：业务代码只用到 fingerprint 与 llm 两个字段，
    lock / client / notify_key 仅供类型完整（真实释放路径在测试里被 mock 掉）。"""
    return LLMSlotLease(
        fingerprint="test-slot",
        llm=AsyncMock(),
        lock=AsyncMock(),
        client=AsyncMock(),
        notify_key="llm_slot:notify:test-slot",
    )


async def _verify_db(helper: TestSqlHelper, ref_id: int, case: MockCase) -> list[str]:
    diffs = []
    expected = case.mock_result
    extra_info = await helper.get_extra_info_by_ref_id(ref_id, case.lot_type)

    if extra_info:
        if expected.prize_names and extra_info.prize_names != expected.prize_names:
            diffs.append(f"prize_names: DB={extra_info.prize_names} 预期={expected.prize_names}")
        if expected.lottery_time and extra_info.lottery_time != expected.lottery_time:
            diffs.append(f"lottery_time: DB={extra_info.lottery_time} 预期={expected.lottery_time}")
        if extra_info.is_lot != int(expected.is_lot):
            diffs.append(f"is_lot: DB={extra_info.is_lot} 预期={int(expected.is_lot)}")
        if extra_info.is_grand_prize != int(expected.is_grand_prize):
            diffs.append(f"is_grand_prize: DB={extra_info.is_grand_prize} 预期={int(expected.is_grand_prize)}")
        if extra_info.need_repost != int(expected.need_repost):
            diffs.append(f"need_repost: DB={extra_info.need_repost} 预期={int(expected.need_repost)}")
        if (extra_info.required_topic_text or "") != (expected.required_topic_text or ""):
            diffs.append(f"required_topic_text: DB={extra_info.required_topic_text!r} 预期={expected.required_topic_text!r}")
    return diffs


async def _enter_patches(stack: AsyncExitStack, case: MockCase, extra: bool = False):
    ctxs = [
        patch("Service.MQ.base.MQClient.PrizeExtract.extract_prize_info_for_biliopusdb",
              new=AsyncMock(return_value=PrizeExtractResp(
                  dyn_content=case.text_content, consume_time=0.0, result=case.mock_result))),
        patch("Service.MQ.base.MQClient.PrizeExtract.extract_prize_info_for_lotdata",
              new=AsyncMock(return_value=PrizeExtractResp(
                  dyn_content=case.text_content, consume_time=0.0, result=case.mock_result))),
    ]
    if extra:
        ctxs += [
            patch("Service.MQ.base.MQClient.PrizeExtract.prize_extract_redis.acquire_lock",
                  new=AsyncMock(return_value=True)),
            patch("Service.MQ.base.MQClient.PrizeExtract.prize_extract_redis.release_lock",
                  new=AsyncMock()),
            patch("Service.MQ.base.MQClient.PrizeExtract.llm_slot_pool.acquire",
                  new=AsyncMock(return_value=_fake_lease())),
            patch("Service.MQ.base.MQClient.PrizeExtract.llm_slot_pool.release",
                  new=AsyncMock()),
            patch("Service.MQ.base.MQClient.PrizeExtract._already_stored",
                  new=AsyncMock(return_value=False)),
        ]
    for ctx in ctxs:
        stack.enter_context(ctx)


# ========================================================================
# 测试 1: _do_extract_and_store
# ========================================================================

@pytest.mark.asyncio
@pytest.mark.parametrize("case", BILIOPUS_CASES, ids=lambda c: c.name)
async def test_do_extract_and_store(case: MockCase, test_sqlhelper: TestSqlHelper):
    ref_id = TEST_REF_ID_BASE
    req = _case_to_req(case, ref_id)
    # 值传递：从 req 复制出独立的 params 对象交给提取函数
    params = PrizeExtractParams.model_validate(req.model_dump())

    async with AsyncExitStack() as stack:
        await _enter_patches(stack, case)
        result = await _do_extract_and_store(params, _fake_lease())

    assert result is not None
    assert result.is_lot == case.mock_result.is_lot
    assert result.is_grand_prize == case.mock_result.is_grand_prize

    diffs = await _verify_db(test_sqlhelper, ref_id, case)
    assert not diffs, f"数据库验证失败: {diffs}"


# ========================================================================
# 测试 2: process_prize_extract（完整消费链路）
# ========================================================================

@pytest.mark.asyncio
@pytest.mark.parametrize("case", BILIOPUS_CASES, ids=lambda c: c.name)
async def test_process_prize_extract(case: MockCase, test_sqlhelper: TestSqlHelper):
    ref_id = TEST_REF_ID_BASE + len(_CASES)
    req = _case_to_req(case, ref_id)
    mq_props = prize_extract_biliopus.mq_props

    mock_msg = AsyncMock()
    mock_msg.ack = AsyncMock()
    mock_msg.nack = AsyncMock()

    async with AsyncExitStack() as stack:
        await _enter_patches(stack, case, extra=True)
        result = await process_prize_extract(mq_props, req, mock_msg)

    mock_msg.ack.assert_called_once()
    assert result is not None

    diffs = await _verify_db(test_sqlhelper, ref_id, case)
    assert not diffs, f"数据库验证失败: {diffs}"


# ========================================================================
# 测试 3: consumer.consume 入口
# ========================================================================

@pytest.mark.asyncio
@pytest.mark.parametrize("case", BILIOPUS_CASES, ids=lambda c: c.name)
async def test_consumer_consume(case: MockCase, test_sqlhelper: TestSqlHelper):
    ref_id = TEST_REF_ID_BASE + len(_CASES) * 2
    req = _case_to_req(case, ref_id)
    consumer = prize_extract_biliopus

    mock_msg = AsyncMock()
    mock_msg.ack = AsyncMock()
    mock_msg.nack = AsyncMock()

    async with AsyncExitStack() as stack:
        await _enter_patches(stack, case, extra=True)
        await consumer.consume(req, mock_msg)

    mock_msg.ack.assert_called_once()

    diffs = await _verify_db(test_sqlhelper, ref_id, case)
    assert not diffs, f"数据库验证失败: {diffs}"


# ========================================================================
# 测试 4: FastStream TestRabbitBroker 内存路由
# ========================================================================

@pytest.mark.asyncio
@pytest.mark.parametrize("case", BILIOPUS_CASES, ids=lambda c: c.name)
async def test_handler_inmemory(case: MockCase, test_sqlhelper: TestSqlHelper):
    """使用 FastStream TestRabbitBroker 内存路由测试完整 handler 调用链。
    注意: TestRabbitBroker 在内存模式下不注入 RabbitMessage，需要 handler 内部构造 mock。"""
    from faststream.rabbit import TestRabbitBroker, RabbitBroker
    from Models.MQ.BaseMQModel import QueueName

    broker = RabbitBroker()

    @broker.subscriber(queue=QueueName.PrizeExtractBiliOpusMQ)
    async def _handle_biliopus(body: dict):
        # TestRabbitBroker 内存模式下 fast_depends 只传 raw dict，不做 Pydantic 解析
        params = PrizeExtractParams(**body)
        mock_msg = AsyncMock()
        mock_msg.ack = AsyncMock()
        mock_msg.nack = AsyncMock()
        await prize_extract_biliopus.consume(params, mock_msg)

    ref_id = TEST_REF_ID_BASE + len(_CASES) * 3
    req = _case_to_req(case, ref_id)

    async with TestRabbitBroker(broker) as br, AsyncExitStack() as stack:
        await _enter_patches(stack, case, extra=True)
        await br.publish(req.model_dump(mode="json"), queue=QueueName.PrizeExtractBiliOpusMQ)

    # TestRabbitBroker 内存模式下本地函数无 .mock 属性，改验证数据库
    diffs = await _verify_db(test_sqlhelper, ref_id, case)
    assert not diffs, f"数据库验证失败: {diffs}"


# ========================================================================
# 测试 5: 已存在记录时跳过
# ========================================================================

@pytest.mark.asyncio
async def test_already_stored_skips(test_sqlhelper: TestSqlHelper):
    case = BILIOPUS_CASES[0]
    ref_id = TEST_REF_ID_BASE + len(_CASES) * 4
    req = _case_to_req(case, ref_id)
    mq_props = prize_extract_biliopus.mq_props

    mock_msg = AsyncMock()
    mock_msg.ack = AsyncMock()
    mock_msg.nack = AsyncMock()

    with patch("Service.MQ.base.MQClient.PrizeExtract.prize_extract_redis.acquire_lock",
               new=AsyncMock(return_value=True)), \
         patch("Service.MQ.base.MQClient.PrizeExtract.prize_extract_redis.release_lock",
               new=AsyncMock()), \
         patch("Service.MQ.base.MQClient.PrizeExtract._already_stored",
               new=AsyncMock(return_value=True)):
        result = await process_prize_extract(mq_props, req, mock_msg)

    mock_msg.ack.assert_called_once()
    assert result is None


# ========================================================================
# 测试 6: 消费任务被取消时仍释放 redis 去重锁与槽位租约（取消安全）
# ========================================================================

@pytest.mark.asyncio
async def test_cancelled_still_releases_resources(test_sqlhelper: TestSqlHelper):
    """取消恰好落在「释放资源」过程中时（真实日志里的典型形态：连接拆除导致
    在途任务被批量 cancel，CancelledError 被投递到 release 的 await 上），
    释放动作必须仍然跑完，不能留下泄漏的去重锁/槽位租约。"""
    case = BILIOPUS_CASES[0]
    ref_id = TEST_REF_ID_BASE + len(_CASES) * 5
    req = _case_to_req(case, ref_id)
    mq_props = prize_extract_biliopus.mq_props

    mock_msg = AsyncMock()
    mock_msg.ack = AsyncMock()
    mock_msg.nack = AsyncMock()

    release_started = asyncio.Event()

    async def _slow_release_slot(*args, **kwargs):
        # 模拟 redis 往返：取消会在这个 await 上被投递
        release_started.set()
        await asyncio.sleep(0.2)

    release_lock = AsyncMock()

    async with AsyncExitStack() as stack:
        await _enter_patches(stack, case, extra=True)
        stack.enter_context(patch(
            "Service.MQ.base.MQClient.PrizeExtract.llm_slot_pool.release",
            new=_slow_release_slot))
        stack.enter_context(patch(
            "Service.MQ.base.MQClient.PrizeExtract.prize_extract_redis.release_lock",
            new=release_lock))

        task = asyncio.create_task(process_prize_extract(mq_props, req, mock_msg))
        await asyncio.wait_for(release_started.wait(), timeout=5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        # 释放动作由 shield 托管为独立任务，给它跑完的时间
        await asyncio.sleep(0.4)

    release_lock.assert_awaited_once()
    mock_msg.ack.assert_called_once()  # 处理已完成并 ack，只是清理阶段被取消
    mock_msg.nack.assert_not_called()


# ========================================================================
# 测试 7: 本轮失败既不能 ack 也不能 nack，由外层继续下一轮
# ========================================================================

@pytest.mark.asyncio
async def test_extract_failure_never_acks(test_sqlhelper: TestSqlHelper):
    """单轮失败时**既不能 ack 也不能 nack**（ack 会丢消息，nack 重投的副本又可能
    撞上尚未释放的去重锁被丢弃），并且消息由外层循环继续重试。

    确定性断言：把「重试耗尽」压成一次（max_tries=1）并替换掉告警回调，
    不做任何等待、不依赖取消。
    """
    case = BILIOPUS_CASES[0]
    ref_id = TEST_REF_ID_ASYNC_ERROR
    params = _case_to_req(case, ref_id)
    mq_props = prize_extract_biliopus.mq_props

    mock_msg = AsyncMock()
    mock_msg.ack = AsyncMock()
    mock_msg.nack = AsyncMock()

    async def _always_fail(*args, **kwargs):
        raise RuntimeError("模拟大模型调用失败")

    giveup = AsyncMock()

    async with AsyncExitStack() as stack:
        await _enter_patches(stack, case, extra=True)
        stack.enter_context(patch(
            "Service.MQ.base.MQClient.PrizeExtract._do_extract_and_store",
            new=_always_fail))
        # 让 run_with_backoff 一次即放弃（避免 6s 起的真实退避），并由 mock 承接 on_giveup
        stack.enter_context(patch(
            "Service.MQ.base.MQClient.PrizeExtract.build_consume_backoff_config",
            new=lambda **kwargs: BackoffConfig(
                max_tries=1, on_giveup=giveup, jitter=None)))

        # 单轮：失败必须抛出，且不得确认消息；槽位与去重锁仍要归还
        with pytest.raises(RuntimeError):
            await _consume_once(mq_props, params, mock_msg)

    mock_msg.ack.assert_not_called()
    mock_msg.nack.assert_not_called()
    giveup.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_prize_extract_retries_next_round(test_sqlhelper: TestSqlHelper):
    """外层循环：本轮失败后继续下一轮，直到成功才 ack，全程不 nack。"""
    case = BILIOPUS_CASES[0]
    ref_id = TEST_REF_ID_ASYNC_ERROR + 1
    params = _case_to_req(case, ref_id)
    mq_props = prize_extract_biliopus.mq_props

    mock_msg = AsyncMock()
    mock_msg.ack = AsyncMock()
    mock_msg.nack = AsyncMock()

    async def _fail_twice_then_ok(*args, **kwargs):
        _fail_twice_then_ok.calls += 1
        if _fail_twice_then_ok.calls <= 2:
            raise RuntimeError("模拟本轮失败")
        await mock_msg.ack()
        return "本轮成功"

    _fail_twice_then_ok.calls = 0

    with patch(
        "Service.MQ.base.MQClient.PrizeExtract._consume_once",
        new=_fail_twice_then_ok,
    ):
        result = await process_prize_extract(mq_props, params, mock_msg)

    assert result == "本轮成功"
    assert _fail_twice_then_ok.calls == 3, "失败两轮后应继续到第三轮"
    mock_msg.ack.assert_awaited_once()
    mock_msg.nack.assert_not_called()


# ========================================================================
# 测试 8: 去重锁被其他副本占用时不得 ack，重试到抢到为止
# ========================================================================

@pytest.mark.asyncio
async def test_lock_held_by_other_copy_never_acks(
    test_sqlhelper: TestSqlHelper, monkeypatch
):
    """重复副本抢不到去重锁时不能 ack 丢弃（旧实现的丢数据窗口），
    而是重试抢锁，抢到后按「库里已有」正常结束。

    确定性断言：acquire_lock 前两次返回 False、第三次成功，并把退避下限调到 0，
    全程无真实等待。
    """
    case = BILIOPUS_CASES[0]
    ref_id = TEST_REF_ID_ASYNC_ERROR + 2
    params = _case_to_req(case, ref_id)
    mq_props = prize_extract_biliopus.mq_props

    mock_msg = AsyncMock()
    mock_msg.ack = AsyncMock()
    mock_msg.nack = AsyncMock()

    # 退避下限临时调小，测试里不真等 1s
    monkeypatch.setattr(prize_extract_module, "LOCK_RETRY_MIN_WAIT", 0.0)
    acquire_lock = AsyncMock(side_effect=[False, False, True])

    async with AsyncExitStack() as stack:
        await _enter_patches(stack, case, extra=True)
        stack.enter_context(patch(
            "Service.MQ.base.MQClient.PrizeExtract.prize_extract_redis.acquire_lock",
            new=acquire_lock))
        # 抢到锁后「库里已有提取信息」→ 正常 ack 结束
        stack.enter_context(patch(
            "Service.MQ.base.MQClient.PrizeExtract._already_stored",
            new=AsyncMock(return_value=True)))

        result = await _consume_once(mq_props, params, mock_msg)

    assert result is None
    assert acquire_lock.await_count == 3, "抢不到锁时应持续重试"
    mock_msg.ack.assert_awaited_once()
    mock_msg.nack.assert_not_called()
