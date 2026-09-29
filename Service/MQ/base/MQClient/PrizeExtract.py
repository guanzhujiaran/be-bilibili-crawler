"""入库消息队列消费者（按目标数据库分队列）。

设计目标：
- 两条队列（biliopusdb / dyndetail）各自可被独立消费、独立扩缩容，
  但「大模型提取 + 写库」的处理逻辑共享同一套：PrizeExtractConsumer.consume
  → process_prize_extract(params)。
- 处理流程（每轮）：
  1. redis 去重锁：该记录是否正被其他副本处理；
  2. 直接查目标数据库是否已存在提取信息（不判断「最近」）；
  3. 抢一个可用的 LLM 槽位（每个 (base_url, model, token) 跨进程最多 1 个在途请求）；
  4. 不存在 → 用该槽位调用大模型提取并写库。
- **ack / nack 语义（不丢消息）**：
  只有「确认库里已有提取结果」与「本轮成功写库」两处会 ack；其余情况（抢不到去重锁、
  没有可用槽位、提取或写库失败）一律保持消息未确认，在本机退避后继续下一轮。
  进程崩溃时未确认消息由 broker 重投，持久化与重投全部交给 RabbitMQ。
  反之，「什么都没做就 ack」会真的丢数据：重复副本一旦撞上尚未释放的去重锁被 ack 丢弃，
  而原副本随后又失败，这条消息就再也没人处理了（旧实现的「并发已满重新入队 + ack」
  与「nack 后 finally 才释放锁」两条路径都存在这个窗口）。

  **唯一的主动 nack 例外**：RabbitMQ 有 ``consumer_timeout``（默认 30 分钟），
  未确认超时会被 broker 强制关闭 channel 并重投（表现为「莫名重投 + 重复消费」，
  且 channel 已关、ack 也会失败）。因此每条消息都带一个「确认超时预算」
  （见 :mod:`Service.MQ.base.MQClient.consume_budget`）：所有等待按预算截断，
  预算耗尽时在**释放完锁之后**主动 ``nack(requeue=True)`` 交还队列，
  由下一个消费者以新预算重新开始。重投的幂等性由去重锁 + 写库 upsert 保证。
- 具体落库目标由消息体里的自定义参数类 PrizeExtractParams.target_db 决定。
"""

import asyncio
import time

from bili_common.core.backoff import equal_jitter, expo_wait, run_with_backoff
from bili_common.models import StrEnumAutoDoc
from faststream.rabbit.fastapi import RabbitMessage

from CONFIG import CONFIG, settings
from Models.MQ.PrizeExtractMQModel import (
    PrizeExtractParams,
    PrizeExtractTargetEnum,
)
from Models.MQ.PrizeExtractResult import (
    OfficialPrizeExtractResult,
    PrizeExtractResult,
)
from Service.GetOthersLotDyn.Sql.sql_helper import SqlHelper
from Service.GetOthersLotDyn.parser.prize_extractor import (
    PrizeExtractResp,
    extract_prize_info_for_biliopusdb,
    extract_prize_info_for_lotdata,
)
from Service.GrpcModule.GrpcSrc.SQLObject.DynDetailSqlHelperMysqlVer import (
    grpc_sql_helper,
)
from Service.MQ.base.MQClient.base import (
    BaseFastStreamMQ,
    prize_extract_biliopus_mq_prop,
    prize_extract_dyndetail_mq_prop,
)
from Service.MQ.base.MQClient.consume_backoff import build_consume_backoff_config
from Service.MQ.base.MQClient.consume_budget import (
    ConsumeBudget,
    ConsumeBudgetExceeded,
)
from Service.llm_service import LLMSlotLease, LLMSlotWaitTimeout, llm_slot_pool
from Utils.redisTool.RedisManager import RedisManagerBase, redis_client_factory
from log.base_log import MQ_logger

# ============ 去重锁相关常量 ============
LOCK_TTL = 600  # 秒：去重锁兜底过期（持有者崩溃后由此兜底，避免记录被永久锁死）
LOCK_RETRY_MAX_BACKOFF = 30.0  # 秒：抢不到去重锁时本机退避的等待上限
LOCK_RETRY_MIN_WAIT = 1.0  # 秒：退避下限（抖动可能给出接近 0 的值，避免空转）

#: 抢不到去重锁时的等待序列：指数退避 + 抖动，封顶 LOCK_RETRY_MAX_BACKOFF
_lock_wait = expo_wait(base=2.0, factor=1.0, max_value=LOCK_RETRY_MAX_BACKOFF)

#: 秒：预算耗尽时留给「释放锁 + nack + 日志」的收尾余量（配置项，见 CONFIG.settings）
ACK_RESERVE = max(1.0, settings.mq_consume_ack_reserve)

#: 秒：单条消息自「收到」起算的确认超时预算 = broker consumer_timeout - 收尾余量
ACK_BUDGET_SECONDS = max(1.0, settings.mq_consume_ack_timeout - ACK_RESERVE)


# ============ redis 去重锁 ============
class PrizeExtractRedisManager(RedisManagerBase):
    class RedisMap(StrEnumAutoDoc):
        lock_prefix = "prize_extract:lock"

    def __init__(self):
        super().__init__(
            host=CONFIG.database.getOtherLotRedis.host,
            port=CONFIG.database.getOtherLotRedis.port,
            db=CONFIG.database.getOtherLotRedis.db,
        )

    async def acquire_lock(self, key: str, ttl: int = LOCK_TTL) -> bool:
        """原子获取锁（SET NX EX）。返回 True 表示抢到锁。"""
        lock_key = f"{self.RedisMap.lock_prefix.value}:{key}"
        async with redis_client_factory(pool=self.pool) as r:
            return bool(await r.set(lock_key, "1", nx=True, ex=ttl))

    async def release_lock(self, key: str) -> None:
        lock_key = f"{self.RedisMap.lock_prefix.value}:{key}"
        async with redis_client_factory(pool=self.pool) as r:
            await r.delete(lock_key)


prize_extract_redis = PrizeExtractRedisManager()

# 在途的「资源释放」任务强引用集合：release 动作被 shield 托管为独立任务后，
# 不能依赖局部变量保活（asyncio 对任务只持弱引用），否则可能被 GC 掉导致锁泄漏。
_pending_release_tasks: set[asyncio.Task] = set()


async def _release_resources(lease: LLMSlotLease | None, lock_key: str) -> None:
    """释放槽位租约（仅在持有租约时）与 redis 去重锁。"""
    if lease is not None:
        await llm_slot_pool.release(lease)
    await prize_extract_redis.release_lock(lock_key)


# region 共享处理核心（两队列共用，按 params.target_db 分支落库）
def _lock_key(params: PrizeExtractParams) -> str:
    if params.target_db == PrizeExtractTargetEnum.DYNDETAIL:
        return f"dyndetail:{params.lottery_id}"
    return f"biliopusdb:{params.ref_id}:{params.lot_type}"


async def _wait_own_lock(module_name: str, lock_key: str, budget: ConsumeBudget) -> None:
    """一直重试到抢到去重锁为止（同一条记录可能正被另一个副本处理）。

    抢不到时本机退避重试，**不 ack、不 nack**：重复副本的正确归宿是「处理」或
    「确认库里已有」，直接 ack 丢弃会在原副本随后失败时真的丢消息。
    持有者若崩溃，去重锁会在 LOCK_TTL 后自动过期，这里自然能抢到。

    等待按 :class:`ConsumeBudget` 的剩余预算截断；预算耗尽抛
    :class:`ConsumeBudgetExceeded`，由上层把消息交还队列重投
    （持有者崩溃后重投副本才可能抢到锁，不能无限占着未确认消息等）。
    """
    attempt = 0
    while not await prize_extract_redis.acquire_lock(lock_key, LOCK_TTL):
        if budget.expired():
            raise ConsumeBudgetExceeded(
                f"等待去重锁 {lock_key} 超出消息确认预算（{budget.describe()}）"
            )
        attempt += 1
        wait = max(LOCK_RETRY_MIN_WAIT, equal_jitter(_lock_wait(attempt)))
        wait = budget.bound(wait)
        MQ_logger.info(
            f"【{module_name}】{lock_key} 正在被其他副本处理，{wait:.1f}s 后重试"
            f"（第 {attempt} 次，消息保持未确认，{budget.describe()}）"
        )
        await asyncio.sleep(wait)


async def _already_stored(params: PrizeExtractParams) -> bool:
    """直接查对应数据库是否已存在该记录的提取信息（不判断「最近」）。"""
    if params.target_db == PrizeExtractTargetEnum.DYNDETAIL:
        lottery_id = params.lottery_id
        if lottery_id is None:
            return False
        return await grpc_sql_helper.is_extra_info_exists(lottery_id=lottery_id)
    ref_id = params.ref_id
    if ref_id is None:
        return False
    return await SqlHelper.is_extra_info_exists(ref_id=ref_id, lot_type=params.lot_type)


async def _do_extract_and_store(
    params: PrizeExtractParams,
    lease: LLMSlotLease,
    budget: ConsumeBudget,
) -> PrizeExtractResult | OfficialPrizeExtractResult:
    """用租约锁定的槽位调用大模型提取并把结果写库，返回 result（值）。

    具体提取函数与落库目标由 params.target_db 决定；LLM 调用一律使用 ``lease.llm``
    —— 锁定哪个槽位就用哪个槽位，否则「锁着 A 槽位、请求打到 B」会让槽位锁形同虚设。

    ``budget`` 的剩余时间透传给提取流程，作为允许阻塞（等待槽位冷却恢复）的上界。
    """
    max_wait = budget.remaining()
    if params.target_db == PrizeExtractTargetEnum.DYNDETAIL:
        lottery_id = params.lottery_id
        if lottery_id is None:
            MQ_logger.warning(f"【dyndetail】缺少 lottery_id，跳过提取: {params}")
            return PrizeExtractResult()
        result: PrizeExtractResp[OfficialPrizeExtractResult] = (
            await extract_prize_info_for_lotdata(
                dyn_content=params.lottery_text,
                chat_openai_client=lease.llm,
                max_wait=max_wait,
            )
        )
        await grpc_sql_helper.save_extra_info(
            lottery_id=lottery_id,
            is_grand_prize=int(result.result.is_grand_prize),
        )
        return result.result
    else:
        ref_id = params.ref_id
        if ref_id is None:
            MQ_logger.warning(f"【biliopusdb】缺少 ref_id，跳过提取: {params}")
            return PrizeExtractResult()
        result = await extract_prize_info_for_biliopusdb(
            dyn_content=params.dyn_content,
            dyn_publish_time=params.dyn_publish_time,
            chat_openai_client=lease.llm,
            max_wait=max_wait,
        )
        r = result.result
        # is_lot 判断逻辑（从 judge_lottery 移入）：
        # LLM 判断为抽奖，或互动量超阈值（评论>2000 或 转发>1000）也算抽奖
        comment_count = params.comment_count or 0
        forward_count = params.forward_count or 0
        r.is_lot = r.is_lot or (comment_count > 2000 or forward_count > 1000)

        # 所有 LLM 提取信息（含 prize_names / lottery_time）统一保存到 t_lot_extra_info，
        # 不再使用独立的 t_others_lot_info 表（is_lot 不存 t_lotdyninfo，只存 t_lot_extra_info）
        await SqlHelper.save_extra_info(
            ref_id=ref_id,
            lot_type=params.lot_type,
            is_lot=int(r.is_lot),
            is_grand_prize=int(r.is_grand_prize),
            need_repost=int(r.need_repost),
            need_comment=params.need_comment,
            required_topic_text=(
                r.required_topic_text if r.required_topic_text else None
            ),
            prize_names=r.prize_names,
            lottery_time=r.lottery_time,
        )
        return r


async def _consume_once(
    mq_props, params: PrizeExtractParams, msg: RabbitMessage, budget: ConsumeBudget
) -> PrizeExtractResult | OfficialPrizeExtractResult | None:
    """执行一轮处理：抢去重锁 → 查库 → 抢槽位 → 提取写库 → ack。

    - 返回值：提取结果（本轮已成功写库并 ack）；``None`` 表示确认「库里已有提取信息」
      并 ack；
    - 抛出的异常表示「本轮没干成」，由 :func:`process_prize_extract` 决定继续下一轮
      或把消息交还队列；调用方**不会** ack/nack 这条消息。

    所有等待（抢锁 / 抢槽位 / 重试）都按 ``budget`` 的剩余时间截断，
    保证未确认时间不会超过 broker 的 ``consumer_timeout``。
    """
    module_name = mq_props.queue_name
    lock_key = _lock_key(params)
    lease: LLMSlotLease | None = None
    try:
        # 1) redis 锁：同一条记录正被其他副本处理时，本机等它结束（不是丢弃）
        await _wait_own_lock(module_name, lock_key, budget)

        # 2) 直接查对应数据库是否已存在提取信息 → 数据确实在库里，消息使命完成
        if await _already_stored(params):
            MQ_logger.info(f"【{module_name}】{lock_key} 已存在提取信息，跳过")
            await msg.ack()
            return None

        # 3) 抢一个 LLM 槽位：没有可用槽位时在本机等（受预算约束，超时交还消息重投）
        lease = await llm_slot_pool.acquire(max_wait=budget.remaining())
        MQ_logger.info(
            f"【{module_name}】{lock_key} 占用 LLM 槽位 {lease.fingerprint}"
        )

        # 4) 用该槽位调用大模型提取并写库。
        #    失败不立即 ack/nack：先在进程内按「指数等待 + 抖动」重试（见
        #    bili_common.core.backoff / consume_backoff），一轮耗尽后由外层继续下一轮；
        #    槽位不可用（冷却 / 已删除）会立刻放弃本轮，由外层换槽位或交还消息。
        result = await run_with_backoff(
            lambda: _do_extract_and_store(params, lease, budget),
            config=build_consume_backoff_config(
                module_name=module_name,
                params=params,
                max_time_limit=budget.remaining(),
            ),
        )

        MQ_logger.info(f"【{module_name}】{lock_key} 提取并入库完成: {result}")
        await msg.ack()
        return result
    finally:
        # 释放动作必须「不可取消」：连接拆除 / 任务取消时 CancelledError 可能恰好
        # 投递在 release 的 await 上，导致去重锁与槽位租约泄漏（后续同 key 消息长期
        # 被「正在处理中」跳过）。这里托管为独立任务并用 shield 等待，
        # 保证释放动作跑完（本任务自身的取消仍会正常向上传播）。
        release_task = asyncio.create_task(
            _release_resources(lease=lease, lock_key=lock_key)
        )
        # asyncio 只对运行中的任务持弱引用，登记一份强引用避免被 GC。
        _pending_release_tasks.add(release_task)
        release_task.add_done_callback(_pending_release_tasks.discard)
        await asyncio.shield(release_task)


async def _requeue_for_budget(
    budget: ConsumeBudget, msg: RabbitMessage, reason: str
) -> None:
    """预算耗尽时主动把消息交还队列（``nack(requeue=True)``）。

    为什么必须「主动」：RabbitMQ 的 ``consumer_timeout``（默认 30 分钟）到点会
    **强制关闭 channel 并把消息重投**，那时 channel 已关、ack/nack 都会失败，
    行为完全不可控。抢在它之前主动交还，至少是可控的，且此时去重锁与槽位租约
    都已由 ``_consume_once`` 的 ``finally`` 释放（``nack`` 之前不存在锁泄漏）。

    交还前加一点随机抖动，避免同一批消息在同一刻集体重投形成热点。
    """
    MQ_logger.warning(
        f"【{budget.queue_name}】{budget.label} 本轮已用尽消息确认预算，"
        f"主动交还队列重投（避免被 broker 强制 nack）：{reason}；{budget.describe()}"
    )
    await budget.requeue_jitter()
    try:
        await msg.nack(requeue=True)
    except Exception as e:  # noqa: BLE001 - nack 失败时消息仍未确认，broker 会重投，不丢
        MQ_logger.error(
            f"【{budget.queue_name}】{budget.label} 交还队列（nack requeue=True）失败："
            f"{type(e).__name__}: {e}；消息仍处于未确认状态，将由 broker 重投"
        )


async def process_prize_extract(
    mq_props, params: PrizeExtractParams, msg: RabbitMessage
) -> PrizeExtractResult | OfficialPrizeExtractResult | None:
    """两队列共享的处理流程：本轮没成就本机继续下一轮，直到成功或**预算耗尽**。

    ack 只发生在「确认库里已有」与「本轮成功写库」两处；其余情况消息保持未确认，
    由本机退避重试（崩溃时由 broker 重投未确认消息），因此不会丢数据。

    唯一的例外是「消息确认超时预算」耗尽（见 :mod:`..consume_budget`）：
    此时主动 ``nack(requeue=True)`` 把消息交还队列，由下一个消费者以新预算重新开始 ——
    否则未确认时间超过 broker 的 ``consumer_timeout`` 会被强制 nack，
    变成了不可控的重投 + 重复消费。重投的幂等由去重锁 + 写库 upsert 保证。
    """
    module_name = mq_props.queue_name
    # 值传递：从入参复制出独立的 params 对象，避免直接引用共享入参的内部状态
    params = PrizeExtractParams.model_validate(params.model_dump())
    # 预算自「收到消息」起算：所有等待都按它截断，保证不会被 broker 强制 nack
    budget = ConsumeBudget.start(
        total_seconds=ACK_BUDGET_SECONDS,
        queue_name=module_name,
        label=_lock_key(params),
    )
    while True:
        # 每轮开始先看预算：不足则不再开始新一轮（避免干到一半被 broker 掐断）
        if budget.expired():
            await _requeue_for_budget(budget, msg, "开始新一轮前预算已耗尽")
            return None
        try:
            return await _consume_once(mq_props, params, msg, budget)
        except asyncio.CancelledError:
            # 取消必须向上传播（连接拆除 / 服务关停），不能当成普通失败吞掉
            raise
        except (ConsumeBudgetExceeded, LLMSlotWaitTimeout) as e:
            # 等待类操作主动报告预算耗尽：交还消息，由下一个消费者重新开始
            await _requeue_for_budget(budget, msg, f"{type(e).__name__}: {e}")
            return None
        except Exception as e:
            MQ_logger.warning(
                f"【{module_name}】{_lock_key(params)} 本轮处理失败，"
                f"消息保持未确认并继续下一轮：{type(e).__name__}: {e}；"
                f"{budget.describe()}"
            )


# endregion


class PrizeExtractConsumer(BaseFastStreamMQ):
    """入库消息队列消费者（可按目标数据库实例化多条）。

    处理逻辑（大模型返回数据处理）全部在 process_prize_extract 中共享，
    本类仅负责绑定队列与消息体的反序列化。
    """

    def __init__(self, mq_props):
        super().__init__(mq_props=mq_props)

    async def consume(self, body: PrizeExtractParams, msg: RabbitMessage):
        MQ_logger.debug(f"【{self.mq_props.queue_name}】消费消息: {body}")
        await process_prize_extract(self.mq_props, body, msg)


# 两条队列各自一个消费者实例，但共用同一套处理逻辑
prize_extract_biliopus = PrizeExtractConsumer(prize_extract_biliopus_mq_prop)
prize_extract_dyndetail = PrizeExtractConsumer(prize_extract_dyndetail_mq_prop)

__all__ = [
    "prize_extract_biliopus",
    "prize_extract_dyndetail",
    "PrizeExtractConsumer",
    "process_prize_extract",
]
