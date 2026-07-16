"""入库消息队列消费者（按目标数据库分队列）。

设计目标：
- 两条队列（biliopusdb / dyndetail）各自可被独立消费、独立扩缩容，
  但「大模型提取 + 写库」的处理逻辑共享同一套：PrizeExtractConsumer.consume
  → process_prize_extract(req)。
- 处理流程（处理大模型的返回数据，统一一套）：
  1. redis 去重锁：该记录是否正在查询/处理；
  2. 直接查目标数据库是否已存在提取信息（不判断「最近」）；
  3. 全局 redis 信号量：限制同时调用大模型的并发数；
  4. 不存在 → 调用大模型提取并写库；存在或并发满 → 跳过/重新入队。
- 具体落库目标由消息体里的自定义参数类 PrizeExtractParams.target_db 决定。
"""
import asyncio
import time
import traceback
from datetime import datetime
from enum import StrEnum

from faststream.rabbit.fastapi import RabbitMessage
from log.base_log import MQ_logger
from Utils.redisTool.RedisManager import RedisManagerBase, redis_client_factory
from Utils.推送.PushMe import a_push_error
from CONFIG import CONFIG

from Service.MQ.base.MQClient.base import (
    BaseFastStreamMQ,
    prize_extract_biliopus_mq_prop,
    prize_extract_dyndetail_mq_prop,
)
from Service.MQ.base.MQClient.BiliLotDataPublisher import BiliLotDataPublisher
from Models.MQ.PrizeExtractMQModel import (
    PrizeExtractReq,
    PrizeExtractParams,
    PrizeExtractTargetEnum,
)
from Service.GetOthersLotDyn.parser.prize_extractor import (
    extract_prize_info_for_biliopusdb,
    extract_prize_info_for_dyndetail,
)
from Service.GetOthersLotDyn.Sql.sql_helper import SqlHelper
from Service.GrpcModule.GrpcSrc.SQLObject.DynDetailSqlHelperMysqlVer import grpc_sql_helper


# ============ 异常兜底（与 BiliLotDataFastStream.handle_exception 一致）============
async def handle_exception(
        module_name: str,
        e: Exception,
        params,
        msg: RabbitMessage
):
    error_msg = f"[ERROR]队列:{module_name}\n异常类型:{type(e)}\n异常:{e}\n时间:{time.strftime('%Y-%m-%d %H:%M:%S')}\n参数:{params}"
    MQ_logger.exception(error_msg)
    await a_push_error(
        subject="运行异常",
        content=f"抽奖MQ错误 - {module_name} - {e}\n{error_msg}",
    )
    await msg.nack()


# ============ 并发/去重相关常量（两队列共享同一把全局信号量）============
LOCK_TTL = 600           # 秒：去重锁兜底过期
MAX_CONCURRENCY = 10     # 全局大模型并发上限
SEM_TTL = 600            # 秒：信号量 key 过期（崩溃自愈）
SEM_ACQUIRE_TIMEOUT = 60.0  # 秒：阻塞等待信号量超时
SEM_KEY = "global"       # 全局唯一 key，所有提取共享同一把并发闸


# ============ redis 锁 + 信号量 ============
class PrizeExtractRedisManager(RedisManagerBase):
    class RedisMap(StrEnum):
        lock_prefix = "prize_extract:lock"
        sem_prefix = "prize_extract:sem"

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

    # region 分布式信号量：限制大模型提取的并发数（跨所有消费者实例）
    # Lua：当前计数 < max 才 INCR，否则返回 0。首次 INCR 时设置过期，
    # 防止消费者崩溃导致计数永不回落（TTL 后 key 自动删除 → 计数归零）。
    _SEM_ACQUIRE_SCRIPT = """
    local cur = redis.call('GET', KEYS[1])
    if cur and tonumber(cur) >= tonumber(ARGV[1]) then
        return 0
    end
    local newv = redis.call('INCR', KEYS[1])
    if newv == 1 then
        redis.call('EXPIRE', KEYS[1], ARGV[2])
    end
    return 1
    """

    async def acquire_semaphore(
        self, key: str, max_concurrency: int, ttl: int
    ) -> bool:
        """原子地尝试获取一个信号量名额。返回 True 表示成功占用一个并发位。"""
        sem_key = f"{self.RedisMap.sem_prefix.value}:{key}"
        async with redis_client_factory(pool=self.pool) as r:
            res = await r.eval(
                self._SEM_ACQUIRE_SCRIPT, 1, sem_key, max_concurrency, ttl
            )
            return bool(res)

    async def release_semaphore(self, key: str) -> None:
        """释放一个信号量名额（DECR，绝不为负；归零则删除 key）。"""
        sem_key = f"{self.RedisMap.sem_prefix.value}:{key}"
        async with redis_client_factory(pool=self.pool) as r:
            cur = await r.decr(sem_key)
            if cur <= 0:
                await r.delete(sem_key)

    async def acquire_semaphore_blocking(
        self,
        key: str,
        max_concurrency: int,
        ttl: int,
        timeout: float = SEM_ACQUIRE_TIMEOUT,
    ) -> bool:
        """带超时的阻塞获取信号量。

        在 timeout 内以轮询方式尝试获取名额；超时仍未拿到则返回 False，
        由调用方决定（重新入队 / 延后处理）。
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if await self.acquire_semaphore(key, max_concurrency, ttl):
                return True
            await asyncio.sleep(0.2)
        return False
    # endregion


prize_extract_redis = PrizeExtractRedisManager()


# region 共享处理核心（两队列共用，按 params.target_db 分支落库）
def _lock_key(params: PrizeExtractParams) -> str:
    if params.target_db == PrizeExtractTargetEnum.DYNDETAIL:
        return f"dyndetail:{params.lottery_id}"
    return f"biliopusdb:{params.ref_id}:{params.lot_type}"


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


async def _do_extract_and_store(req: PrizeExtractReq) -> None:
    """调用大模型提取并把结果写库；成功后把 result 回填到 req。

    具体提取函数与落库目标由 params.target_db 决定。
    """
    params = req.params
    if params.target_db == PrizeExtractTargetEnum.DYNDETAIL:
        lottery_id = params.lottery_id
        if lottery_id is None:
            MQ_logger.warning(f"【dyndetail】缺少 lottery_id，跳过提取: {params}")
            return
        result = await extract_prize_info_for_dyndetail(
            dyn_content=params.lottery_text)
        await grpc_sql_helper.save_extra_info(
            lottery_id=lottery_id,
            is_grand_prize=int(result.result.is_grand_prize),
        )
        req.result = result.result
    else:
        ref_id = params.ref_id
        if ref_id is None:
            MQ_logger.warning(f"【biliopusdb】缺少 ref_id，跳过提取: {params}")
            return
        result = await extract_prize_info_for_biliopusdb(
            dyn_content=params.dyn_content,
            dyn_publish_time=params.dyn_publish_time,
        )
        r = result.result
        if r.prize_names or r.lottery_time:
            await SqlHelper.save_prize(
                dyn_id=ref_id,
                prize_names=r.prize_names,
                lottery_time=r.lottery_time,
            )
        if r.is_grand_prize or r.need_repost or r.required_topic_text:
            await SqlHelper.save_extra_info(
                ref_id=ref_id,
                lot_type=params.lot_type,
                is_grand_prize=int(r.is_grand_prize),
                need_repost=int(r.need_repost),
                need_comment=params.need_comment,
            )
        req.result = r


async def process_prize_extract(
        mq_props, req: PrizeExtractReq, msg: RabbitMessage
) -> None:
    """两队列共享的处理流程：去重锁 → 查库 → 信号量 → 大模型提取写库。

    mq_props 用于「并发已满」时把消息重新入队回原队列。
    """
    module_name = mq_props.queue_name
    params = req.params
    lock_key = _lock_key(params)
    sem_acquired = False
    try:
        # 1) redis 锁：正在查询/处理则跳过
        if not await prize_extract_redis.acquire_lock(lock_key, LOCK_TTL):
            MQ_logger.info(
                f"【{module_name}】{lock_key} 正在查询/处理中，跳过")
            return await msg.ack()

        # 2) 直接查对应数据库是否已存在提取信息
        if await _already_stored(params):
            MQ_logger.info(
                f"【{module_name}】{lock_key} 已存在提取信息，跳过")
            return await msg.ack()

        # 3) 全局信号量：限制大模型提取并发数
        #    并发已满则重新入队到队尾，稍后（信号量释放后）再处理，
        #    避免 RabbitMQ 持续投递导致瞬时并发压垮 LLM。
        sem_acquired = await prize_extract_redis.acquire_semaphore_blocking(
            key=SEM_KEY,
            max_concurrency=MAX_CONCURRENCY,
            ttl=SEM_TTL,
            timeout=SEM_ACQUIRE_TIMEOUT,
        )
        if not sem_acquired:
            MQ_logger.warning(
                f"【{module_name}】{lock_key} 大模型提取并发已满"
                f"({MAX_CONCURRENCY})，重新入队稍后处理")
            await BiliLotDataPublisher.pub_prize_extract(req, mq_props=mq_props)
            return await msg.ack()

        # 4) 不存在 → 调用大模型提取并写库
        #    大模型彻底失败：直接 nack，交给 RabbitMQ 延迟重试
        await _do_extract_and_store(req)

        MQ_logger.info(
            f"【{module_name}】{lock_key} 提取并入库完成: {req.result}")
        return await msg.ack()
    except Exception as e:
        MQ_logger.warning(
            f"【{module_name}】{lock_key} 大模型提取失败，"
            f"nack 交给 RabbitMQ 延迟重试: {type(e).__name__}: {e}")
        return await msg.nack()
    finally:
        # 释放信号量（仅在持有名额时）与 redis 去重锁
        if sem_acquired:
            await prize_extract_redis.release_semaphore(SEM_KEY)
        await prize_extract_redis.release_lock(lock_key)
# endregion


class PrizeExtractConsumer(BaseFastStreamMQ):
    """入库消息队列消费者（可按目标数据库实例化多条）。

    处理逻辑（大模型返回数据处理）全部在 process_prize_extract 中共享，
    本类仅负责绑定队列与消息体的反序列化。
    """

    def __init__(self, mq_props):
        super().__init__(mq_props=mq_props)

    async def consume(self, body: PrizeExtractReq, msg: RabbitMessage):
        MQ_logger.debug(
            f"【{self.mq_props.queue_name}】消费消息: {body}")
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
