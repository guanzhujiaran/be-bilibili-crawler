"""云端 LLM 槽位租约池：把「同一 (base_url, model_name, token) 同时最多 1 个在途请求」
做成跨进程成立的分布式租约。

槽位 = ``settings.llm_apis`` 里的一条 ``LLMApiConfig``，身份用 sha256 指纹表示
（见 :func:`tracked_llm.slot_fingerprint`，apikey 不落明文）。

为什么不是「全局计数信号量」：计数信号量只保证「总数 ≤ N」，不保证同一条配置不被并发
使用 —— 下游 ``_do_extract`` 会遍历实例列表逐个尝试，两个并发任务很容易同时选中同一条
配置。槽位租约把「锁定哪个槽位」与「用哪个槽位发请求」绑定在一起。

锁直接用 redis-py 的 :class:`redis.asyncio.lock.Lock`（不再自研 zset/Lua 计数）：

- ``SET NX PX``：租约到期自动释放 → 进程崩溃自愈；
- ``LUA_RELEASE`` 比对 token 才删 → 不会误删他人的锁；
- ``extend(replace_ttl=True)`` 心跳续约 → 处理耗时超过租约也不会被回收。

等待不轮询：抢不到时 ``BLPOP`` 阻塞在所有槽位的通知队列上，由 release 侧 ``LPUSH`` 唤醒。
没有可用槽位（未配置 / 全部冷却中 / 全部被删除）时在本机**无限等待**，
不返回、不抛错 —— 调用方的消息始终保持未确认，因此不丢。
等待时长取「指数退避」与「最近一个槽位的自动恢复时间」的较小值：
冷却中的槽位（限流 / 今日额度 / 欠费）睡到点即可自动恢复，
真正需要人工介入的（未配置 / 全部被删除）才退回指数退避等运维补配置。
"""

import asyncio
import random
import time
from dataclasses import dataclass

from bili_common.core.backoff import equal_jitter, expo_wait
from bili_common.models import StrEnumAutoDoc
from pydantic import BaseModel, Field
from redis.asyncio import Redis
from redis.asyncio.lock import Lock, LockError

from CONFIG import CONFIG
from Service.llm_service.health import LLMState
from Service.llm_service.pool import LLMSlot, get_llm_slots
from Service.llm_service.tracked_llm import TrackedChatOpenAI
from Utils.推送.PushMe import a_push_error
from Utils.redisTool.RedisManager import RedisManagerBase, redis_client_factory
from log.base_log import MQ_logger

#: 秒：单个槽位租约的存活时长；心跳停止（进程崩溃）后到期自动回收
SLOT_LEASE_TTL = 600
#: 秒：心跳续约间隔（租约的 1/3，留足重试余量）
SLOT_HEARTBEAT_INTERVAL = SLOT_LEASE_TTL / 3
#: 秒：单次 BLPOP 的阻塞上限（兜底漏唤醒，且必须小于 socket_timeout=10）
SLOT_NOTIFY_BLPOP_TIMEOUT = 5
#: 唤醒令牌最多堆积的数量（无人等待时防止无限增长）
SLOT_NOTIFY_BACKLOG = 64
#: 秒：「没有可用槽位」时本机退避等待的上限（指数退避封顶值）
SLOT_WAIT_MAX_BACKOFF = 60.0
#: 秒：「没有可用槽位」告警的推送间隔（同因告警不刷屏，由 message-service 聚合）
SLOT_ALERT_INTERVAL = 600.0

#: 「没有可用槽位」时的等待序列：指数退避，封顶 SLOT_WAIT_MAX_BACKOFF
_no_slot_wait = expo_wait(base=2.0, factor=1.0, max_value=SLOT_WAIT_MAX_BACKOFF)


@dataclass(slots=True)
class LLMSlotLease:
    """一次槽位占用（租约）：指纹 + 实例 + redis 锁 + 心跳任务。

    整个租约期间持有一个 redis 客户端（池上限 4096、槽位数为个位数，占用可忽略），
    释放时一并关闭。
    """

    fingerprint: str
    llm: TrackedChatOpenAI
    lock: Lock
    client: Redis
    notify_key: str
    heartbeat: asyncio.Task | None = None
    released: bool = False


class SlotStateCount(BaseModel):
    """某一健康状态下有多少个槽位（用于「没有可用槽位」时的可读汇总）"""

    state: LLMState = Field(description="健康状态")
    count: int = Field(description="处于该状态的槽位数量")


class LLMSlotWaitTimeout(RuntimeError):
    """在给定预算内没有抢到可用槽位。

    ``acquire(max_wait=...)`` 的上界被用尽时抛出；调用方（MQ 消费者）据此把消息
    交还队列重投，而不是让未确认消息超过 broker 的 ``consumer_timeout`` 被强制 nack。
    """


class LLMSlotPool(RedisManagerBase):
    """跨进程的 LLM 槽位池：按槽位抢锁，同一槽位同时只放行 1 个在途请求。"""

    class RedisMap(StrEnumAutoDoc):
        lock_prefix = "llm_slot:lock"
        notify_prefix = "llm_slot:notify"

    def __init__(self):
        super().__init__(
            host=CONFIG.database.getOtherLotRedis.host,
            port=CONFIG.database.getOtherLotRedis.port,
            db=CONFIG.database.getOtherLotRedis.db,
        )
        # 上一次「没有可用槽位」告警的推送时间（节流用）
        self._last_no_slot_alert: float = 0.0

    # region key 拼装
    def _lock_key(self, fingerprint: str) -> str:
        return f"{self.RedisMap.lock_prefix.value}:{fingerprint}"

    def _notify_key(self, fingerprint: str) -> str:
        return f"{self.RedisMap.notify_prefix.value}:{fingerprint}"

    # endregion

    # region 抢槽位 / 释放
    @staticmethod
    def _enabled_slots() -> list[LLMSlot]:
        """当前可用槽位（健康状态为 healthy）；配置为空 / 全部冷却 / 全部被删时返回空列表。

        「已删除」（不可恢复错误）与「冷却中」（限流 / 今日额度 / 欠费 / 连续失败）
        都不参与抢锁：前者配置已经从池里摘掉，后者到点会自动恢复。
        """
        return [slot for slot in get_llm_slots() if slot.llm.available]

    @staticmethod
    def _earliest_resume_delay() -> float | None:
        """最近一个槽位距自动恢复还有多少秒；没有任何槽位会自动恢复时返回 None。"""
        delays = [
            delay
            for slot in get_llm_slots()
            if (delay := slot.llm.stats.health.resume_delay()) is not None
        ]
        return min(delays) if delays else None

    async def acquire(self, max_wait: float | None = None) -> LLMSlotLease:
        """抢一个可用槽位；``max_wait`` 为 None 时一直等到拿到为止。

        调用方（MQ 消费者）在等待期间保持消息未确认，因此等多久都不会丢数据：

        - 没有可用槽位 → :meth:`_wait_for_any_slot` 本机退避等待配置 / 冷却恢复；
        - 槽位全被占用 → ``BLPOP`` 事件驱动等待，被 release 唤醒后重试。

        Args:
            max_wait: 本次等待的时间上界（秒）。MQ 消费者会传入「消息确认超时预算」
                的剩余时间 —— 超过上界仍抢不到时抛 :class:`LLMSlotWaitTimeout`，
                由调用方把消息交还队列重投，避免未确认时间超出 broker 的
                ``consumer_timeout`` 被强制 nack。``None`` 表示无限等待（脚本等场景）。
        """
        deadline = None if max_wait is None else time.monotonic() + max(0.0, max_wait)
        await self._wait_for_any_slot(deadline)
        while True:
            slots = self._enabled_slots()
            if not slots:
                # 等待期间配置被清空 / 全部删除 / 全部冷却：回到「等槽位」分支
                await self._wait_for_any_slot(deadline)
                continue
            lease = await self._try_acquire_any(slots)
            if lease is not None:
                return lease
            await self._wait_notify(slots, deadline)

    @staticmethod
    def _remaining(deadline: float | None) -> float | None:
        """距等待截止还剩多少秒；``None`` 表示不设上界"""
        if deadline is None:
            return None
        return deadline - time.monotonic()

    async def _try_acquire_any(self, slots: list[LLMSlot]) -> LLMSlotLease | None:
        """按随机顺序尝试抢锁，全忙则返回 None。

        随机顺序是为了避免所有消费者实例都去抢配置列表里的第一条槽位（热点）。
        """
        ordered = list(slots)
        random.shuffle(ordered)
        for slot in ordered:
            lease = await self._try_acquire(slot)
            if lease is not None:
                return lease
        return None

    async def _try_acquire(self, slot: LLMSlot) -> LLMSlotLease | None:
        """尝试占用单个槽位；抢到则启动心跳续约。"""
        client = redis_client_factory(pool=self.pool)
        fingerprint = slot.fingerprint
        lock = Lock(
            client,
            name=self._lock_key(fingerprint),
            timeout=SLOT_LEASE_TTL,
            # thread_local=False：同一事件循环里 thread-local 会被所有任务共享，
            # 必须每个租约一个 Lock 实例，token 才不会被别的任务覆盖。
            thread_local=False,
            # 租约被 TTL 回收后再释放不抛错（释放动作本身幂等）
            raise_on_release_error=False,
        )
        try:
            acquired = await lock.acquire(blocking=False)
        except BaseException:
            await self._close_client(client)
            raise
        if not acquired:
            await self._close_client(client)
            return None
        lease = LLMSlotLease(
            fingerprint=fingerprint,
            llm=slot.llm,
            lock=lock,
            client=client,
            notify_key=self._notify_key(fingerprint),
        )
        lease.heartbeat = asyncio.create_task(self._renew_loop(lease))
        return lease

    async def release(self, lease: LLMSlotLease) -> None:
        """释放槽位：只删自己持有的锁，并唤醒一个等待者。"""
        lease.released = True
        self._stop_heartbeat(lease)
        try:
            try:
                await lease.lock.release()
            except LockError as e:
                # 租约已因 TTL 到期被回收：锁本来就不在了，无需唤醒
                # （等待者最多多等一个 BLPOP 超时后自行重试）
                MQ_logger.warning(
                    f"【LLM槽位】{lease.fingerprint} 释放时已不持有锁"
                    f"（租约到期被回收）：{e}"
                )
                return
            # 名额空出来了：放一个唤醒令牌，并限制堆积量 —— 无人等待时令牌会积压，
            # 不限制的话后续等待者会连续「醒来 → 抢不到 → 再醒」空转很多轮。
            async with lease.client.pipeline(transaction=False) as pipe:
                pipe.lpush(lease.notify_key, "1")
                pipe.ltrim(lease.notify_key, 0, SLOT_NOTIFY_BACKLOG - 1)
                await pipe.execute()
        finally:
            await self._close_client(lease.client)

    # endregion

    # region 等待
    @staticmethod
    def _check_deadline(deadline: float | None) -> None:
        """超出等待上界时抛错（``None`` 表示不设上界）"""
        if deadline is not None and time.monotonic() >= deadline:
            raise LLMSlotWaitTimeout("等待可用 LLM 槽位已超出本次预算，需要稍后重试")

    async def _wait_notify(self, slots: list[LLMSlot], deadline: float | None) -> None:
        """阻塞等待任一槽位释放（Redis 服务端阻塞，消费者侧零空转）。

        BLPOP 支持多 key，返回 ``(key, value)`` 即知道是哪个槽位空出来了；
        单次阻塞有时长上限，超时后由调用方重新尝试抢锁（兜底漏唤醒）。
        ``deadline`` 非空时，本次阻塞不会超过剩余上界。
        """
        keys = [self._notify_key(slot.fingerprint) for slot in slots]
        timeout = SLOT_NOTIFY_BLPOP_TIMEOUT
        remaining = self._remaining(deadline)
        if remaining is not None:
            self._check_deadline(deadline)
            timeout = min(timeout, max(0.1, remaining))
        async with redis_client_factory(pool=self.pool) as r:
            await r.blpop(keys, timeout=timeout)

    async def _wait_for_any_slot(self, deadline: float | None) -> None:
        """没有可用槽位时本机等待，直到出现可用槽位为止。

        等待时长取「指数退避」与「最近一个槽位的自动恢复时间」中的较小值：

        - 槽位只是冷却中（限流 / 今日额度 / 欠费）→ 按 ``resume_at`` 睡到点，
          到点即恢复，不需要靠 60s 一轮的盲等慢慢蹭；
        - 全部已被删除 / 未配置 → 没有自动恢复时间，退回指数退避，
          一直等到运维通过 ``POST /llm/config`` 补回可用配置。

        ``deadline`` 非空时（MQ 的消息确认超时预算）等待不会越过上界，
        超出即抛 :class:`LLMSlotWaitTimeout`，由调用方把消息交还队列重投。
        """
        attempt = 0
        while not self._enabled_slots():
            self._check_deadline(deadline)
            attempt += 1
            # full_jitter 会给出接近 0 的等待，这里用等抖动并把下限抬到 1s，
            # 避免「没有可用槽位」时空转刷日志。
            wait = max(1.0, equal_jitter(_no_slot_wait(attempt)))
            resume_delay = self._earliest_resume_delay()
            if resume_delay is not None:
                # +1s 抵消时间流逝带来的边界误差，确保醒来时冷却确实已到期
                wait = min(wait, max(1.0, resume_delay + 1.0))
            remaining = self._remaining(deadline)
            if remaining is not None:
                wait = min(wait, max(0.0, remaining))
            await self._alert_no_slot(attempt, wait)
            await asyncio.sleep(wait)

    @staticmethod
    def _unavailable_summary() -> str:
        """把「当前所有槽位分别处于什么状态」压成一行，便于区分「该等」还是「该修配置」"""
        counts: list[SlotStateCount] = []
        for slot in get_llm_slots():
            state = slot.llm.state
            for item in counts:
                if item.state is state:
                    item.count += 1
                    break
            else:
                counts.append(SlotStateCount(state=state, count=1))
        if not counts:
            return "当前未配置任何云端 LLM"
        return "；".join(f"{item.state.value}×{item.count}" for item in counts)

    async def _alert_no_slot(self, attempt: int, wait: float) -> None:
        """记录（并按 SLOT_ALERT_INTERVAL 节流推送）「没有可用槽位」告警。

        推送正文刻意保持稳定（不带次数/时间），message-service 才能按内容去重聚合成
        「×N」摘要；逐轮明细只写日志。
        """
        summary = self._unavailable_summary()
        resume_delay = self._earliest_resume_delay()
        resume_text = (
            "不会自动恢复" if resume_delay is None else f"{resume_delay:.0f}s 后自动恢复"
        )
        MQ_logger.warning(
            f"【LLM槽位】当前没有可用的云端 LLM 槽位（{summary}），"
            f"{wait:.1f}s 后重试（第 {attempt} 次，最近恢复时间：{resume_text}）；"
            f"消息保持未确认，等待槽位恢复或运维补回配置"
        )
        now = time.monotonic()
        if now - self._last_no_slot_alert < SLOT_ALERT_INTERVAL:
            return
        self._last_no_slot_alert = now
        try:
            await a_push_error(
                content=(
                    "没有可用的云端 LLM 槽位，抽奖提取任务正在本机等待："
                    f"{summary}。冷却中的槽位到点会自动恢复；"
                    "若全部为 removed 或未配置，需运维补回 llm_apis 配置"
                ),
                subject="LLM槽位不可用",
            )
        except Exception as e:  # noqa: BLE001 - 推送失败不影响等待逻辑
            MQ_logger.warning(f"【LLM槽位】推送「槽位不可用」告警失败：{e}")

    # endregion

    # region 心跳续约
    async def _renew_loop(self, lease: LLMSlotLease) -> None:
        """心跳续约：处理耗时超过租约时槽位不被回收，也就不会出现并发超限。

        进程崩溃 / 被 kill 时心跳随之消失，槽位在租约到期后被别的消费者抢走 ——
        这就是崩溃自愈，粒度是单个槽位而不是整把闸。
        """
        while True:
            await asyncio.sleep(SLOT_HEARTBEAT_INTERVAL)
            try:
                await lease.lock.extend(SLOT_LEASE_TTL, replace_ttl=True)
            except asyncio.CancelledError:
                raise
            except LockError as e:
                # 正常释放时的心跳竞态不该刷 warning：释放方已置 released
                if lease.released:
                    return
                MQ_logger.warning(
                    f"【LLM槽位】{lease.fingerprint} 心跳续约失败"
                    f"（槽位已被回收，本次处理可能与其他实例并发）：{e}"
                )
                return
            except Exception as e:  # noqa: BLE001 - 续约失败只告警，到期回收兜底
                MQ_logger.warning(
                    f"【LLM槽位】{lease.fingerprint} 心跳续约异常："
                    f"{type(e).__name__}: {e}"
                )

    @staticmethod
    def _stop_heartbeat(lease: LLMSlotLease) -> None:
        heartbeat = lease.heartbeat
        lease.heartbeat = None
        if heartbeat is not None and not heartbeat.done():
            heartbeat.cancel()

    # endregion

    @staticmethod
    async def _close_client(client: Redis) -> None:
        """把租约期间占用的客户端归还连接池（关闭失败不影响主流程）。"""
        try:
            await client.aclose()
        except Exception as e:  # noqa: BLE001
            MQ_logger.warning(f"【LLM槽位】关闭 redis 客户端失败：{e}")


# 全局单例：与 MQ 流程共用同一套槽位闸
llm_slot_pool = LLMSlotPool()

__all__ = [
    "LLMSlotLease",
    "LLMSlotPool",
    "LLMSlotWaitTimeout",
    "SlotStateCount",
    "llm_slot_pool",
    "SLOT_HEARTBEAT_INTERVAL",
    "SLOT_LEASE_TTL",
]
