"""代理健康状态机（Redis 热缓存）。

## 为什么需要它

原实现里代理健康只有 ``proxy_tab`` 的 ``score`` / ``success_times`` 两个**净累计**字段：

- ``score`` 每次失败 -10、成功 +10，长期互相抵消，反映不出「最近是否还能用」；
- ``success_times`` 同理，且没有「连续失败」概念，坏代理要失败很多次才被稀释到低位；
- ``refresh_proxy`` 每 10 分钟把所有满足条件的 ``status`` 复位成 0，
  412 冷却 2 小时的设计因此形同虚设。

结果是：坏代理永远被随机抽到，worker 一直在重试同一批坏代理。

## 设计

- 状态机：``hot`` / ``warm`` / ``suspect`` / ``quarantine`` / ``dead``，
  外加隐式状态 ``cold``（Redis 无记录 = 未验证，只能被 slow 档探测）；
- 连续失败 ``warm_after_fails`` / ``suspect_after_fails`` 触发降档与指数冷却，
  成功一次即清零并回到 hot；
- 风控层（412/352）按 IP 冷却 2~6h，网络层冷却 10~30min；
- 明细放 Redis（热缓存），MySQL 仍只写 ``proxy_tab`` 原有的
  status / score / success_times 语义，**不新增列、不改字段含义**；
- 冷却中的代理由 ``SQLHelper.flush_proxy_health`` 定期回写 ``update_ts``，
  使 ``refresh_proxy`` 的「update_ts 超过 2 小时才复位」条件不成立 —— 冷却不会被复位。

所有方法都是**尽力而为**：Redis 不可用时降级为「无记录」（cold），
绝不把异常抛进业务请求路径（代理池故障不能拖垮爬虫）。
"""

import asyncio
import random
import time
from typing import Iterable, Sequence

import redis.asyncio as aioredis
from bili_common.models import StrEnumAutoDoc
from pydantic import Field

from CONFIG import CONFIG
from log.base_log import sql_log
from Models.base.custom_pydantic import CustomBaseModel

#: 明细 key 的存活时间（秒）：代理长期不用会自动淡出，不会无限膨胀
_RECORD_TTL = 7 * 24 * 3600
#: 状态集合 key 前缀
_STATE_SET_PREFIX = "proxy_health:v1:state:"
#: 明细 hash 前缀（每个代理一个 key）
_DETAIL_PREFIX = "proxy_health:v1:detail:"
#: 在途计数 key 前缀
_IN_FLIGHT_PREFIX = "proxy_health:v1:inflight:"
#: 冷却集合（zset：member=代理串，score=冷却到期时间戳）
_COOLING_KEY = "proxy_health:v1:cooling"


class ProxyHealthState(StrEnumAutoDoc):
    """代理健康状态。

    流转（失败累计 cf，成功清零）：

    - ``cold``：无健康记录（未验证）。只能被 slow 档按比例探测；
    - ``hot``：最近表现良好（``ewma_success`` 高、未在冷却），fast 档的主池；
    - ``warm``：连续失败 >= warm_after_fails，冷却时间开始指数增长；
    - ``suspect``：连续失败 >= suspect_after_fails，只允许 slow 档低频探测；
    - ``quarantine``：风控层命中（412/352）或连续失败 >= quarantine_after_fails，
      按 IP 长冷却（2~6h），期间任何档位都不取；
    - ``dead``：连续失败 >= dead_after_fails，长冷却且不进 fast 池，
      只有 slow 档冷却到期后的探测能把它救回来。
    """

    COLD = "cold"
    HOT = "hot"
    WARM = "warm"
    SUSPECT = "suspect"
    QUARANTINE = "quarantine"
    DEAD = "dead"

    @property
    def is_usable(self) -> bool:
        return self in (ProxyHealthState.HOT, ProxyHealthState.WARM)


class ProxyFailureKind(StrEnumAutoDoc):
    """代理失败的分流口径。

    - ``network``：网络层（curl 28/7/35、407、连接失败、超时）；
      扣分 + 连续失败计数 + 10~30 分钟冷却；
    - ``risk_control``：风控层（412 / 352）；按 IP 冷却 2~6 小时；
    - ``response``：代理返回了真实的上游错误（HTTP 通了但内容不可用）；
      按网络层处理，但单独记数，便于区分「代理坏」和「上游坏」；
    - ``unknown``：无法归类；按网络层处理。
    """

    NETWORK = "network"
    RISK_CONTROL = "risk_control"
    RESPONSE = "response"
    UNKNOWN = "unknown"


class ProxyHealthRecord(CustomBaseModel):
    """单个代理的健康明细（Redis 热缓存里的一条记录）。"""

    state: ProxyHealthState = Field(default=ProxyHealthState.COLD)
    consecutive_fail: int = Field(default=0, description="连续失败次数，成功即清零")
    ewma_success: float = Field(
        default=0.0, description="近期成功率 EWMA（0~1），fast 档准入依据"
    )
    cooldown_until: int = Field(default=0, description="冷却到期 unix 秒；0 表示未冷却")
    success_total: int = 0
    fail_total: int = 0
    quarantine_total: int = Field(default=0, description="被风控层命中的累计次数")
    update_ts: int = 0
    last_kind: ProxyFailureKind | None = None

    def is_cooling(self, now: int | None = None) -> bool:
        return self.cooldown_until > (now if now is not None else int(time.time()))

    def remaining_cooldown(self, now: int | None = None) -> int:
        return max(0, self.cooldown_until - (now if now is not None else int(time.time())))


class ProxyHealthMap(CustomBaseModel):
    """一批代理的健康记录（field=代理串）。

    用模型包一层而不是到处传裸 dict：调用方只能通过 ``records`` / ``get`` 访问，
    避免「键拼错 / 缺省值不一致」这类低级错误扩散到选代理逻辑里。
    """

    records: dict[str, ProxyHealthRecord] = Field(default_factory=dict)

    def get(self, proxy_key: str) -> ProxyHealthRecord:
        return self.records.get(proxy_key) or ProxyHealthRecord()

    def has(self, proxy_key: str) -> bool:
        return proxy_key in self.records

    def __len__(self) -> int:
        return len(self.records)


class ProxyInFlightMap(CustomBaseModel):
    """一批代理的当前在途并发数。"""

    counts: dict[str, int] = Field(default_factory=dict)

    def get(self, proxy_key: str) -> int:
        return self.counts.get(proxy_key, 0)

    def __len__(self) -> int:
        return len(self.counts)


class ProxyHealthCounts(CustomBaseModel):
    """分档数量快照（供调度做背压 / 供接口与日志观测）。"""

    cold: int = Field(default=0, description="无健康记录（未验证）的代理数，需抽样估算")
    hot: int = 0
    warm: int = 0
    suspect: int = 0
    quarantine: int = 0
    dead: int = 0
    cooling: int = Field(default=0, description="当前处于冷却中的代理数")
    update_ts: int = 0

    @property
    def known_total(self) -> int:
        return self.hot + self.warm + self.suspect + self.quarantine + self.dead

    @property
    def usable_total(self) -> int:
        """未被冷却、可参与取用的数量（hot + warm）。"""
        return self.hot + self.warm

    def to_log_str(self) -> str:
        return (
            f"hot={self.hot} warm={self.warm} suspect={self.suspect} "
            f"quarantine={self.quarantine} dead={self.dead} "
            f"cooling={self.cooling} cold={self.cold}"
        )


def seed_health_record(
    success_times: int | None, score: int | None
) -> ProxyHealthRecord:
    """Redis 无记录时的冷启动推断（不发 Redis 读写，纯计算）。

    口径刻意保守：只有「历史上成功过若干次且分数为正」的老代理才被当成 ``warm``
    并给出刚好达标的 EWMA；其余一律 ``cold``（只能被 slow 档探测），
    避免 Redis 被清空后 fast 档又一次次抽到从未验证的垃圾代理。
    """
    cfg = CONFIG.proxy_health
    cfg_min_success = max(1, cfg.seed_success_times)
    if (success_times or 0) >= cfg_min_success and (score or 0) >= cfg.seed_min_score:
        return ProxyHealthRecord(
            state=ProxyHealthState.WARM,
            consecutive_fail=0,
            ewma_success=min(1.0, cfg.fast_min_ewma),
            success_total=int(success_times or 0),
            update_ts=int(time.time()),
        )
    return ProxyHealthRecord()


class ProxyHealthStore:
    """代理健康状态的 Redis 存储（异步、短超时、永不抛异常）。"""

    def __init__(self) -> None:
        cfg = CONFIG.proxy_health
        # 连接池延迟到首次使用时创建：本模块会被 CONFIG / SQLHelper 间接 import，
        # 模块导入期不应建立任何 socket。
        self._pool: aioredis.ConnectionPool | None = None
        self._cfg = cfg
        self._counts_cache: ProxyHealthCounts | None = None
        self._counts_cache_ts: float = 0.0
        self._warn_ts: float = 0.0

    # region 基础设施
    @property
    def cfg(self):
        return CONFIG.proxy_health

    def _get_client(self) -> aioredis.Redis:
        if self._pool is None:
            redis_cfg = CONFIG.database.proxyRedis
            cfg = self.cfg
            self._pool = aioredis.ConnectionPool.from_url(
                url=(
                    f"redis://:{redis_cfg.pwd}@{redis_cfg.host}:{redis_cfg.port}"
                    f"/{redis_cfg.db}?decode_responses=True"
                ),
                max_connections=64,
                socket_timeout=cfg.redis_timeout,
                socket_connect_timeout=cfg.redis_timeout,
            )
        return aioredis.Redis(connection_pool=self._pool)

    def _warn(self, msg: str) -> None:
        """Redis 异常限流告警：热路径上失败一次不能刷屏。"""
        now = time.monotonic()
        if now - self._warn_ts < 60:
            return
        self._warn_ts = now
        sql_log.warning(f"代理健康缓存不可用（本次仅告警一次/分钟）：{msg}")

    @staticmethod
    def _detail_key(proxy_key: str) -> str:
        return f"{_DETAIL_PREFIX}{proxy_key}"

    @staticmethod
    def _state_set_key(state: ProxyHealthState) -> str:
        return f"{_STATE_SET_PREFIX}{state.value}"

    @staticmethod
    def _in_flight_key(proxy_key: str) -> str:
        return f"{_IN_FLIGHT_PREFIX}{proxy_key}"

    # endregion

    # region 读取
    async def get(self, proxy_key: str) -> ProxyHealthRecord:
        if not proxy_key:
            return ProxyHealthRecord()
        try:
            client = self._get_client()
            raw = await client.hgetall(self._detail_key(proxy_key))
        except Exception as e:  # noqa: BLE001 健康缓存失败必须降级
            self._warn(f"hgetall 失败：{type(e).__name__} {e}")
            return ProxyHealthRecord()
        return self._parse(raw)

    async def get_many(self, proxy_keys: Sequence[str]) -> ProxyHealthMap:
        """批量读取（选代理时对一批候选一次取回，pipeline 单次往返）。

        只返回**有记录**的代理：没有记录的由调用方按 ``seed_health_record`` 推断，
        Redis 里不会因为「读到一次空值」就写满冷数据。
        """
        keys = [k for k in proxy_keys if k]
        if not keys:
            return ProxyHealthMap()
        try:
            client = self._get_client()
            async with client.pipeline(transaction=False) as pipe:
                for key in keys:
                    pipe.hgetall(self._detail_key(key))
                results = await pipe.execute()
        except Exception as e:  # noqa: BLE001
            self._warn(f"批量 hgetall 失败：{type(e).__name__} {e}")
            return ProxyHealthMap()
        return ProxyHealthMap(
            records={key: self._parse(raw) for key, raw in zip(keys, results) if raw}
        )

    def _parse(self, raw: dict) -> ProxyHealthRecord:
        if not raw:
            return ProxyHealthRecord()
        try:
            return ProxyHealthRecord(
                state=ProxyHealthState(raw.get("state") or ProxyHealthState.COLD.value),
                consecutive_fail=int(raw.get("cf") or 0),
                ewma_success=float(raw.get("ewma") or 0.0),
                cooldown_until=int(raw.get("cd") or 0),
                success_total=int(raw.get("ok") or 0),
                fail_total=int(raw.get("fail") or 0),
                quarantine_total=int(raw.get("qn") or 0),
                update_ts=int(raw.get("ts") or 0),
                last_kind=(
                    ProxyFailureKind(raw["kind"]) if raw.get("kind") else None
                ),
            )
        except Exception:  # noqa: BLE001 脏数据按 cold 处理
            return ProxyHealthRecord()

    async def get_counts(self, force: bool = False) -> ProxyHealthCounts:
        """分档数量（SCARD，O(1)），进程内缓存 ``count_cache_seconds`` 秒。"""
        now = time.monotonic()
        cached = self._counts_cache
        if (
            not force
            and cached is not None
            and now - self._counts_cache_ts < max(0, self.cfg.count_cache_seconds)
        ):
            return cached
        ts = int(time.time())
        counts = ProxyHealthCounts(update_ts=ts)
        try:
            client = self._get_client()
            async with client.pipeline(transaction=False) as pipe:
                for state in ProxyHealthState:
                    if state is ProxyHealthState.COLD:
                        continue
                    pipe.scard(self._state_set_key(state))
                pipe.zcount(_COOLING_KEY, ts, "+inf")
                results = await pipe.execute()
        except Exception as e:  # noqa: BLE001
            self._warn(f"统计分档数量失败：{type(e).__name__} {e}")
            return cached or counts
        # 顺序与上面 pipe 的顺序一一对应（跳过 cold）
        ordered = [s for s in ProxyHealthState if s is not ProxyHealthState.COLD]
        for state, value in zip(ordered, results[:-1]):
            setattr(counts, state.value, int(value or 0))
        counts.cooling = int(results[-1] or 0)
        self._counts_cache = counts
        self._counts_cache_ts = now
        return counts

    async def get_in_flight(self, proxy_keys: Sequence[str]) -> ProxyInFlightMap:
        """批量取在途并发数（MGET 单次往返）。"""
        keys = [k for k in proxy_keys if k]
        if not keys:
            return ProxyInFlightMap()
        try:
            client = self._get_client()
            values = await client.mget([self._in_flight_key(k) for k in keys])
        except Exception as e:  # noqa: BLE001
            self._warn(f"读取在途计数失败：{type(e).__name__} {e}")
            return ProxyInFlightMap()
        return ProxyInFlightMap(
            counts={key: int(value or 0) for key, value in zip(keys, values)}
        )

    async def list_cooling_keys(self, limit: int = 1000) -> list[str]:
        """当前仍在冷却的代理串（按到期时间升序，最多 ``limit`` 条）。"""
        now = int(time.time())
        try:
            client = self._get_client()
            # 顺手清掉已经过期的成员，zset 不会无限增长
            await client.zremrangebyscore(_COOLING_KEY, "-inf", now)
            return await client.zrangebyscore(_COOLING_KEY, now, "+inf", start=0, num=limit)
        except Exception as e:  # noqa: BLE001
            self._warn(f"读取冷却集合失败：{type(e).__name__} {e}")
            return []

    async def sample_state_keys(
        self, states: Sequence[ProxyHealthState], count: int
    ) -> list[str]:
        """从指定分档的集合里随机取样（``SRANDMEMBER``，O(count)，不扫全表）。

        这是「优先用已验证代理」的关键：代理池 68 万行里已验证的只有几千条，
        靠「随机主键起点 + 32 行候选」几乎抽不到（实测 80 次里只有 1 次）。
        分档集合本身就是一份可枚举的候选名单，直接从中取样才拿得到好代理。
        """
        states = [s for s in states if s is not ProxyHealthState.COLD]
        if not states or count <= 0:
            return []
        try:
            client = self._get_client()
            async with client.pipeline(transaction=False) as pipe:
                for state in states:
                    pipe.srandmember(self._state_set_key(state), count)
                results = await pipe.execute()
        except Exception as e:  # noqa: BLE001
            self._warn(f"取样已验证代理失败：{type(e).__name__} {e}")
            return []
        keys: list[str] = []
        seen: set[str] = set()
        # 按 states 顺序取（调用方把 hot 放在前面，天然优先），同一条只出现一次
        for group in results:
            for key in group or []:
                if key and key not in seen and len(keys) < count:
                    seen.add(key)
                    keys.append(key)
        return keys

    async def forget_state(self, proxy_key: str) -> None:
        """把一条代理从所有分档集合里摘掉（MySQL 里已不可用的僵尸成员）。

        MySQL 才是代理池的真相：行被删 / status 被置黑之后，Redis 集合里的残留
        必须清掉，否则每次取样都会抽到它、白跑一次查库。
        """
        if not proxy_key:
            return
        try:
            client = self._get_client()
            async with client.pipeline(transaction=False) as pipe:
                for state in ProxyHealthState:
                    pipe.srem(self._state_set_key(state), proxy_key)
                pipe.delete(self._detail_key(proxy_key))
                await pipe.execute()
        except Exception as e:  # noqa: BLE001
            self._warn(f"清理失效代理失败：{type(e).__name__} {e}")

    # endregion

    # region 在途并发计数
    async def acquire_in_flight(self, proxy_key: str) -> None:
        """标记「有一请求正在用该代理」；TTL 保证进程崩溃后自动释放。"""
        if not proxy_key:
            return
        try:
            client = self._get_client()
            key = self._in_flight_key(proxy_key)
            async with client.pipeline(transaction=False) as pipe:
                pipe.incr(key)
                pipe.expire(key, max(1, self.cfg.in_flight_ttl))
                await pipe.execute()
        except Exception as e:  # noqa: BLE001
            self._warn(f"在途计数 +1 失败：{type(e).__name__} {e}")

    async def release_in_flight(self, proxy_key: str) -> None:
        if not proxy_key:
            return
        try:
            client = self._get_client()
            key = self._in_flight_key(proxy_key)
            value = await client.decr(key)
            if value is not None and value <= 0:
                # 计数归零直接删 key，避免 -1 残留让该代理永久「看起来忙」
                await client.delete(key)
        except Exception as e:  # noqa: BLE001
            self._warn(f"在途计数 -1 失败：{type(e).__name__} {e}")

    # endregion

    # region 状态迁移
    async def record_success(
        self, proxy_key: str, previous: ProxyHealthRecord | None = None
    ) -> ProxyHealthRecord:
        """成功：EWMA 上升、连续失败清零、状态回到 hot、清冷却。"""
        if not proxy_key:
            return ProxyHealthRecord()
        cfg = self.cfg
        record = previous or await self.get(proxy_key)
        # 必须在改字段**之前**记下旧状态：previous 与 record 常常是同一个对象，
        # 改完再读 previous.state 只会拿到 hot，旧状态集合就清不掉了。
        previous_state = record.state
        alpha = min(max(cfg.ewma_alpha, 0.01), 1.0)
        record.ewma_success = min(
            1.0, record.ewma_success * (1 - alpha) + alpha
        )
        record.consecutive_fail = 0
        record.state = ProxyHealthState.HOT
        record.cooldown_until = 0
        record.last_kind = None
        record.update_ts = int(time.time())
        await self._save(proxy_key, record, previous_state=previous_state)
        return record

    async def record_failure(
        self,
        proxy_key: str,
        kind: ProxyFailureKind = ProxyFailureKind.NETWORK,
        previous: ProxyHealthRecord | None = None,
    ) -> ProxyHealthRecord:
        """失败：降档 + 指数冷却；风控层按 IP 长冷却。"""
        if not proxy_key:
            return ProxyHealthRecord()
        cfg = self.cfg
        record = previous or await self.get(proxy_key)
        previous_state = record.state
        alpha = min(max(cfg.ewma_alpha, 0.01), 1.0)
        record.ewma_success = max(0.0, record.ewma_success * (1 - alpha))
        record.consecutive_fail += 1
        record.fail_total += 1
        now = int(time.time())
        if kind is ProxyFailureKind.RISK_CONTROL:
            record.quarantine_total += 1
            state = ProxyHealthState.QUARANTINE
            cooldown = self._risk_cooldown()
        else:
            state = self._state_for_fails(record.consecutive_fail)
            cooldown = self._network_cooldown(record.consecutive_fail, state)
        record.state = state
        record.cooldown_until = now + cooldown
        record.last_kind = kind
        record.update_ts = now
        await self._save(proxy_key, record, previous_state=previous_state)
        return record

    def _state_for_fails(self, fails: int) -> ProxyHealthState:
        cfg = self.cfg
        if fails >= cfg.dead_after_fails:
            return ProxyHealthState.DEAD
        if fails >= cfg.quarantine_after_fails:
            return ProxyHealthState.QUARANTINE
        if fails >= cfg.suspect_after_fails:
            return ProxyHealthState.SUSPECT
        if fails >= cfg.warm_after_fails:
            return ProxyHealthState.WARM
        return ProxyHealthState.HOT

    def _network_cooldown(self, fails: int, state: ProxyHealthState) -> int:
        """网络层冷却：10~30 分钟，随降档次数指数增长后夹在区间内。"""
        cfg = self.cfg
        level = {
            ProxyHealthState.HOT: 0,
            ProxyHealthState.WARM: 1,
            ProxyHealthState.SUSPECT: 2,
            ProxyHealthState.QUARANTINE: 3,
            ProxyHealthState.DEAD: 4,
        }.get(state, 0)
        low = max(cfg.network_cooldown_min, 1)
        high = max(cfg.network_cooldown_max, low)
        raw = low * (2**level)
        cooldown = float(min(max(raw, low), high))
        upper = float(high)
        if state is ProxyHealthState.DEAD:
            # dead 走更长的冷却（默认 6h），上界同步放宽，不受 10~30min 区间限制
            cooldown = max(cooldown, float(cfg.dead_cooldown))
            upper = max(upper, float(cfg.dead_cooldown))
        # 抖动后仍要夹回区间上界，否则 30 分钟冷却会抖成 33 分钟
        return int(min(upper, max(float(low), self._jitter(cooldown))))

    def _risk_cooldown(self) -> int:
        """风控层冷却：2~6 小时，区间内均匀分布（避免同一批 IP 同时解冻）。"""
        cfg = self.cfg
        low = max(cfg.risk_cooldown_min, 1)
        high = max(cfg.risk_cooldown_max, low)
        return int(random.uniform(low, high))

    @staticmethod
    def _jitter(value: float) -> float:
        """±10% 抖动。"""
        return max(1.0, value * random.uniform(0.9, 1.1))

    @staticmethod
    def _record_mapping(record: ProxyHealthRecord) -> dict[str, str | int | float]:
        """记录 → Redis hash 字段（字段名短，减少热缓存体积）。"""
        return {
            "state": record.state.value,
            "cf": record.consecutive_fail,
            "ewma": round(record.ewma_success, 6),
            "cd": record.cooldown_until,
            "ok": record.success_total,
            "fail": record.fail_total,
            "qn": record.quarantine_total,
            "ts": record.update_ts,
            "kind": record.last_kind.value if record.last_kind else "",
        }

    async def _save(
        self,
        proxy_key: str,
        record: ProxyHealthRecord,
        previous_state: ProxyHealthState | None = None,
    ) -> None:
        """写明细 + 维护状态集合 + 冷却 zset（单次 pipeline）。"""
        try:
            client = self._get_client()
            detail_key = self._detail_key(proxy_key)
            new_set = self._state_set_key(record.state)
            async with client.pipeline(transaction=False) as pipe:
                pipe.hset(detail_key, mapping=self._record_mapping(record))
                pipe.expire(detail_key, _RECORD_TTL)
                # 状态集合：旧状态移除、新状态加入（旧状态未知时全量移除一次）
                states = (
                    [previous_state]
                    if previous_state is not None
                    else list(ProxyHealthState)
                )
                for state in states:
                    if state is ProxyHealthState.COLD or state is record.state:
                        continue
                    pipe.srem(self._state_set_key(state), proxy_key)
                pipe.sadd(new_set, proxy_key)
                pipe.expire(new_set, _RECORD_TTL)
                if record.cooldown_until:
                    pipe.zadd(_COOLING_KEY, {proxy_key: record.cooldown_until})
                else:
                    pipe.zrem(_COOLING_KEY, proxy_key)
                await pipe.execute()
        except Exception as e:  # noqa: BLE001
            self._warn(f"写入健康记录失败：{type(e).__name__} {e}")

    async def bulk_seed(self, items: Iterable[tuple[str, ProxyHealthRecord]]) -> int:
        """批量播种（只在进程首次启动时用，一条 pipeline 写一批）。

        用途：Redis 被清空 / 首次上线时，把 MySQL 里「历史上确实成功过」的代理
        一次性写进健康缓存，避免 fast 车道在冷启动期间一个候选都没有。
        """
        entries = [(key, record) for key, record in items if key]
        if not entries:
            return 0
        written = 0
        try:
            client = self._get_client()
            for start in range(0, len(entries), 500):
                chunk = entries[start : start + 500]
                async with client.pipeline(transaction=False) as pipe:
                    for key, record in chunk:
                        detail_key = self._detail_key(key)
                        state_set = self._state_set_key(record.state)
                        pipe.hset(detail_key, mapping=self._record_mapping(record))
                        pipe.expire(detail_key, _RECORD_TTL)
                        pipe.sadd(state_set, key)
                        pipe.expire(state_set, _RECORD_TTL)
                    await pipe.execute()
                written += len(chunk)
        except Exception as e:  # noqa: BLE001
            self._warn(f"批量播种健康记录失败：{type(e).__name__} {e}")
        return written

    async def mark_db_success_times(self, proxy_key: str, success_total: int) -> None:
        """把 MySQL 的历史成功次数同步进 hot 计数（冷启动时用，避免观测口径从 0 开始）。"""
        record = await self.get(proxy_key)
        if record.success_total >= success_total:
            return
        record.success_total = success_total
        await self._save(proxy_key, record, previous_state=record.state)

    # endregion

    # region 决策
    def is_fast_eligible(
        self, record: ProxyHealthRecord, in_flight: int, now: int | None = None
    ) -> bool:
        """fast 档准入：未冷却 + EWMA 达标 + 在途并发未打满。"""
        cfg = self.cfg
        return (
            not record.is_cooling(now)
            and record.ewma_success >= cfg.fast_min_ewma
            and in_flight < max(1, cfg.fast_max_in_flight)
        )

    def is_slow_eligible(
        self, record: ProxyHealthRecord, now: int | None = None
    ) -> bool:
        """slow 档准入：未冷却即取（dead 只在探测抽签中放行）。"""
        return not record.is_cooling(now)

    # endregion

    async def reset(self) -> int:
        """清空全部健康数据（运维 / 测试用）。"""
        deleted = 0
        try:
            client = self._get_client()
            for state in ProxyHealthState:
                if state is ProxyHealthState.COLD:
                    continue
                deleted += int(await client.delete(self._state_set_key(state)) or 0)
            deleted += int(await client.delete(_COOLING_KEY) or 0)
            cursor = 0
            while True:
                cursor, keys = await client.scan(
                    cursor=cursor, match=f"{_DETAIL_PREFIX}*", count=1000
                )
                if keys:
                    deleted += int(await client.delete(*keys) or 0)
                if cursor == 0:
                    break
        except Exception as e:  # noqa: BLE001
            self._warn(f"清空健康数据失败：{type(e).__name__} {e}")
        self._counts_cache = None
        return deleted


proxy_health_store = ProxyHealthStore()


async def gather_counts() -> ProxyHealthCounts:
    """供日志 / 接口统一取分档数量（永不抛异常）。"""
    return await proxy_health_store.get_counts()


def format_records_for_log(
    records: Iterable[tuple[str, ProxyHealthRecord]]
) -> str:
    """调试用：把若干代理的健康状态拼成一行日志。"""
    parts = [
        f"{key}->{record.state.value}(cf={record.consecutive_fail},"
        f"ewma={record.ewma_success:.2f},cool={record.remaining_cooldown()}s)"
        for key, record in records
    ]
    return " | ".join(parts)


if __name__ == "__main__":

    async def _demo():
        store = proxy_health_store
        await store.reset()
        print(await store.get_counts(force=True))
        await store.record_failure("http://1.1.1.1:1", ProxyFailureKind.NETWORK)
        await store.record_failure("http://1.1.1.1:1", ProxyFailureKind.NETWORK)
        await store.record_failure("http://1.1.1.1:1", ProxyFailureKind.NETWORK)
        print(await store.get("http://1.1.1.1:1"))
        await store.record_failure("http://2.2.2.2:2", ProxyFailureKind.RISK_CONTROL)
        print(await store.get("http://2.2.2.2:2"))
        print(await store.get_counts(force=True))
        print(await store.list_cooling_keys())
        print((await store.get_counts(force=True)).model_dump_json())

    asyncio.run(_demo())
