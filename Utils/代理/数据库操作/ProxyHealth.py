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

## 前缀级（网段）维度

地址级状态机对「换地址 = 同 IP」的出口无效（自建 go-proxy 每拨号在同 /64 内随机选地址，
风控按 /64 聚合；public 池也常整段同生共死）。因此再维护一层**前缀级**状态：

- IPv6 按 ``prefix_v6_length``（默认 /64）、IPv4 按 ``prefix_v4_length``（默认 /24）归并，
  见 ``Utils/代理/mdoel/ProxyPrefix.py``；
- **判定看前缀**（准入 / 熔断 / 在途配额），地址级明细只留作日志与排障；
- 412/352 记在前缀上，冷却阶梯 2h → 6h → 24h（``prefix_risk_cooldown_*``），
  同一轮冷却用 ``SET NX`` 互斥，并发命中不会把阶梯瞬间打满；
- 前缀下「被实际用过」的代理串记在 ``prefix_members``，冷却时据此把整段写回 MySQL。

所有方法都是**尽力而为**：Redis 不可用时降级为「无记录」（cold），
绝不把异常抛进业务请求路径（代理池故障不能拖垮爬虫）。
"""

import asyncio
import math
import random
import time
from typing import Iterable, Sequence

import redis.asyncio as aioredis
from bili_common.models import StrEnumAutoDoc
from pydantic import Field

from CONFIG import CONFIG
from log.base_log import sql_log
from Models.base.custom_pydantic import CustomBaseModel
from Utils.代理.mdoel.ProxyPrefix import proxy_prefix

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
#: 前缀级明细 hash 前缀（IPv6 /64、IPv4 /24 归并后的一条记录）
_PREFIX_DETAIL_PREFIX = "proxy_health:v1:prefix_detail:"
#: 前缀级状态集合 key 前缀
_PREFIX_STATE_SET_PREFIX = "proxy_health:v1:prefix_state:"
#: 前缀级冷却集合（zset：member=前缀，score=冷却到期时间戳）
_PREFIX_COOLING_KEY = "proxy_health:v1:prefix_cooling"
#: 前缀在途并发计数 key 前缀
_PREFIX_IN_FLIGHT_PREFIX = "proxy_health:v1:prefix_inflight:"
#: 前缀 → 该段内被实际使用过的代理串（冷却时把整段写回 MySQL 的依据）
_PREFIX_MEMBERS_PREFIX = "proxy_health:v1:prefix_members:"
#: 风控冷却「一轮」的互斥标记：同一段并发命中 412 时只算一次阶梯升级
_PREFIX_EPISODE_PREFIX = "proxy_health:v1:prefix_episode:"
#: 通道 QPS 配额（固定 1 秒窗口）
_QPS_PREFIX = "proxy_health:v1:qps:"
#: 单个前缀的成员集合最多保留多少条（防止异常段无限膨胀）
_PREFIX_MEMBERS_MAX = 512


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
    """分档数量快照（供调度做背压 / 供接口与日志观测）。

    前缀口径（``prefix_*``）才是「实际可用性」：地址级 hot 里有一大批是
    「同一 /24 同生共死」的成员，整段冷却时它们一条都取不到。
    """

    cold: int = Field(default=0, description="无健康记录（未验证）的代理数，需抽样估算")
    hot: int = 0
    warm: int = 0
    suspect: int = 0
    quarantine: int = 0
    dead: int = 0
    cooling: int = Field(default=0, description="当前处于冷却中的代理数")
    # ---- 前缀口径（IPv6 /64、IPv4 /24 归并）----
    prefix_hot: int = 0
    prefix_warm: int = 0
    prefix_suspect: int = 0
    prefix_quarantine: int = 0
    prefix_dead: int = 0
    prefix_cooling: int = Field(default=0, description="当前处于冷却中的前缀数")
    update_ts: int = 0

    @property
    def known_total(self) -> int:
        return self.hot + self.warm + self.suspect + self.quarantine + self.dead

    @property
    def usable_total(self) -> int:
        """未被冷却、可参与取用的数量（hot + warm）。"""
        return self.hot + self.warm

    @property
    def prefix_known_total(self) -> int:
        return (
            self.prefix_hot
            + self.prefix_warm
            + self.prefix_suspect
            + self.prefix_quarantine
            + self.prefix_dead
        )

    def to_log_str(self) -> str:
        return (
            f"hot={self.hot} warm={self.warm} suspect={self.suspect} "
            f"quarantine={self.quarantine} dead={self.dead} "
            f"cooling={self.cooling} cold={self.cold} | "
            f"prefix(hot={self.prefix_hot} warm={self.prefix_warm} "
            f"suspect={self.prefix_suspect} quarantine={self.prefix_quarantine} "
            f"dead={self.prefix_dead} cooling={self.prefix_cooling})"
        )


class ProxyPrefixRecord(ProxyHealthRecord):
    """前缀级健康明细（IPv6 /64、IPv4 /24 归并后的一条记录）。

    字段复用地址级（state / cf / ewma / cd / 计数），只多一个 ``risk_level``：
    风控冷却阶梯档位（0 = 还没被风控命中过），每进入一轮新冷却升一级、成功后回落一级。
    """

    risk_level: int = Field(default=0, description="风控冷却阶梯档位（0 起）")


class ProxyPrefixMap(CustomBaseModel):
    """一批前缀的健康记录（field=前缀串）。缺失的按「无记录 = 未冷却」处理。"""

    records: dict[str, ProxyPrefixRecord] = Field(default_factory=dict)

    def get(self, prefix: str) -> ProxyPrefixRecord:
        return self.records.get(prefix) or ProxyPrefixRecord()

    def has(self, prefix: str) -> bool:
        return prefix in self.records

    def __len__(self) -> int:
        return len(self.records)


class ProxySelectionState(CustomBaseModel):
    """一次选代理用的健康快照（地址级明细 + 前缀级闸门）。

    选代理时必须保持「随机主键起点 O(1)」：这里只对**已取回的候选批**做批量读
    （一条 pipeline），逐条判定都在内存里完成，不新增任何全表扫描。
    """

    address: ProxyHealthMap = Field(default_factory=ProxyHealthMap)
    address_in_flight: ProxyInFlightMap = Field(default_factory=ProxyInFlightMap)
    prefixes: ProxyPrefixMap = Field(default_factory=ProxyPrefixMap)
    prefix_in_flight: ProxyInFlightMap = Field(default_factory=ProxyInFlightMap)
    excluded_prefixes: frozenset[str] = Field(
        default_factory=frozenset, description="本任务内已失败代理所在的前缀（同段不再换地址重试）"
    )

    def prefix_allows(self, prefix: str, now: int | None = None) -> bool:
        """该前缀是否放行（冷却 / 在途配额 / 本任务排除，三者任一命中即拒）。"""
        if not prefix:
            return True
        if prefix in self.excluded_prefixes:
            return False
        return proxy_health_store.is_prefix_eligible(
            self.prefixes.get(prefix), self.prefix_in_flight.get(prefix), now
        )


class PrefixFailurePlan(CustomBaseModel):
    """一次前缀级失败要落地的变更（把「读 → 决策 → 写」拆开，便于单测）。

    - ``claim_episode``：需要抢占「本轮风控冷却」的互斥标记；
    - ``cooldown``：本次要施加的冷却秒数（0 = 不冷却，只累加计数）；
    - ``next_risk_level``：抢占成功后要写入的阶梯档位。
    """

    state: ProxyHealthState = Field(default=ProxyHealthState.COLD)
    cooldown: int = Field(default=0, description="本次施加的冷却秒数，0 表示不冷却")
    claim_episode: bool = Field(default=False, description="是否需要抢占本轮冷却的互斥标记")
    next_risk_level: int = Field(default=0, description="抢占成功后写入的阶梯档位")
    risk_control: bool = Field(default=False, description="是否为 412/352 类风控信号")


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

    @staticmethod
    def _prefix_detail_key(prefix: str) -> str:
        return f"{_PREFIX_DETAIL_PREFIX}{prefix}"

    @staticmethod
    def _prefix_state_set_key(state: ProxyHealthState) -> str:
        return f"{_PREFIX_STATE_SET_PREFIX}{state.value}"

    @staticmethod
    def _prefix_in_flight_key(prefix: str) -> str:
        return f"{_PREFIX_IN_FLIGHT_PREFIX}{prefix}"

    @staticmethod
    def _prefix_members_key(prefix: str) -> str:
        return f"{_PREFIX_MEMBERS_PREFIX}{prefix}"

    @staticmethod
    def _prefix_episode_key(prefix: str) -> str:
        return f"{_PREFIX_EPISODE_PREFIX}{prefix}"

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

    async def get_prefix(self, prefix: str) -> ProxyPrefixRecord:
        """单个前缀的明细（无记录 = 未冷却，返回默认值）。"""
        if not prefix:
            return ProxyPrefixRecord()
        try:
            client = self._get_client()
            raw = await client.hgetall(self._prefix_detail_key(prefix))
        except Exception as e:  # noqa: BLE001
            self._warn(f"读取前缀明细失败：{type(e).__name__} {e}")
            return ProxyPrefixRecord()
        return self._parse_prefix(raw)

    async def get_prefixes(self, prefixes: Sequence[str]) -> ProxyPrefixMap:
        """批量读取前缀明细（选代理时一次 pipeline 取回，O(候选数)）。"""
        unique = [p for p in dict.fromkeys(prefixes) if p]
        if not unique:
            return ProxyPrefixMap()
        try:
            client = self._get_client()
            async with client.pipeline(transaction=False) as pipe:
                for prefix in unique:
                    pipe.hgetall(self._prefix_detail_key(prefix))
                results = await pipe.execute()
        except Exception as e:  # noqa: BLE001
            self._warn(f"批量读取前缀明细失败：{type(e).__name__} {e}")
            return ProxyPrefixMap()
        return ProxyPrefixMap(
            records={
                prefix: self._parse_prefix(raw)
                for prefix, raw in zip(unique, results)
                if raw
            }
        )

    def _parse_prefix(self, raw: dict) -> ProxyPrefixRecord:
        if not raw:
            return ProxyPrefixRecord()
        base = self._parse(raw)
        try:
            return ProxyPrefixRecord(
                **base.model_dump(),
                risk_level=int(raw.get("rl") or 0),
            )
        except Exception:  # noqa: BLE001 脏数据按 cold 处理
            return ProxyPrefixRecord()

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
        ordered = [s for s in ProxyHealthState if s is not ProxyHealthState.COLD]
        try:
            client = self._get_client()
            async with client.pipeline(transaction=False) as pipe:
                for state in ordered:
                    pipe.scard(self._state_set_key(state))
                pipe.zcount(_COOLING_KEY, ts, "+inf")
                for state in ordered:
                    pipe.scard(self._prefix_state_set_key(state))
                pipe.zcount(_PREFIX_COOLING_KEY, ts, "+inf")
                results = await pipe.execute()
        except Exception as e:  # noqa: BLE001
            self._warn(f"统计分档数量失败：{type(e).__name__} {e}")
            return cached or counts
        # 顺序与上面 pipe 的顺序一一对应（跳过 cold）
        split = len(ordered) + 1
        for state, value in zip(ordered, results[: len(ordered)]):
            setattr(counts, state.value, int(value or 0))
        counts.cooling = int(results[len(ordered)] or 0)
        for state, value in zip(ordered, results[split:-1]):
            setattr(counts, f"prefix_{state.value}", int(value or 0))
        counts.prefix_cooling = int(results[-1] or 0)
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

    async def read_selection_state(
        self,
        proxy_keys: Sequence[str],
        prefixes: Sequence[str],
        excluded_prefixes: frozenset[str] | set[str] = frozenset(),
    ) -> ProxySelectionState:
        """选代理用的健康快照：**一次 pipeline** 取回地址级 + 前缀级（各含在途）。

        等价于原来的 ``get_many`` + ``get_in_flight``，再叠加前缀维度；
        合并成一条 pipeline 是为了不让「加一层前缀判定」变成 4 次 Redis 往返。
        """
        keys = [k for k in dict.fromkeys(proxy_keys) if k]
        unique_prefixes = [p for p in dict.fromkeys(prefixes) if p]
        if not keys and not unique_prefixes:
            return ProxySelectionState(excluded_prefixes=frozenset(excluded_prefixes))
        order: list[str] = []
        try:
            client = self._get_client()
            async with client.pipeline(transaction=False) as pipe:
                for key in keys:
                    pipe.hgetall(self._detail_key(key))
                    order.append("address")
                if keys:
                    pipe.mget([self._in_flight_key(k) for k in keys])
                    order.append("address_flight")
                for prefix in unique_prefixes:
                    pipe.hgetall(self._prefix_detail_key(prefix))
                    order.append("prefix")
                if unique_prefixes:
                    pipe.mget([self._prefix_in_flight_key(p) for p in unique_prefixes])
                    order.append("prefix_flight")
                results = await pipe.execute()
        except Exception as e:  # noqa: BLE001
            self._warn(f"批量读取选代理状态失败：{type(e).__name__} {e}")
            return ProxySelectionState(excluded_prefixes=frozenset(excluded_prefixes))
        address_records: dict[str, ProxyHealthRecord] = {}
        address_counts: dict[str, int] = {}
        prefix_records: dict[str, ProxyPrefixRecord] = {}
        prefix_counts: dict[str, int] = {}
        address_cursor = 0
        prefix_cursor = 0
        for kind, value in zip(order, results):
            match kind:
                case "address":
                    key = keys[address_cursor]
                    address_cursor += 1
                    if value:
                        address_records[key] = self._parse(value)
                case "address_flight":
                    address_counts = {
                        key: int(count or 0)
                        for key, count in zip(keys, value or [])
                    }
                case "prefix":
                    prefix = unique_prefixes[prefix_cursor]
                    prefix_cursor += 1
                    if value:
                        prefix_records[prefix] = self._parse_prefix(value)
                case "prefix_flight":
                    prefix_counts = {
                        prefix: int(count or 0)
                        for prefix, count in zip(unique_prefixes, value or [])
                    }
        return ProxySelectionState(
            address=ProxyHealthMap(records=address_records),
            address_in_flight=ProxyInFlightMap(counts=address_counts),
            prefixes=ProxyPrefixMap(records=prefix_records),
            prefix_in_flight=ProxyInFlightMap(counts=prefix_counts),
            excluded_prefixes=frozenset(excluded_prefixes),
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
        prefix = proxy_prefix_of(proxy_key)
        try:
            client = self._get_client()
            async with client.pipeline(transaction=False) as pipe:
                for state in ProxyHealthState:
                    pipe.srem(self._state_set_key(state), proxy_key)
                pipe.delete(self._detail_key(proxy_key))
                if prefix:
                    # 同时从「前缀成员集合」摘掉：它已经不可用，
                    # 前缀冷却时不该再为它回写 MySQL。
                    pipe.srem(self._prefix_members_key(prefix), proxy_key)
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

    async def get_prefix_in_flight(self, prefixes: Sequence[str]) -> ProxyInFlightMap:
        """批量取前缀在途并发数（MGET 单次往返）。"""
        unique = [p for p in dict.fromkeys(prefixes) if p]
        if not unique:
            return ProxyInFlightMap()
        try:
            client = self._get_client()
            values = await client.mget(
                [self._prefix_in_flight_key(p) for p in unique]
            )
        except Exception as e:  # noqa: BLE001
            self._warn(f"读取前缀在途计数失败：{type(e).__name__} {e}")
            return ProxyInFlightMap()
        return ProxyInFlightMap(
            counts={p: int(value or 0) for p, value in zip(unique, values)}
        )

    async def acquire_prefix_in_flight(self, prefix: str) -> None:
        """同前缀在途 +1（TTL 兜底，进程崩溃自动释放）。"""
        if not prefix:
            return
        try:
            client = self._get_client()
            key = self._prefix_in_flight_key(prefix)
            async with client.pipeline(transaction=False) as pipe:
                pipe.incr(key)
                pipe.expire(key, max(1, self.cfg.in_flight_ttl))
                await pipe.execute()
        except Exception as e:  # noqa: BLE001
            self._warn(f"前缀在途 +1 失败：{type(e).__name__} {e}")

    async def release_prefix_in_flight(self, prefix: str) -> None:
        if not prefix:
            return
        try:
            client = self._get_client()
            key = self._prefix_in_flight_key(prefix)
            value = await client.decr(key)
            if value is not None and value <= 0:
                await client.delete(key)
        except Exception as e:  # noqa: BLE001
            self._warn(f"前缀在途 -1 失败：{type(e).__name__} {e}")

    # endregion

    # region 前缀级读取 / 冷却
    async def is_prefix_cooling(self, prefix: str) -> bool:
        """该前缀当前是否在冷却（专线闸门用，一次 HGET）。"""
        if not prefix:
            return False
        try:
            client = self._get_client()
            raw = await client.hget(self._prefix_detail_key(prefix), "cd")
        except Exception as e:  # noqa: BLE001
            self._warn(f"读取前缀冷却失败：{type(e).__name__} {e}")
            return False
        return int(float(raw or 0)) > int(time.time())

    async def list_prefix_cooling(self, limit: int = 1000) -> list[str]:
        """当前仍在冷却的前缀（按到期时间升序，最多 ``limit`` 条）。"""
        now = int(time.time())
        try:
            client = self._get_client()
            await client.zremrangebyscore(_PREFIX_COOLING_KEY, "-inf", now)
            return await client.zrangebyscore(
                _PREFIX_COOLING_KEY, now, "+inf", start=0, num=limit
            )
        except Exception as e:  # noqa: BLE001
            self._warn(f"读取前缀冷却集合失败：{type(e).__name__} {e}")
            return []

    async def prefix_members(self, prefix: str, limit: int = _PREFIX_MEMBERS_MAX) -> list[str]:
        """该前缀下被实际使用过的代理串（冷却时据此把整段写回 MySQL）。"""
        if not prefix:
            return []
        try:
            client = self._get_client()
            return await client.srandmember(
                self._prefix_members_key(prefix), max(1, limit)
            ) or []
        except Exception as e:  # noqa: BLE001
            self._warn(f"读取前缀成员失败：{type(e).__name__} {e}")
            return []

    # endregion

    # region 通道 QPS 配额
    async def acquire_qps(
        self, channel: str, per_second: float, max_wait: float = 1.0
    ) -> bool:
        """通道级 QPS 配额（固定 1 秒窗口，供自建出口这类「专线」限流）。

        - ``per_second <= 0`` 表示不限制；
        - 单窗口允许 ``ceil(per_second)`` 次（Redis 的 TTL 最小 1s，做不到更细的滑动窗口）；
        - 超配额时最多等待 ``max_wait`` 秒，仍拿不到就返回 False（由调用方快速失败，
          而不是把请求挂在这里）。Redis 不可用时**放行**：限流是保护措施，
          不能因为缓存故障把整条业务链路堵死。
        """
        if per_second <= 0 or not channel:
            return True
        allowance = max(1, math.ceil(per_second))
        deadline = time.monotonic() + max(0.0, max_wait)
        try:
            client = self._get_client()
        except Exception as e:  # noqa: BLE001
            self._warn(f"QPS 配额不可用：{type(e).__name__} {e}")
            return True
        while True:
            window = int(time.time())
            key = f"{_QPS_PREFIX}{channel}:{window}"
            try:
                count = int(await client.incr(key) or 0)
                if count == 1:
                    await client.expire(key, 2)
            except Exception as e:  # noqa: BLE001
                self._warn(f"QPS 配额计数失败：{type(e).__name__} {e}")
                return True
            if count <= allowance:
                return True
            remaining = min(window + 1 - time.time(), deadline - time.monotonic())
            if remaining <= 0:
                return False
            await asyncio.sleep(remaining + 0.005)

    # endregion

    # region 状态迁移
    async def record_success(
        self, proxy_key: str, previous: ProxyHealthRecord | None = None
    ) -> ProxyHealthRecord:
        """成功：地址级与中国级一起更新（一次读 + 一次写，不额外增加往返）。

        - 地址级：EWMA 上升、连续失败清零、状态回 hot（明细，供日志 / 排障）；
        - 前缀级：同段一起恢复；但**风控冷却期内不解冻**（见 ``_prefix_success_transition``）。
        """
        if not proxy_key:
            return ProxyHealthRecord()
        prefix = proxy_prefix_of(proxy_key)
        now = int(time.time())
        record, prefix_record = await self._read_pair(proxy_key, prefix, previous)
        # 必须在改字段**之前**记下旧状态：previous 与 record 常常是同一个对象，
        # 改完再读 previous.state 只会拿到 hot，旧状态集合就清不掉了。
        previous_state = record.state
        prefix_previous_state = prefix_record.state
        self._success_transition(record, now)
        if prefix:
            self._prefix_success_transition(prefix_record, now)
        await self._save_pair(
            proxy_key=proxy_key,
            record=record,
            previous_state=previous_state,
            prefix=prefix,
            prefix_record=prefix_record,
            prefix_previous_state=prefix_previous_state,
        )
        return record

    async def record_failure(
        self,
        proxy_key: str,
        kind: ProxyFailureKind = ProxyFailureKind.NETWORK,
        previous: ProxyHealthRecord | None = None,
    ) -> ProxyHealthRecord:
        """失败：地址级降档 + **前缀级冷却**。

        412/352 是段级风控信号：冷却记在 /64、/24 上（阶梯 2h→6h→24h），
        同段内的其它地址一并被挡，避免「换个地址继续撞墙」。
        """
        if not proxy_key:
            return ProxyHealthRecord()
        prefix = proxy_prefix_of(proxy_key)
        now = int(time.time())
        record, prefix_record = await self._read_pair(proxy_key, prefix, previous)
        previous_state = record.state
        self._failure_transition(record, kind, now)
        prefix_previous_state = prefix_record.state
        plan = self.plan_prefix_failure(prefix_record, kind, now)
        # 抢不到「本轮冷却」时不能写 state / cd / rl：并发命中 412 时，
        # 后到的请求读到的是旧值，直接写回去会把别人刚施加的冷却与阶梯冲掉。
        write_prefix_state = True
        if prefix:
            claimed = True
            if plan.claim_episode:
                claimed = await self._claim_prefix_episode(prefix, plan.cooldown)
            self._apply_prefix_failure(prefix_record, plan, claimed, kind, now)
            write_prefix_state = (not plan.claim_episode) or claimed
            # 同一轮冷却只打印一次：并发命中时只有抢到标记的那条会升级阶梯，
            # 日志里就能看到「该前缀进入冷却」唯一一条，便于核对「不再换地址撞墙」。
            if plan.claim_episode and claimed:
                sql_log.warning(
                    f"前缀 {prefix} 命中风控（{kind.value}）：冷却 {plan.cooldown}s"
                    f"（阶梯 {plan.next_risk_level}/{len(self.prefix_risk_ladder())}），"
                    f"同段内不再换地址重试；触发地址 {proxy_key}"
                )
        await self._save_pair(
            proxy_key=proxy_key,
            record=record,
            previous_state=previous_state,
            prefix=prefix,
            prefix_record=prefix_record,
            prefix_previous_state=prefix_previous_state,
            prefix_write_state=write_prefix_state,
        )
        return record

    def _success_transition(self, record: ProxyHealthRecord, now: int) -> None:
        """地址级成功迁移（纯计算，便于单测）。"""
        alpha = min(max(self.cfg.ewma_alpha, 0.01), 1.0)
        record.ewma_success = min(1.0, record.ewma_success * (1 - alpha) + alpha)
        record.consecutive_fail = 0
        record.success_total += 1
        record.state = ProxyHealthState.HOT
        record.cooldown_until = 0
        record.last_kind = None
        record.update_ts = now

    def _prefix_success_transition(
        self, record: ProxyPrefixRecord, now: int
    ) -> None:
        """前缀级成功迁移（纯计算）。

        风控冷却期内的成功**不解冻**：那些请求是冷却生效前发出去的，
        用它们把 2h→6h→24h 的惩罚抹掉，等于给风控开了后门。
        """
        alpha = min(max(self.cfg.ewma_alpha, 0.01), 1.0)
        record.ewma_success = min(1.0, record.ewma_success * (1 - alpha) + alpha)
        record.consecutive_fail = 0
        record.success_total += 1
        record.update_ts = now
        if record.last_kind is ProxyFailureKind.RISK_CONTROL and record.is_cooling(now):
            return
        record.state = ProxyHealthState.HOT
        record.cooldown_until = 0
        record.last_kind = None
        record.risk_level = max(0, record.risk_level - 1)

    def _failure_transition(
        self, record: ProxyHealthRecord, kind: ProxyFailureKind, now: int
    ) -> None:
        """地址级失败迁移（纯计算）：降档 + 指数冷却，风控层按 IP 长冷却。"""
        alpha = min(max(self.cfg.ewma_alpha, 0.01), 1.0)
        record.ewma_success = max(0.0, record.ewma_success * (1 - alpha))
        record.consecutive_fail += 1
        record.fail_total += 1
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

    def plan_prefix_failure(
        self,
        record: ProxyPrefixRecord,
        kind: ProxyFailureKind,
        now: int | None = None,
    ) -> PrefixFailurePlan:
        """前缀级失败决策（纯计算）。

        - 412/352：QUARANTINE + 阶梯冷却，需要抢占「本轮冷却」标记；
        - 网络层：连续失败达到 ``prefix_network_fail_threshold`` 才冷却，
          时长沿用 10~30min 区间（单个坏代理掉线不该停掉整段）。
        """
        if kind is ProxyFailureKind.RISK_CONTROL:
            level = min(max(record.risk_level, 0), len(self.prefix_risk_ladder()) - 1)
            return PrefixFailurePlan(
                state=ProxyHealthState.QUARANTINE,
                cooldown=self.prefix_risk_cooldown(level),
                claim_episode=True,
                next_risk_level=min(
                    record.risk_level + 1, len(self.prefix_risk_ladder()) - 1
                ),
                risk_control=True,
            )
        fails = record.consecutive_fail + 1
        if fails < max(1, self.cfg.prefix_network_fail_threshold):
            # 还没到段级信号：只累加计数（state 保持原值，不冷却、不改分档集合）
            return PrefixFailurePlan(state=record.state, cooldown=0)
        state = self._state_for_fails(fails)
        return PrefixFailurePlan(
            state=state, cooldown=self._network_cooldown(fails, state)
        )

    def _apply_prefix_failure(
        self,
        record: ProxyPrefixRecord,
        plan: PrefixFailurePlan,
        claimed: bool,
        kind: ProxyFailureKind,
        now: int,
    ) -> None:
        """把前缀级失败决策落到记录上（纯计算，写库由 ``_save_pair`` 统一做）。"""
        alpha = min(max(self.cfg.ewma_alpha, 0.01), 1.0)
        record.ewma_success = max(0.0, record.ewma_success * (1 - alpha))
        record.consecutive_fail += 1
        record.fail_total += 1
        if plan.risk_control:
            record.quarantine_total += 1
        if plan.claim_episode:
            record.state = plan.state
            if claimed:
                record.risk_level = plan.next_risk_level
                record.cooldown_until = now + plan.cooldown
            else:
                # 同一轮冷却内的重复命中：不升级阶梯，但保底不弱于当前阶梯
                record.cooldown_until = max(
                    record.cooldown_until, now + plan.cooldown
                )
        elif plan.cooldown:
            record.state = plan.state
            # 冷却只延长不缩短：并发失败时后到的请求不能把别人的冷却改小
            record.cooldown_until = max(record.cooldown_until, now + plan.cooldown)
        record.last_kind = kind
        record.update_ts = now

    def prefix_risk_ladder(self) -> tuple[int, int, int]:
        """风控冷却阶梯（2h → 6h → 24h，全部可配；最后一级即上限）。"""
        cfg = self.cfg
        first = max(1, int(cfg.prefix_risk_cooldown_first))
        second = max(first, int(cfg.prefix_risk_cooldown_second))
        cap = max(second, int(cfg.prefix_risk_cooldown_max))
        return (first, second, cap)

    def prefix_risk_cooldown(self, risk_level: int) -> int:
        """按阶梯档位取冷却秒数（超出末级按上限，不允许无限增长）。"""
        ladder = self.prefix_risk_ladder()
        return ladder[min(max(int(risk_level), 0), len(ladder) - 1)]

    async def _claim_prefix_episode(self, prefix: str, cooldown: int) -> bool:
        """抢占「本轮风控冷却」的互斥标记（``SET NX EX``）。

        并发命中 412 时只有第一条会升级阶梯，其余只累加计数；
        否则同一秒内的 10 个在途请求会把 2h→6h→24h 直接打满。
        """
        if not prefix:
            return False
        try:
            client = self._get_client()
            claimed = await client.set(
                self._prefix_episode_key(prefix),
                int(time.time()),
                nx=True,
                ex=max(60, int(cooldown)),
            )
        except Exception as e:  # noqa: BLE001
            self._warn(f"抢占前缀冷却标记失败：{type(e).__name__} {e}")
            # 缓存异常时按「已抢占」处理：宁可多升一级，也不要漏掉风控信号
            return True
        return bool(claimed)

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
        """只写地址级明细（前缀维度由 ``_save_pair`` 负责）。"""
        try:
            client = self._get_client()
            async with client.pipeline(transaction=False) as pipe:
                self._queue_state_write(pipe, proxy_key, record, previous_state)
                await pipe.execute()
        except Exception as e:  # noqa: BLE001
            self._warn(f"写入健康记录失败：{type(e).__name__} {e}")

    @classmethod
    def _queue_state_write(
        cls,
        pipe,
        proxy_key: str,
        record: ProxyHealthRecord,
        previous_state: ProxyHealthState | None = None,
    ) -> None:
        """把地址级写入排进 pipeline：明细 + 状态集合 + 冷却 zset。"""
        detail_key = cls._detail_key(proxy_key)
        new_set = cls._state_set_key(record.state)
        pipe.hset(detail_key, mapping=cls._record_mapping(record))
        pipe.expire(detail_key, _RECORD_TTL)
        # 状态集合：旧状态移除、新状态加入（旧状态未知时全量移除一次）
        states = (
            [previous_state] if previous_state is not None else list(ProxyHealthState)
        )
        for state in states:
            if state is ProxyHealthState.COLD or state is record.state:
                continue
            pipe.srem(cls._state_set_key(state), proxy_key)
        pipe.sadd(new_set, proxy_key)
        pipe.expire(new_set, _RECORD_TTL)
        if record.cooldown_until:
            pipe.zadd(_COOLING_KEY, {proxy_key: record.cooldown_until})
        else:
            pipe.zrem(_COOLING_KEY, proxy_key)

    async def _read_pair(
        self,
        proxy_key: str,
        prefix: str,
        previous: ProxyHealthRecord | None,
    ) -> tuple[ProxyHealthRecord, ProxyPrefixRecord]:
        """一次 pipeline 读取「地址 + 前缀」两条明细（各自失败时降级为 cold）。"""
        wants: list[str] = []
        if previous is None:
            wants.append("address")
        if prefix:
            wants.append("prefix")
        if not wants:
            return previous or ProxyHealthRecord(), ProxyPrefixRecord()
        try:
            client = self._get_client()
            async with client.pipeline(transaction=False) as pipe:
                if previous is None:
                    pipe.hgetall(self._detail_key(proxy_key))
                if prefix:
                    pipe.hgetall(self._prefix_detail_key(prefix))
                results = await pipe.execute()
        except Exception as e:  # noqa: BLE001
            self._warn(f"读取健康明细失败：{type(e).__name__} {e}")
            return previous or ProxyHealthRecord(), ProxyPrefixRecord()
        raw_by_kind = dict(zip(wants, results))
        address = (
            previous
            if previous is not None
            else self._parse(raw_by_kind.get("address") or {})
        )
        return address, self._parse_prefix(raw_by_kind.get("prefix") or {})

    async def _save_pair(
        self,
        proxy_key: str,
        record: ProxyHealthRecord,
        previous_state: ProxyHealthState | None,
        prefix: str,
        prefix_record: ProxyPrefixRecord,
        prefix_previous_state: ProxyHealthState | None,
        prefix_write_state: bool = True,
    ) -> None:
        """一次 pipeline 写完「地址 + 前缀」两级状态。"""
        try:
            client = self._get_client()
            async with client.pipeline(transaction=False) as pipe:
                self._queue_state_write(pipe, proxy_key, record, previous_state)
                if prefix:
                    self._queue_prefix_write(
                        pipe,
                        proxy_key,
                        prefix,
                        prefix_record,
                        prefix_previous_state,
                        write_state=prefix_write_state,
                    )
                await pipe.execute()
        except Exception as e:  # noqa: BLE001
            self._warn(f"写入健康记录失败：{type(e).__name__} {e}")

    def _queue_prefix_write(
        self,
        pipe,
        proxy_key: str,
        prefix: str,
        record: ProxyPrefixRecord,
        previous_state: ProxyHealthState | None,
        write_state: bool = True,
    ) -> None:
        """把前缀级写入排进 pipeline。

        ``write_state=False`` 时只写计数 / EWMA / 时间戳，**不写** state / cd / rl：
        并发命中同一轮风控时，后到的请求读到的是旧值，写回会把冷却与阶梯冲掉。
        """
        detail_key = self._prefix_detail_key(prefix)
        pipe.hset(
            detail_key,
            mapping=self._prefix_record_mapping(record, write_state=write_state),
        )
        pipe.expire(detail_key, _RECORD_TTL)
        if write_state:
            new_set = self._prefix_state_set_key(record.state)
            states = (
                [previous_state]
                if previous_state is not None
                else list(ProxyHealthState)
            )
            for state in states:
                if state is ProxyHealthState.COLD or state is record.state:
                    continue
                pipe.srem(self._prefix_state_set_key(state), prefix)
            pipe.sadd(new_set, prefix)
            pipe.expire(new_set, _RECORD_TTL)
            if record.cooldown_until:
                pipe.zadd(_PREFIX_COOLING_KEY, {prefix: record.cooldown_until})
            else:
                pipe.zrem(_PREFIX_COOLING_KEY, prefix)
        # 成员集合：前缀冷却时据此把整段写回 MySQL（只记「实际用过」的代理）
        members_key = self._prefix_members_key(prefix)
        pipe.sadd(members_key, proxy_key)
        pipe.expire(members_key, _RECORD_TTL)

    @staticmethod
    def _prefix_record_mapping(
        record: ProxyPrefixRecord, write_state: bool = True
    ) -> dict[str, str | int | float]:
        """前缀记录 → Redis hash 字段（与地址级共用短字段名）。"""
        mapping: dict[str, str | int | float] = {
            "cf": record.consecutive_fail,
            "ewma": round(record.ewma_success, 6),
            "ok": record.success_total,
            "fail": record.fail_total,
            "qn": record.quarantine_total,
            "ts": record.update_ts,
            "kind": record.last_kind.value if record.last_kind else "",
        }
        if write_state:
            mapping["state"] = record.state.value
            mapping["cd"] = record.cooldown_until
            mapping["rl"] = record.risk_level
        return mapping

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
                        # 播种时一并登记前缀成员：这样首次前缀冷却就能把整段
                        # 写回 MySQL，不必等这些代理先失败一次。
                        prefix = proxy_prefix_of(key)
                        if prefix:
                            members_key = self._prefix_members_key(prefix)
                            pipe.sadd(members_key, key)
                            pipe.expire(members_key, _RECORD_TTL)
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

    def is_prefix_eligible(
        self,
        record: ProxyPrefixRecord,
        in_flight: int,
        now: int | None = None,
    ) -> bool:
        """前缀级准入：未冷却 + 同段在途未打满（fast / slow 都遵守）。

        - 前缀无记录 = 未验证过、也从未被风控命中 → 放行（新段不该被无条件挡掉）；
        - ``prefix_inflight_limit`` 是段级并发上限：同段并发打满会被整段封，
          宁可让上层快速失败 / 换段，也不要继续加压。
        """
        if record.is_cooling(now):
            return False
        return in_flight < max(1, self.cfg.prefix_inflight_limit)

    # endregion

    async def reset(self) -> int:
        """清空全部健康数据（运维 / 测试用），地址级与前缀级一起清。"""
        deleted = 0
        try:
            client = self._get_client()
            for state in ProxyHealthState:
                if state is ProxyHealthState.COLD:
                    continue
                deleted += int(await client.delete(self._state_set_key(state)) or 0)
                deleted += int(
                    await client.delete(self._prefix_state_set_key(state)) or 0
                )
            deleted += int(await client.delete(_COOLING_KEY) or 0)
            deleted += int(await client.delete(_PREFIX_COOLING_KEY) or 0)
            for pattern in (
                f"{_DETAIL_PREFIX}*",
                f"{_PREFIX_DETAIL_PREFIX}*",
                f"{_PREFIX_MEMBERS_PREFIX}*",
                f"{_PREFIX_EPISODE_PREFIX}*",
                f"{_PREFIX_IN_FLIGHT_PREFIX}*",
                f"{_QPS_PREFIX}*",
            ):
                cursor = 0
                while True:
                    cursor, keys = await client.scan(
                        cursor=cursor, match=pattern, count=1000
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


def proxy_prefix_of(proxy_key: str) -> str:
    """代理串 → 前缀标识（IPv6 /64、IPv4 /24，长度取 ``CONFIG.proxy_health``）。

    空串 / 无法解析时返回空串表示「不参与前缀维度」（调用方跳过前缀读写）。
    """
    if not (proxy_key or "").strip():
        return ""
    cfg = CONFIG.proxy_health
    return proxy_prefix(
        proxy_key,
        v4_length=cfg.prefix_v4_length,
        v6_length=cfg.prefix_v6_length,
    )


def proxy_prefixes_of(proxy_keys: Iterable[str]) -> list[str]:
    """批量取前缀（顺序与入参一一对应，便于与行 / key 对齐 zip）。"""
    return [proxy_prefix_of(key) for key in proxy_keys]


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
