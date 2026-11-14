# -*- coding: utf-8 -*-
"""
异步sqlalchemy操作方法
"""

from bili_common.models import StrEnumAutoDoc
import ast
import asyncio
import datetime
import json
import random
import time
from typing import List, Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field
from sqlalchemy import select, func, update, and_, or_, delete
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from CONFIG import CONFIG
from dao.base.sqlHelperBase import SqlHelperBase
from log.base_log import sql_log
from Models.v1.background_service.background_service_model import ProxyStatusResp
from Utils.通用.Common import GLOBAL_SCHEDULER, log_sql_retry_wrapper, asyncio_gather
from Utils.数据库.SqlalchemyTool import sqlalchemy_model_2_dict
from Utils.redisTool.RedisManager import RedisManagerBase
from Utils.代理.mdoel.ProxyTier import ProxyTier
from Utils.代理.数据库操作.ProxyHealth import (
    ProxyHealthCounts,
    ProxyHealthRecord,
    ProxyHealthState,
    proxy_health_store,
    seed_health_record,
)
from Utils.代理.数据库操作.SqlAlcheyObj.ProxyModel import ProxyTab
from Utils.代理.数据库操作.comm import get_scheme_ip_port_form_proxy_dict

MIN_REFRESH_SUCCESS_TIME = -3  # 最低允许刷新状态的代理获取请求成功次数
MIN_REFRESH_SCORE = 0  # 最低允许刷新状态的代理分数
DEFAULT_CHUNK_SIZE = 1000  # Adjust as needed
# zset 与 MySQL 短暂不一致时，最多换几个候选代理（每次都把自己剔除）
ZSET_ZOMBIE_MAX_RETRY = 3
# 整点重建 zset 的最低条数门槛：低于它宁可不动，避免把代理池清空
ZSET_MIN_REBUILD_SIZE = 300
# 僵尸口径：用过但净失败（success_times < 0），已被真实业务请求反复证明不可用。
# 不以 score 为条件：score 每次失败 -10、下限 10，本库中 score<=0 恒为 0 行
# （见计划书 1.3），用它做过滤条件等于不过滤。
ZOMBIE_SUCCESS_TIMES_MAX = 0
# fresh 池（新鲜度优先）：只保留最近被源站列出 / 新入库的代理，score = 源站时间戳
FRESH_PROXY_KEEP_SECONDS = 24 * 3600
# 从 fresh 池取代理时，只在「最新的一批」里随机，避免老成员反复被抽中
FRESH_PROXY_PICK_WINDOW = 300
# 代理池概览（GetProxyStatus）的进程内缓存 TTL（秒）。
# 4 个计数都是 158 万行表的全量聚合，冷查询合计 300~400ms；代理池本身是分钟级变化，
# 30s 缓存对可观测性没有影响，又足以让轮询该接口的前端 / 网关稳定拿到毫秒级响应。
# 刻意用进程内缓存而不是 Redis：代理池 Redis 已整体废弃（见 ProxyCommOp 模块注释）。
PROXY_STATUS_CACHE_TTL_SECONDS = 30

database = CONFIG.database


def is_zombie_success_times(success_times: int | None) -> bool:
    """``success_times`` 是否代表「用过但从未成功」的僵尸代理。

    与 SQL 侧 ``coalesce(success_times, 0) < ZOMBIE_SUCCESS_TIMES_MAX`` 口径一致：
    NULL（从未被使用）不算僵尸。
    """
    return (success_times or 0) < ZOMBIE_SUCCESS_TIMES_MAX


class ProxyKeyLookup(BaseModel):
    """一轮抓取前对候选代理做的批量存在性 / 存活检查结果。

    用途：抓取一轮去重后常有 9 万条候选，逐条 ``get_proxy_by_key`` 要 9 万次查询；
    事先用 ``IN (...)`` 批量问一次（每 1000 条一批），后续入库只查内存。
    """

    existing: set[str] = Field(default_factory=set, description="库中已存在的代理串")
    alive: set[str] = Field(
        default_factory=set, description="已存在且未被判死（status=0 且非僵尸）的代理串"
    )

    def is_new(self, proxy_key: str) -> bool:
        return proxy_key not in self.existing

    def is_alive(self, proxy_key: str) -> bool:
        return proxy_key in self.alive


def to_proxy_key(proxy_info_dict: dict | str) -> str | None:
    """代理字典 / JSON 串 → 代理串。

    口径必须与 ``proxy_tab.computed_proxy_str`` 生成列一致（即代理 JSON 对象
    第一个 key 的 value），否则按串等值查不到任何行。
    """
    try:
        if isinstance(proxy_info_dict, str):
            proxy_info_dict = json.loads(proxy_info_dict)
        return get_scheme_ip_port_form_proxy_dict(proxy_info_dict)
    except Exception:
        return str(proxy_info_dict)


def _is_local_ipv6_proxy(proxy_key: str) -> bool:
    """判断代理串是否为「本地 IPv6 代理」（配置项 PROXY_SERVER / CONFIG.my_ipv6_addr）。

    它是配置项而不是代理池成员：既不参与打分，也不该往 proxy_db 写。
    原先这层判断只写在 ``RedisRequestProxy.update_to_proxy_dict`` 里，
    ProxyEvent / grpc_api 等直接调用 ``update_to_proxy_list`` 的路径会绕过它，
    于是本地代理每次被加减分都会走到「MySQL 与 Redis 均不存在」分支刷屏。
    """
    local = (CONFIG.my_ipv6_addr or "").strip().rstrip("/")
    return bool(local) and proxy_key.strip().rstrip("/") == local


def is_local_ipv6_proxy(proxy_key: str) -> bool:
    """对外暴露的只读判断：本地 ipv6 代理不参与打分 / 健康状态统计。"""
    return _is_local_ipv6_proxy(proxy_key)



class SQLHelperClass(SqlHelperBase):
    def __init__(self):
        mysql_db_url = CONFIG.database.MYSQL.proxy_db_URI
        # 爬虫专用连接池，设置 is_crawler=True
        super().__init__(mysql_db_url=mysql_db_url, is_crawler=True)
        self._lock = asyncio.Lock()
        self._underscore_spe_time = 24 * 3600  # 0分以下的无响应代理休眠时间
        self._412_sep_time = 2 * 3600  # 0分以上但是"-412"风控的代理休眠时间
        self.engine.dialect.supports_sane_rowcount = (
            False  # 避免了批量update报错stableData
        )
        GLOBAL_SCHEDULER.add_job(
            self.refresh_proxy,
            "interval",
            seconds=600,
            next_run_time=datetime.datetime.now(),
            misfire_grace_time=600,
        )
        # 冷却中的代理需要持续回写 update_ts（心跳），否则 refresh_proxy 会在 2 小时后
        # 把它们复位成 status=0，风控层的 2~6h 冷却就白设了。周期取
        # PROXY_HEALTH__PERSIST_INTERVAL（默认 60s），远小于 refresh 的 2h 窗口。
        GLOBAL_SCHEDULER.add_job(
            self.flush_proxy_health,
            "interval",
            seconds=max(10, CONFIG.proxy_health.persist_interval),
            next_run_time=datetime.datetime.now(),
            misfire_grace_time=120,
        )
        # 取代理改为「MySQL 随机主键起点」，需要知道 id 范围；
        # 每 10 分钟的任务顺带刷新即可，这里只在首次为空时懒加载。
        self._proxy_id_range: tuple[int, int] | None = None
        self._proxy_id_range_ts: int = 0
        # 代理池概览的进程内缓存（见 PROXY_STATUS_CACHE_TTL_SECONDS）
        self._proxy_status_cache: ProxyStatusResp | None = None
        self._proxy_status_cache_ts: float = 0.0
        # 健康状态冷启动播种是否已做过（进程级，只做一次）
        self._health_bootstrapped: bool = False

    @log_sql_retry_wrapper()
    async def clear_unusable_proxy(self) -> int:
        """清理 MySQL 中彻底不可用的代理（物理删行，不可回滚，口径必须保守）。

        删除条件：``score <= 0 OR success_times < -3``，线上实测长期 0 行
        （score 有 10 的下限，success_times < -3 从未出现），所以实际不删任何数据。

        为什么不按 ``success_times < 0`` 删：那是 100 万行，一次性删除会长时间持锁；
        取代理时的过滤条件（``status=0 且 success_times>=0``）已经在查询层把僵尸排除，
        不需要物理删除。
        """
        deleted_rows = 0
        async with self.async_session() as session:
            # 查找所有将要被删除的 proxy_tab_id
            stmt = select(ProxyTab.proxy_id).where(
                or_(
                    ProxyTab.score <= MIN_REFRESH_SCORE,
                    ProxyTab.success_times < MIN_REFRESH_SUCCESS_TIME,
                )
            )
            result = await session.execute(stmt)
            proxy_ids_to_delete = [row[0] for row in result.all()]

            if proxy_ids_to_delete:
                delete_proxy_tab = delete(ProxyTab).where(
                    ProxyTab.proxy_id.in_(proxy_ids_to_delete)
                )
                res = await session.execute(delete_proxy_tab)

                await session.commit()
                deleted_rows = res.rowcount or 0

        zombie_num = await self.get_zombie_proxy_num()
        if zombie_num:
            sql_log.warning(
                f"代理池清理：本轮 MySQL 删除 {deleted_rows} 行；"
                f"僵尸代理（success_times<0）{zombie_num} 条不参与随机选取"
            )
        return deleted_rows

    async def get_zombie_proxy_num(self) -> int:
        """僵尸代理数量（用过但净失败）。只做观测，不参与删行决策。"""
        sql = select(func.count(ProxyTab.proxy_id)).where(
            func.coalesce(ProxyTab.success_times, 0) < ZOMBIE_SUCCESS_TIMES_MAX
        )
        async with self.async_session() as session:
            res = await session.execute(sql)
        return res.scalars().first() or 0

    # ---------------- 方案 C：代理明细统一回 MySQL ----------------

    async def get_proxy_by_key(self, proxy_key: str) -> ProxyTab | None:
        """按代理串取整行。

        ``proxy_tab.computed_proxy_str`` 是持久化生成列且带索引，等值查走索引，
        单行毫秒级；这是方案 C 下读取代理明细的唯一入口。
        """
        if not proxy_key:
            return None
        sql = select(ProxyTab).where(ProxyTab.computed_proxy_str == proxy_key).limit(1)
        async with self.async_session() as session:
            res = await session.execute(sql)
        return res.scalars().first()

    async def lookup_proxy_keys(self, proxy_keys: list[str]) -> ProxyKeyLookup:
        """批量查询一批代理串是否已在库、是否仍可用（供一轮抓取做内存预筛）。

        只取三列（生成列 + status + success_times），每 1000 条一批走
        ``computed_proxy_str`` 索引，9 万条候选 ≈ 90 次查询，替代 9 万次单条查询。
        """
        lookup = ProxyKeyLookup()
        unique_keys = list(dict.fromkeys(k for k in proxy_keys if k))
        for start in range(0, len(unique_keys), DEFAULT_CHUNK_SIZE):
            chunk = unique_keys[start : start + DEFAULT_CHUNK_SIZE]
            sql = select(
                ProxyTab.computed_proxy_str,
                ProxyTab.status,
                ProxyTab.success_times,
            ).where(ProxyTab.computed_proxy_str.in_(chunk))
            async with self.async_session() as session:
                res = await session.execute(sql)
                for proxy_key, status, success_times in res.all():
                    if not proxy_key:
                        continue
                    lookup.existing.add(proxy_key)
                    if status == 0 and not is_zombie_success_times(success_times):
                        lookup.alive.add(proxy_key)
        return lookup

    async def get_proxies_by_keys(self, proxy_keys: list[str]) -> dict[str, ProxyTab]:
        """批量取整行（``computed_proxy_str`` 等值 / IN 走索引），只返回**仍可用**的行。

        供「从已验证分档取样」后回 MySQL 确认使用：一次 IN 查询，避免 N 次单行查询。
        """
        keys = [k for k in dict.fromkeys(proxy_keys) if k]
        if not keys:
            return {}
        sql = select(ProxyTab).where(
            ProxyTab.computed_proxy_str.in_(keys),
            ProxyTab.status == 0,
            or_(
                ProxyTab.success_times.is_(None),
                ProxyTab.success_times >= ZOMBIE_SUCCESS_TIMES_MAX,
            ),
        )
        async with self.async_session() as session:
            res = await session.execute(sql)
        return {
            row.computed_proxy_str: row
            for row in res.scalars().all()
            if row is not None and row.computed_proxy_str
        }

    @log_sql_retry_wrapper()
    async def select_score_top_proxy(self) -> ProxyTab | None:
        """取分数最高的一条（不再经过 Redis zset）。"""
        sql = select(ProxyTab).order_by(ProxyTab.score.desc()).limit(1)
        async with self.async_session() as session:
            res = await session.execute(sql)
        return res.scalars().first()

    async def _get_proxy_id_range(self) -> tuple[int, int]:
        """``proxy_id`` 的 [min, max]，缓存 1 小时（随机起点要用）。"""
        now = int(time.time())
        if self._proxy_id_range and now - self._proxy_id_range_ts < 3600:
            return self._proxy_id_range
        sql = select(func.min(ProxyTab.proxy_id), func.max(ProxyTab.proxy_id))
        async with self.async_session() as session:
            res = await session.execute(sql)
            row = res.first()
        min_id, max_id = (row[0] or 0, row[1] or 0) if row else (0, 0)
        self._proxy_id_range = (int(min_id), int(max_id))
        self._proxy_id_range_ts = now
        return self._proxy_id_range

    async def _select_candidate_rows(
        self, start_id: int, limit: int
    ) -> list[ProxyTab]:
        """从随机主键起点往后取一批候选行（走 ``(proxy_id, status, ...)`` 索引）。

        为什么不用 ``ORDER BY RAND()``：线上实测 69 万可用行时 **2.3 秒/次**，
        每次业务请求都要等 2 秒，不可接受；``LIMIT 1 OFFSET 30万`` 也要 1.7 秒。
        这里用「随机主键起点 + ``ORDER BY proxy_id LIMIT N``」，实测 **14~18ms**，
        N 只取十几条（健康状态在 Redis，需要多取几条才能筛出合格代理），
        仍然是索引范围扫描，不排序、不全表扫。
        """
        sql = (
            select(ProxyTab)
            .where(
                ProxyTab.proxy_id >= start_id,
                ProxyTab.status == 0,
                or_(
                    ProxyTab.success_times.is_(None),
                    ProxyTab.success_times >= ZOMBIE_SUCCESS_TIMES_MAX,
                ),
            )
            .order_by(ProxyTab.proxy_id)
            .limit(limit)
        )
        async with self.async_session() as session:
            res = await session.execute(sql)
        # 过滤 None：单测 / 驱动返回的空行不应进入候选集（否则会在取 key 时炸）
        return [row for row in res.scalars().all() if row is not None]

    @staticmethod
    def _proxy_key_of(proxy_tab: ProxyTab) -> str:
        """取代理串（优先用生成列，避免再解析一次 JSON）。"""
        return proxy_tab.computed_proxy_str or to_proxy_key(proxy_tab.proxy) or ""

    async def _pick_proxy_by_tier(
        self, rows: list[ProxyTab], tier: ProxyTier
    ) -> ProxyTab | None:
        """在一批候选行里按档位挑一条。

        - ``fast``：只接受「未冷却 + EWMA 达标 + 在途并发未打满」的代理，挑不到就返回 None，
          由上层快速失败（fast 车道绝不能为了「凑一条」而下沉到坏代理池）；
        - ``slow``：未冷却即可用，并按 ``cold_probe_ratio`` 掺入 cold / suspect 代理做探测，
          dead 一律不取（只有等冷却过期后由成功请求救回）。
        """
        keys = [self._proxy_key_of(row) for row in rows]
        health_map = await proxy_health_store.get_many(keys)
        in_flight_map = await proxy_health_store.get_in_flight(keys)
        now = int(time.time())
        cfg = CONFIG.proxy_health

        scored: list[tuple[ProxyTab, str, ProxyHealthRecord, int]] = []
        for row, key in zip(rows, keys):
            record = health_map.records.get(key) or seed_health_record(
                row.success_times, row.score
            )
            scored.append((row, key, record, in_flight_map.get(key)))

        if tier is ProxyTier.FAST:
            fast_candidates = [
                item
                for item in scored
                if proxy_health_store.is_fast_eligible(item[2], item[3], now)
            ]
            if fast_candidates:
                return random.choice(fast_candidates)[0]
            sql_log.debug(
                f"fast 档本次候选 {len(rows)} 条均不达标"
                f"（需 ewma>={cfg.fast_min_ewma}、未冷却、在途<{cfg.fast_max_in_flight}）"
            )
            return None

        usable = [
            item
            for item in scored
            if proxy_health_store.is_slow_eligible(item[2], now)
            and item[2].state is not ProxyHealthState.DEAD
        ]
        if not usable:
            return None
        # 探测抽签：按比例放行 cold（完全没记录）/ suspect，用来把新代理「养」成 hot
        if random.random() < cfg.cold_probe_ratio:
            probes = [
                item
                for item in usable
                if item[2].state
                in (
                    ProxyHealthState.COLD,
                    ProxyHealthState.SUSPECT,
                    ProxyHealthState.WARM,
                )
            ]
            if probes:
                return random.choice(probes)[0]
        hot = [item for item in usable if item[2].state is ProxyHealthState.HOT]
        if hot:
            return random.choice(hot)[0]
        warm = [item for item in usable if item[2].state is ProxyHealthState.WARM]
        if warm:
            return random.choice(warm)[0]
        return random.choice(usable)[0]

    async def select_random_proxy(
        self,
        tier: ProxyTier = ProxyTier.FAST,
        exclude: frozenset[str] | set[str] | None = None,
    ) -> ProxyTab | None:
        """按档位从 MySQL 随机取一条可用代理（随机主键起点，禁止 ORDER BY / 全表扫）。

        :param tier: ``fast`` 只取已证明可用的代理；``slow`` 允许掺入 cold 代理做探测
        :param exclude: 本次任务已失败的代理串（换代理用）。注意 ``fast`` 档在候选不足时
            宁可返回 None 也不放宽条件，否则又会回到「反复命中同一个坏代理」的老问题。

        随机点落在最后一条可用行之后会取空，此时最后一次尝试回退到「从头取」，
        保证有结果；取到的行在 id 上略有偏差（空洞后面的行更容易被选中），
        但 id 与代理质量无关。
        """
        min_id, max_id = await self._get_proxy_id_range()
        if max_id <= 0:
            return None
        cfg = CONFIG.proxy_health
        excluded = exclude or frozenset()
        batch = max(1, cfg.candidate_batch)
        attempts = max(1, cfg.pick_attempts)

        # 第一步：从「已验证分档集合」直接取样。
        # 代理池 68 万行里已验证的只有几千条，随机主键起点几乎抽不到（实测 80 次中 1 次），
        # 不先从这里取，就等于「名义上优先好代理、实际全用冷代理」。
        if tier is ProxyTier.FAST or random.random() >= cfg.cold_probe_ratio:
            verified = await self._pick_from_verified_pool(tier, excluded)
            if verified is not None:
                return verified

        fetched_any = False
        for _ in range(attempts):
            # 每次都从随机主键起点取一批：健康状态在 Redis，单条太容易筛空
            start_id = random.randint(min_id, max_id)
            rows = await self._select_candidate_rows(start_id, batch)
            if excluded:
                rows = [
                    row for row in rows if self._proxy_key_of(row) not in excluded
                ]
            if rows:
                fetched_any = True
            proxy_tab = await self._pick_proxy_by_tier(rows, tier)
            if proxy_tab is not None:
                return proxy_tab
        if not fetched_any:
            # 所有随机起点都落在「末段空洞」（id 已被删干净）之后的兜底：从头取一批
            rows = await self._select_candidate_rows(min_id, batch)
            if excluded:
                rows = [
                    row for row in rows if self._proxy_key_of(row) not in excluded
                ]
            proxy_tab = await self._pick_proxy_by_tier(rows, tier)
            if proxy_tab is not None:
                return proxy_tab
        if tier is ProxyTier.FAST:
            # fast 档取不到：可能是 Redis 刚清空（还没播种）。懒触发一次播种，
            # 这样即使调度器没跑（开发环境 IS_DEV=1），fast 车道也不会长期空池。
            self._ensure_health_bootstrapped()
        return None

    async def _pick_from_verified_pool(
        self, tier: ProxyTier, exclude: frozenset[str] | set[str]
    ) -> ProxyTab | None:
        """从「已验证分档」（hot → warm）里取样，取到就返回，取不到返回 None。

        为什么不能只靠随机主键起点：已验证代理只占池子的极小比例（实测几千 / 68 万），
        32 行候选里出现一条的概率约 0.4%，相当于「名义上优先、实际上全用冷代理」。
        分档集合（``SRANDMEMBER``）才是可枚举的好代理名单，取样是 O(count)。

        取样后要回 MySQL 做一次点查（``computed_proxy_str`` 有索引）：
        Redis 只是缓存，MySQL 才是「这行还能不能用」的真相；
        已经被删 / 被置黑的成员顺手从集合里摘掉，避免每次都抽到它。
        """
        cfg = CONFIG.proxy_health
        states = (ProxyHealthState.HOT, ProxyHealthState.WARM)
        keys = await proxy_health_store.sample_state_keys(states, max(1, cfg.verified_sample))
        if exclude:
            keys = [key for key in keys if key not in exclude]
        if not keys:
            return None

        health_map = await proxy_health_store.get_many(keys)
        in_flight_map = await proxy_health_store.get_in_flight(keys)
        now = int(time.time())
        stale: list[str] = []
        candidates: list[tuple[str, ProxyHealthRecord]] = []
        for key in keys:
            record = health_map.records.get(key)
            if record is None:
                # 分档集合里有、明细已过期：属于残留，清掉
                stale.append(key)
                continue
            if tier is ProxyTier.FAST:
                if not proxy_health_store.is_fast_eligible(
                    record, in_flight_map.get(key), now
                ):
                    continue
            elif not proxy_health_store.is_slow_eligible(record, now):
                continue
            candidates.append((key, record))
        if stale:
            await asyncio_gather(
                *[proxy_health_store.forget_state(key) for key in stale], log=sql_log
            )
        if not candidates:
            return None

        # 同档内按 EWMA 加权：成功率越高的越先被抽到，但不会把流量全压在少数几条上
        chosen_pool = [c for c in candidates if c[1].state is ProxyHealthState.HOT]
        if not chosen_pool:
            chosen_pool = candidates
        weights = [max(0.05, record.ewma_success) for _, record in chosen_pool]
        chosen_key = random.choices(
            [key for key, _ in chosen_pool], weights=weights, k=1
        )[0]

        rows = await self.get_proxies_by_keys([chosen_key])
        row = rows.get(chosen_key)
        if row is None:
            # 这一条在 MySQL 里已经不可用（被删 / 被置黑）：摘掉并让上层继续找
            await proxy_health_store.forget_state(chosen_key)
            return None
        return row

    def _ensure_health_bootstrapped(self) -> None:
        """首次需要时触发一次健康状态播种（后台任务，不阻塞请求）。"""
        if self._health_bootstrapped:
            return
        # 先置位再起任务：并发请求同时失败时只播种一次
        self._health_bootstrapped = True
        try:
            asyncio.create_task(self._bootstrap_health_quietly())
        except RuntimeError as e:  # 没有运行中的事件循环（同步脚本入口）
            sql_log.warning(f"没有可用事件循环，跳过健康状态播种：{e}")

    async def _bootstrap_health_quietly(self) -> None:
        try:
            await self.bootstrap_proxy_health()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 后台播种失败不能影响请求路径
            sql_log.exception(f"健康状态播种失败：{type(e).__name__} {e}")

    @log_sql_retry_wrapper()
    async def select_proxy(
        self,
        mode: Literal["single", "all", "rand"] = "single",
        channel="bili",
        tier: ProxyTier = ProxyTier.FAST,
        exclude: frozenset[str] | set[str] | None = None,
    ) -> ProxyTab | List[ProxyTab] | None:
        """
        选择一个可用的代理
        :param channel:
        :param mode: single 就选择分数最高的未被风控的代理 默认是rand，改成single之后从分数最高的代理开始用，这样获取响应特别快
        :param tier: 代理档位（fast=只用已验证的；slow=允许掺 cold 探测）
        :param exclude: 本次任务内已失败的代理串
        :return:[{...}, {...}] proxy_dict
        """
        if mode == "rand":
            # MySQL 随机（随机主键起点，实测 ~15ms）；不再经过 Redis zset。
            # 注意：不能用这里的 `ORDER BY RAND()` 分支，69 万可用行时 2.3s/次。
            return await self.select_random_proxy(tier=tier, exclude=exclude)
        if mode == "single":
            return await self.select_score_top_proxy()
        available_status = 0
        available_score = 0
        _412_status = -412
        _412_sep_time = self._412_sep_time
        _underscore_spe_time = self._underscore_spe_time
        sql = select(ProxyTab).where(
            or_(
                and_(
                    ProxyTab.status == available_status,
                    ProxyTab.score >= available_score,
                ),
                and_(
                    ProxyTab.status == _412_status,
                    ProxyTab.score >= available_score,
                    int(time.time()) - ProxyTab.update_ts >= _412_sep_time,
                ),
                and_(
                    ProxyTab.score < available_score,
                    int(time.time()) - ProxyTab.update_ts >= _underscore_spe_time,
                ),
            )
        )
        if channel == "zhihu":
            sql = select(ProxyTab).where(
                and_(
                    ProxyTab.zhihu_status == available_status,
                    ProxyTab.score >= available_score,
                    ProxyTab.success_times > 0,
                )
            )
        if mode == "single":
            sql = (
                sql.order_by(ProxyTab.score.desc(), ProxyTab.update_ts.desc())
                .limit(1)
                .order_by(func.random())
            )
        elif mode == "all":
            # sql = sql.limit(10000).order_by(func.random())  # 1万条一取差不多，多了没啥用
            pass
        else:
            sql = sql.order_by(func.random()).limit(1)
        async with self.async_session() as session:
            res = await session.execute(sql)
        if mode == "all":
            ret_list_dict = res.scalars().all()
            return list(ret_list_dict)
        else:
            ret_list_dict = res.scalars().first()
            if ret_list_dict:
                return ret_list_dict
            else:
                return None

    @log_sql_retry_wrapper()
    async def is_exist_proxy_by_proxy(self, proxy: dict) -> int:
        """
        查询是否存在这个代理
        :param proxy: 代理IP字典，数据来源于 ProxyTab.proxy 字段。
        :return:int 1：存在 0：不存在
        """
        proxy_str = get_scheme_ip_port_form_proxy_dict(proxy)
        sql = select(func.count(ProxyTab.proxy_id)).where(
            ProxyTab.computed_proxy_str == proxy_str
        )
        async with self.async_session() as session:
            res = await session.execute(sql)
            exist_num = res.scalars().first()
        return exist_num or 0

    @log_sql_retry_wrapper()
    async def remove_list_dict_data_by_proxy(self) -> bool:
        """
        根据proxy列对数据库table去重
        :return:
        """
        subquery = select(func.max(ProxyTab.proxy_id)).group_by(ProxyTab.proxy)
        sql = select(ProxyTab).where(ProxyTab.proxy_id.not_in(subquery))
        async with self.async_session() as session:
            async with session.begin():
                res = await session.execute(sql)
                original = res.scalars().all()
                if not original:
                    sql_log.info("代理数据重复记录不存在")
                    return True
                for record in original:
                    await session.delete(record)
        return True

    @log_sql_retry_wrapper()
    async def update_to_proxy_list(
        self, proxy_tab: ProxyTab, change_score_num=10
    ) -> bool:
        """
        更新数据 update 最好只用update，upsert会导致主键增长异常
        :param change_score_num: 修改的分
        :param proxy_tab:
        :return:
        """
        proxy_key = to_proxy_key(proxy_tab.proxy)
        if not proxy_key:
            sql_log.warning(f"代理串解析为空，跳过分数更新：{proxy_tab.proxy}")
            return False
        if _is_local_ipv6_proxy(proxy_key):
            # 本地 IPv6 代理（PROXY_SERVER）不是代理池成员：不参与分数更新，
            # 否则每次请求都会走到「不存在」分支并刷屏。
            sql_log.debug(f"本地 IPv6 代理不参与分数更新，已跳过：{proxy_key}")
            return False
        succ_times_num = 1 if change_score_num >= 0 else -1
        sql = (
            update(ProxyTab)
            .where(ProxyTab.computed_proxy_str == proxy_key)
            .values(
                status=proxy_tab.status,
                # 分数钳制在 ±10000（与原实现一致），成功/失败次数用 SQL 原子增减
                score=func.least(
                    10000,
                    func.greatest(
                        -10000,
                        func.coalesce(ProxyTab.score, 0) + change_score_num,
                    ),
                ),
                success_times=func.coalesce(ProxyTab.success_times, 0) + succ_times_num,
                update_ts=int(time.time()),
            )
        )
        async with self.async_session() as session:
            async with session.begin():
                res = await session.execute(sql)
        if not res.rowcount:
            # 代理已被清理（或本地代理）：属可预期噪音，只留 DEBUG 明细
            sql_log.debug(f"代理在 MySQL 不存在，跳过分数更新：{proxy_key}")
            return False
        return True

    @log_sql_retry_wrapper()
    async def add_to_proxy_tab_database(self, proxy_tab: ProxyTab) -> bool:
        """
        添加数据（带 MySQL ON DUPLICATE KEY UPDATE）
        :param proxy_tab:
        :return:
        """
        async with self.async_session() as session:
            async with session.begin():
                # 通过 MySQL 方言实现幂等 upsert
                data_dict = sqlalchemy_model_2_dict(proxy_tab)
                # 过滤掉 MySQL 生成列，避免 (3105) 错误
                data_dict.pop("computed_proxy_str", None)
                stmt = mysql_insert(ProxyTab.__table__).values(**data_dict)
                # 非主键字段使用 INSERT 值更新，同时排除生成列
                update_cols = {
                    c.name: stmt.inserted[c.name]
                    for c in ProxyTab.__table__.columns
                    if not c.primary_key and c.name != "computed_proxy_str"
                }
                await session.execute(stmt.on_duplicate_key_update(**update_cols))
                # 刷新生成的主键或其它服务器端默认值
                await session.flush()
                # 释放这个data数据，避免持久化会话耦合

        return True

    @log_sql_retry_wrapper()
    async def remove_proxy(self, proxy_tab: ProxyTab):
        """
        删除
        :param proxy_tab:
        :return:
        """
        async with self.async_session() as session:
            sql = select(ProxyTab).where(
                ProxyTab.proxy_id == proxy_tab.proxy_id
            )  # 删除无效代理，暂时先不用
            async with session.begin():
                res = await session.execute(sql)
                original = res.scalars().all()
                if original:
                    for record in original:
                        # async with self.async_lock:
                        await session.delete(record)

    @log_sql_retry_wrapper()
    async def get_412_proxy_num(self) -> int:
        sql = select(func.count(ProxyTab.proxy_id)).where(ProxyTab.status == -412)
        async with self.async_session() as session:
            result = await session.execute(sql)
        res = result.scalars().first()
        return res

    async def get_latest_add_ts(self) -> int:
        """最近一次入库代理的 ``add_ts``（代理源最新产出时间）。

        只取 ``add_ts`` 一列：原实现 ``select(ProxyTab)`` 会把整行（含 JSON 列
        ``proxy``）取回内存再排序，158 万行实测 1.6~2.0s；改成单列 + ``add_ts``
        索引后走「索引倒序取第一条」，实测 < 2ms。
        """
        try:
            sql = select(ProxyTab.add_ts).order_by(ProxyTab.add_ts.desc()).limit(1)
            async with self.async_session() as session:
                result = await session.execute(sql)
            return result.scalars().first() or 0
        except Exception as e:
            sql_log.exception(e)
            return 0

    @log_sql_retry_wrapper()
    async def get_all_proxy_nums(self) -> int:
        sql = select(func.count(ProxyTab.proxy_id))
        async with self.async_session() as session:
            # async with self.async_lock:
            result = await session.execute(sql)
        res = result.scalars().first()
        if res:
            return res
        else:
            return 0

    @log_sql_retry_wrapper()
    async def get_available_proxy_nums(self):
        sql = select(func.count(ProxyTab.proxy_id)).where(
            and_(ProxyTab.score >= 0, ProxyTab.status != -412)
        )
        async with self.async_session() as session:
            # async with self.async_lock:
            result = await session.execute(sql)
        res = result.scalars().first()
        if res:
            return res
        else:
            return 0

    # region 定时任务
    @log_sql_retry_wrapper()
    async def refresh_proxy(self):
        start_ts = int(time.time())
        avaliable_score = -50
        available_status = 0
        now = int(time.time())
        _412_sep_time = self._412_sep_time
        # async with self.session() as session:
        #     async with session.begin():
        #         del_num = 0
        #         ____sql = delete(ProxyTab).where(and_(
        #             ProxyTab.status != 0,
        #             ProxyTab.success_times < -50
        #         ))
        #         del_num += (await session.execute(____sql)).rowcount  # 刷新超过12小时的无效代理，改变status和score
        #         await session.commit()
        # sql_log.debug(f'【刷新代理池】\t删除无效代理，影响数量：{del_num}个！')
        ___sql = (
            update(ProxyTab)
            .where(
                and_(
                    ProxyTab.status != available_status,
                    now - ProxyTab.update_ts >= _412_sep_time,
                    ProxyTab.score >= avaliable_score,
                    ProxyTab.success_times >= MIN_REFRESH_SUCCESS_TIME,
                ),
            )
            .values(status=available_status, update_ts=now)
        )
        __sql = (
            update(ProxyTab)
            .where(
                and_(
                    ProxyTab.score < avaliable_score,
                    now - ProxyTab.update_ts >= self._underscore_spe_time,
                )
            )
            .values(status=available_status, update_ts=now, score=50)
        )
        async with self.async_session() as session:
            async with session.begin():
                await session.execute(
                    ___sql
                )  # 刷新超过两小时的412风控代理 不改变分数，只改变status
                await session.execute(
                    __sql
                )  # 刷新超过12小时的无效代理，改变status和score
                await session.commit()
        # 上面的批量复位只看「update_ts 是否够旧」，不足以表达健康状态机里的冷却：
        # 风控层冷却 2~6h、网络层冷却 10~30min，必须由 Redis 冷却集合兜底拉回，
        # 否则 412 代理会在 2 小时后被复位成可用（历史 bug）。
        cooling_keys = await proxy_health_store.list_cooling_keys(
            limit=max(1, CONFIG.proxy_health.persist_batch)
        )
        if cooling_keys:
            cooling_keys = await self._filter_still_cooling(cooling_keys)
            await self._mark_proxy_unavailable(cooling_keys, status=-412, now=now)
            sql_log.warning(
                f"【刷新代理池】有 {len(cooling_keys)} 条代理仍在健康冷却中，"
                f"已保持不可用（不会被 refresh 复位）"
            )
        return

    async def _filter_still_cooling(self, proxy_keys: list[str]) -> list[str]:
        """从冷却集合里剔除「已经解冻」的代理。

        必要性：冷却集合是快照，读出来到写库之间代理可能已经成功（成功会立即清冷却），
        此时若仍把它写成 ``status != 0``，它会在 MySQL 里躺到下一次 refresh（最长 2h）
        才会重新可用 —— 等于把好代理误封 2 小时。
        """
        if not proxy_keys:
            return []
        health = await proxy_health_store.get_many(proxy_keys)
        now = int(time.time())
        still_cooling: list[str] = []
        for key in proxy_keys:
            if not health.has(key):
                # Redis 明细已过期但冷却集合里还有：按「仍在冷却」保守处理
                still_cooling.append(key)
                continue
            if health.get(key).is_cooling(now):
                still_cooling.append(key)
        return still_cooling

    async def _mark_proxy_unavailable(
        self, proxy_keys: list[str], status: int, now: int | None = None
    ) -> int:
        """把一批代理置为不可用并刷新 update_ts（不改变 score / success_times 语义）。

        分批更新（每批 200 条）而不是一条大 SQL：``computed_proxy_str`` 是带索引的
        生成列，分批等值 IN 走索引，单批毫秒级，避免长事务。
        """
        if not proxy_keys:
            return 0
        ts = now if now is not None else int(time.time())
        touched = 0
        for start in range(0, len(proxy_keys), 200):
            chunk = proxy_keys[start : start + 200]
            stmt = (
                update(ProxyTab)
                .where(ProxyTab.computed_proxy_str.in_(chunk))
                .values(status=status, update_ts=ts)
            )
            async with self.async_session() as session:
                async with session.begin():
                    res = await session.execute(stmt)
            touched += res.rowcount or 0
        return touched

    @log_sql_retry_wrapper()
    async def flush_proxy_health(self) -> int:
        """把 Redis 里的冷却状态定期「落盘」到 MySQL（心跳）。

        MySQL 侧**不新增字段**，只复用 ``proxy_tab`` 原有的 status / update_ts：
        冷却中的代理保持 ``status != 0``，并持续刷新 ``update_ts``，
        使 ``refresh_proxy`` 的「update_ts 超过 2 小时才复位」条件永远不成立。

        明细（连续失败、EWMA、冷却到期时间）留在 Redis 热缓存 —— 它本来就是
        「可重建的观测数据」，丢失只会让代理退回 cold（需重新探测），不会破坏业务。
        """
        # 进程首次执行时先做一次冷启动播种：Redis 空 → fast 车道没有任何候选，
        # 会把「上线 / 清缓存」变成一次全量停摆。
        if not self._health_bootstrapped:
            await self.bootstrap_proxy_health()
            self._health_bootstrapped = True
        keys = await proxy_health_store.list_cooling_keys(
            limit=max(1, CONFIG.proxy_health.persist_batch)
        )
        # 写库前再确认一次：读快照到写库之间代理可能已经成功解冻，
        # 不能把刚恢复的好代理误标成不可用（否则要等 refresh 才回来）。
        keys = await self._filter_still_cooling(keys)
        if not keys:
            return 0
        touched = await self._mark_proxy_unavailable(keys, status=-412)
        if touched:
            sql_log.debug(f"代理冷却落盘：{len(keys)} 条冷却中，回写 {touched} 行")
        return touched

    @log_sql_retry_wrapper()
    async def bootstrap_proxy_health(self, limit: int = 5000) -> int:
        """冷启动播种：把 MySQL 里「历史上确实成功过」的代理写进 Redis 健康缓存。

        只读一次 ``(status, success_times)`` 索引的范围查询（``success_times >= 3``
        在 158 万行的池子里只有几千条，走索引毫秒级），**不是**选代理的常规路径
        （常规路径必须保持随机主键起点 O(1)）。

        播种口径与 ``seed_health_record`` 完全一致：``ewma`` 只给到刚好达标的
        ``fast_min_ewma``，状态为 ``warm``（不是 hot）—— 它们还需要被真实请求再验证一次。
        """
        cfg = CONFIG.proxy_health
        min_success = max(1, cfg.seed_success_times)
        sql = (
            select(
                ProxyTab.computed_proxy_str,
                ProxyTab.success_times,
                ProxyTab.score,
            )
            .where(
                ProxyTab.status == 0,
                ProxyTab.success_times >= min_success,
                ProxyTab.score >= cfg.seed_min_score,
            )
            .limit(max(1, limit))
        )
        async with self.async_session() as session:
            res = await session.execute(sql)
            rows = res.all()
        if not rows:
            return 0
        keys = [row[0] for row in rows if row[0] and not is_local_ipv6_proxy(row[0])]
        existing = await proxy_health_store.get_many(keys)
        to_seed = []
        for key, success_times, score in rows:
            if not key or is_local_ipv6_proxy(key) or existing.has(key):
                continue
            record = seed_health_record(success_times, score)
            if record.state is ProxyHealthState.COLD:
                continue
            to_seed.append((key, record))
        if not to_seed:
            return 0
        written = await proxy_health_store.bulk_seed(to_seed)
        if written:
            sql_log.warning(
                f"代理健康冷启动播种：从 MySQL 历史表现播种 {written} 条"
                f"（success_times>={min_success} 且 score>={cfg.seed_min_score}）"
            )
        return written

    async def get_proxy_health_counts(self, force: bool = False) -> ProxyHealthCounts:
        """分档数量（hot/warm/suspect/quarantine/dead/cooling），供背压与接口观测。"""
        return await proxy_health_store.get_counts(force=force)

    @log_sql_retry_wrapper()
    async def get_proxy_by_ip(self, ip: str) -> ProxyTab | None:
        """按 'scheme://ip:port' 取代理。

        方案 C 下不再走 Redis 明细 hash，直接按代理串等值查（computed_proxy_str 带索引）。
        """
        return await self.get_proxy_by_key(ip)

    @log_sql_retry_wrapper()
    async def get_black_proxy_num(self) -> int:
        """黑名单（status != 0）代理数量。

        方案 C 下 Redis 不再保存黑名单 hash，改由 MySQL 统计；
        `(status, score, success_times, update_ts)` 索引可覆盖该计数。
        """
        sql = select(func.count(ProxyTab.proxy_id)).where(ProxyTab.status != 0)
        async with self.async_session() as session:
            res = await session.execute(sql)
        return res.scalars().first() or 0

    async def get_proxy_database_redis(self) -> ProxyStatusResp:
        """代理池概览（Redis 池已废弃，改为 MySQL 实时统计）。

        4 个计数都是 158 万行表的全量聚合（实测冷查询合计 300~400ms），
        而该接口会被前端 / 网关周期轮询，因此在进程内缓存
        ``PROXY_STATUS_CACHE_TTL_SECONDS`` 秒：命中约 1~2ms，
        过期后的第一次请求仍需付一次全量聚合并并发的代价。
        """
        now = time.monotonic()
        if (
            self._proxy_status_cache is not None
            and now - self._proxy_status_cache_ts < PROXY_STATUS_CACHE_TTL_SECONDS
        ):
            return self._proxy_status_cache

        total_count, black_count, usable_count, unknown_count, latest_add_ts = (
            await asyncio_gather(
                self.get_all_proxy_nums(),
                self.get_black_proxy_num(),
                self.get_usable_proxy_num(),
                self.get_unknown_proxy_num(),
                self.get_latest_add_ts(),
                log=sql_log,
            )
        )
        # 健康分档来自 Redis 热缓存（自带 10s 进程内缓存，失败时返回全 0，不影响本接口）
        health_counts = await proxy_health_store.get_counts()
        # cold 没有 Redis 记录（=未验证），用「MySQL 可用行数 - 已有记录的代理数」估算，
        # 这样接口能同时看到 hot / warm / suspect / quarantine / dead / cold 六个口径。
        # 用 model_copy 而不是原地改：counts 是带缓存的共享对象，不能污染其它读取方。
        health_counts = health_counts.model_copy(
            update={"cold": max(0, (usable_count or 0) - health_counts.known_total)}
        )
        resp = ProxyStatusResp(
            proxy_total_count=total_count or 0,
            proxy_black_count=black_count or 0,
            # 真实「从未使用」口径（success_times 为 NULL 或 0），
            # 不再用「total - black」冒充（旧口径来自已废弃的 Redis zset 成员数）
            proxy_unknown_count=unknown_count or 0,
            proxy_usable_count=usable_count or 0,
            free_proxy_fetch_ts=latest_add_ts or 0,
            proxy_hot_count=health_counts.hot,
            proxy_warm_count=health_counts.warm,
            proxy_suspect_count=health_counts.suspect,
            proxy_quarantine_count=health_counts.quarantine,
            proxy_dead_count=health_counts.dead,
            proxy_cooling_count=health_counts.cooling,
            proxy_cold_count=health_counts.cold,
        )
        self._proxy_status_cache = resp
        self._proxy_status_cache_ts = time.monotonic()
        return resp

    async def get_usable_proxy_num(self) -> int:
        """可参与随机取用的行数（status=0 且非僵尸）。

        条件必须写成可走索引的形式（``success_times IS NULL OR success_times >= 0``）：
        原写法 ``coalesce(success_times, 0) >= 0`` 是函数调用，MySQL 无法用
        ``(status, success_times)`` 索引做范围定位，只能全表扫描（实测 1.99s）。
        """
        sql = select(func.count(ProxyTab.proxy_id)).where(
            ProxyTab.status == 0,
            or_(
                ProxyTab.success_times.is_(None),
                ProxyTab.success_times >= ZOMBIE_SUCCESS_TIMES_MAX,
            ),
        )
        async with self.async_session() as session:
            res = await session.execute(sql)
        return res.scalars().first() or 0

    async def get_unknown_proxy_num(self) -> int:
        """从未被业务请求使用过的代理数（``success_times`` 为 NULL 或 0）。"""
        sql = select(func.count(ProxyTab.proxy_id)).where(
            or_(
                ProxyTab.success_times.is_(None),
                ProxyTab.success_times == 0,
            )
        )
        async with self.async_session() as session:
            res = await session.execute(sql)
        return res.scalars().first() or 0



SQLHelper = SQLHelperClass()

if __name__ == "__main__":

    async def _test_select_one_proxy():
        print(await SQLHelper.select_proxy("rand"))

    async def _test_get_proxy_by_ip():
        print(await SQLHelper.get_proxy_by_ip("http://116.203.206.103:8080"))

    asyncio.run(_test_select_one_proxy())
