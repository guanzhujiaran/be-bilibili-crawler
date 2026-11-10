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
        # 取代理改为「MySQL 随机主键起点」，需要知道 id 范围；
        # 每 10 分钟的任务顺带刷新即可，这里只在首次为空时懒加载。
        self._proxy_id_range: tuple[int, int] | None = None
        self._proxy_id_range_ts: int = 0
        # 代理池概览的进程内缓存（见 PROXY_STATUS_CACHE_TTL_SECONDS）
        self._proxy_status_cache: ProxyStatusResp | None = None
        self._proxy_status_cache_ts: float = 0.0

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

    async def select_random_proxy(self) -> ProxyTab | None:
        """从 MySQL 随机取一条可用代理（不再依赖 Redis 池）。

        为什么不用 ``ORDER BY RAND()``：线上实测 69 万可用行时 **2.3 秒/次**，
        每次业务请求都要等 2 秒，不可接受；``LIMIT 1 OFFSET 30万`` 也要 1.7 秒。
        这里用「随机主键起点 + ``ORDER BY proxy_id LIMIT 1``」，实测 **14~18ms**。

        随机点落在最后一条可用行之后会取空，此时回退到「从头取下一条」，保证有结果；
        取到的行在 id 上略有偏差（空洞后面的行更容易被选中），但 id 与代理质量无关。
        """
        min_id, max_id = await self._get_proxy_id_range()
        if max_id <= 0:
            return None
        for attempt in range(2):
            start_id = min_id if attempt else random.randint(min_id, max_id)
            sql = (
                select(ProxyTab)
                .where(
                    ProxyTab.proxy_id >= start_id,
                    ProxyTab.status == 0,
                    func.coalesce(ProxyTab.success_times, 0)
                    >= ZOMBIE_SUCCESS_TIMES_MAX,
                )
                .order_by(ProxyTab.proxy_id)
                .limit(1)
            )
            async with self.async_session() as session:
                res = await session.execute(sql)
                proxy_tab = res.scalars().first()
            if proxy_tab is not None:
                return proxy_tab
        return None

    @log_sql_retry_wrapper()
    async def select_proxy(
        self, mode: Literal["single", "all", "rand"] = "single", channel="bili"
    ) -> ProxyTab | List[ProxyTab] | None:
        """
        选择一个可用的代理
        :param channel:
        :param mode: single 就选择分数最高的未被风控的代理 默认是rand，改成single之后从分数最高的代理开始用，这样获取响应特别快
        :return:[{...}, {...}] proxy_dict
        """
        if mode == "rand":
            # MySQL 随机（随机主键起点，实测 ~15ms）；不再经过 Redis zset。
            # 注意：不能用这里的 `ORDER BY RAND()` 分支，69 万可用行时 2.3s/次。
            return await self.select_random_proxy()
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
        return

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
        resp = ProxyStatusResp(
            proxy_total_count=total_count or 0,
            proxy_black_count=black_count or 0,
            # 真实「从未使用」口径（success_times 为 NULL 或 0），
            # 不再用「total - black」冒充（旧口径来自已废弃的 Redis zset 成员数）
            proxy_unknown_count=unknown_count or 0,
            proxy_usable_count=usable_count or 0,
            free_proxy_fetch_ts=latest_add_ts or 0,
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
