# -*- coding: utf-8 -*-
"""
异步sqlalchemy操作方法
"""
from bili_common.models import StrEnumAutoDoc
import ast
import asyncio
import datetime
import json
import time
from typing import List, Literal
from zoneinfo import ZoneInfo

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
from Utils.代理.数据库操作.SqlAlcheyObj.ProxyModel import ProxyTab, AvailableProxy
from Utils.代理.数据库操作.available_proxy_sql_helper import sql_helper
from Utils.代理.数据库操作.comm import get_scheme_ip_port_form_proxy_dict

MIN_REFRESH_SUCCESS_TIME = -3  # 最低允许刷新状态的代理获取请求成功次数
MIN_REFRESH_SCORE = 0  # 最低允许刷新状态的代理分数
DEFAULT_CHUNK_SIZE = 1000  # Adjust as needed
# zset 与 MySQL 短暂不一致时，最多换几个候选代理（每次都把自己剔除）
ZSET_ZOMBIE_MAX_RETRY = 3
# 整点重建 zset 的最低条数门槛：低于它宁可不动，避免把代理池清空
ZSET_MIN_REBUILD_SIZE = 300

database = CONFIG.database


class SubRedisStore(RedisManagerBase):
    """代理相关的 Redis 缓存（方案 C：只保留 zset + 变更缓冲）。

    Redis 只保存「代理串 → 分数」的 zset 作为唯一的代理索引，代理明细一律回
    MySQL 按 ``proxy_tab.computed_proxy_str``（带索引的持久化生成列）等值查。
    原先的 ``bili_proxy_available_hm`` / ``bili_proxy_black_hm`` 各存一份 160 万条
    整行 JSON，约占 600MB 内存，已废弃；上线后需手动 ``DEL`` 掉这两把 key。
    """

    class RedisMap(StrEnumAutoDoc):
        bili_proxy_changed_hm = "bili_proxy_changed_hm"  # 待回写 MySQL 的代理变更
        bili_proxy_sync_ts = f"sync_ts:bili_proxy"
        bili_proxy_zset = (
            "zset_bili_proxy"  # 有序集合，member=代理串，score=分数（唯一代理索引）
        )

    def __init__(self):
        super().__init__(db=database.proxySubRedis.db)
        self.sync_ts = 0
        self.sync_sep_ts = 0.5 * 60 * 60  # 0.5小时同步一次，同步的时候锁死无法获取代理
        self.RedisTimeout = 600

    async def _get_redis_count_by_prefix(self, prefix: RedisMap):
        cursor = 0
        count = 0
        match_str = f"{prefix.value}*"
        while True:
            cursor, keys = await self._scan(cursor=cursor, match_str=match_str)
            count += len(keys)
            if cursor == 0:
                break
        return count

    @staticmethod
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

    # ---------------- 变更缓冲（待回写 MySQL） ----------------

    async def redis_get_all_changed_proxy(self) -> List[ProxyTab]:
        all_changed_proxy_dict = await self._hgetall(
            self.RedisMap.bili_proxy_changed_hm.value
        )
        ret_list = []
        for k, v in all_changed_proxy_dict.items():
            ret_list.append(ProxyTab(**json.loads(v)))
        del all_changed_proxy_dict
        return ret_list

    async def redis_get_changed_proxy(
        self, proxy_info_dict: dict | str
    ) -> ProxyTab | None:
        """读取「本同步周期内已变更过」的代理，可作为增量累加的基础值"""
        proxy_key = self.to_proxy_key(proxy_info_dict)
        if not proxy_key:
            return None
        value = await self._hmget(
            self.RedisMap.bili_proxy_changed_hm.value, proxy_key
        )
        if value:
            return ProxyTab(**json.loads(value))
        return None

    # ---------------- zset（唯一代理索引） ----------------

    async def redis_bili_proxy_zset_count(self) -> int:
        total_count = await self._zcard(key=self.RedisMap.bili_proxy_zset.value)
        return int(total_count) if total_count else 0

    async def redis_select_one_proxy_key(self) -> str | None:
        """从 zset 随机取一个代理串（可能已不在 MySQL，由调用方校验并剔除）"""
        if await self.redis_bili_proxy_zset_count() <= 300:
            return None
        rand_members = await self._zrand_member(
            key=self.RedisMap.bili_proxy_zset.value, count=1
        )
        if rand_members and isinstance(rand_members, list) and len(rand_members) > 0:
            return rand_members[0]
        return None

    async def redis_select_top_proxy_key(self) -> str | None:
        """取分数最高一档的代理串（最高分前 20 名内随机，做负载均衡）"""
        return await self._zget_top_score(
            key=self.RedisMap.bili_proxy_zset.value, rand=True
        )

    async def redis_zadd_proxy_key(self, proxy_key: str, score: int | float) -> int:
        if not proxy_key:
            return 0
        return await self._zadd(
            self.RedisMap.bili_proxy_zset.value, {proxy_key: score}
        )

    async def redis_zrem_proxy_key(self, proxy_key: str) -> int:
        """把代理从 zset 移除（僵尸清理 / 标记不可用）"""
        if not proxy_key:
            return 0
        return await self._zdel_elements(self.RedisMap.bili_proxy_zset.value, proxy_key)

    async def sync_2_redis(
        self,
        proxy_key_score_pairs: list[tuple[str, int | float]],
        key: str | None = None,
    ):
        """把 MySQL 中「可用」的代理（代理串 + 分数）写入 zset。

        方案 C 下只写 zset，不再写代理明细 hash（那正是 600MB 的来源）。
        默认写线上 zset；整点重建时传临时 key，写完再原子 RENAME 顶替线上 zset。
        分批写入，避免一次性构造 160 万项的 dict。
        """
        if not proxy_key_score_pairs:
            return
        zset_key = key or self.RedisMap.bili_proxy_zset.value
        for i in range(0, len(proxy_key_score_pairs), DEFAULT_CHUNK_SIZE):
            chunk = proxy_key_score_pairs[i : i + DEFAULT_CHUNK_SIZE]
            await self._zadd(
                zset_key,
                {
                    proxy_key: (0 if score is None else score)
                    for proxy_key, score in chunk
                    if proxy_key
                },
            )

    async def redis_replace_zset(self, tmp_key: str) -> bool:
        """用临时 zset 原子顶替线上 zset。

        临时 key 不存在时直接返回，避免把线上代理池清空。
        """
        if not await self.exists(tmp_key):
            sql_log.warning(f"临时 zset {tmp_key} 不存在，跳过替换")
            return False
        return bool(await self._rename(tmp_key, self.RedisMap.bili_proxy_zset.value))

    async def redis_update_proxy(
        self, proxy_tab: ProxyTab, score_change_num: int, base: ProxyTab
    ) -> bool:
        """在变更缓冲里累加分数/状态，并维护 zset。

        :param proxy_tab: 事件值（其 status 为期望的新状态）
        :param base: 基础值，由调用方提供（优先取变更缓冲，否则回 MySQL 读当前行）。
                     方案 C 下已无 available_hm 可读，基础值必须由调用方给出。

        修复点：原实现只在 available_hm 里找基础值，找不到就返回 False，
        调用方随即跳过 MySQL 更新，导致分数/状态变更被静默丢弃。
        """
        succ_times_num = 1 if score_change_num >= 0 else -1
        base.status = proxy_tab.status
        base.score = (base.score or 0) + score_change_num
        if base.score > 10000:
            base.score = 10000
        elif base.score < -10000:
            base.score = -10000
        base.success_times = (base.success_times or 0) + succ_times_num
        proxy_key = self.to_proxy_key(base.proxy)
        if not proxy_key:
            sql_log.warning(f"代理串解析为空，跳过变更缓冲写入：{base.proxy}")
            return False
        await self._hmset(
            name=self.RedisMap.bili_proxy_changed_hm.value,
            field_values={proxy_key: json.dumps(sqlalchemy_model_2_dict(base))},
        )
        if base.status != 0:
            # 不可用：从 zset 摘掉，避免再次被选中
            await self.redis_zrem_proxy_key(proxy_key)
        else:
            await self.redis_zadd_proxy_key(proxy_key, base.score)
        return True

    @log_sql_retry_wrapper()
    async def set_sync_ts(self):
        self.sync_ts = int(time.time())
        await self._set(self.RedisMap.bili_proxy_sync_ts.value, self.sync_ts)

    @log_sql_retry_wrapper()
    async def get_sync_ts(self) -> int:
        if self.sync_ts:
            return self.sync_ts
        _ = await self._get(self.RedisMap.bili_proxy_sync_ts.value)
        return int(_) if _ else 0

    @log_sql_retry_wrapper()
    async def redis_clear_all_proxy(self):
        """只清变更缓冲；zset 作为热缓存保留，由整点刷新重建。"""
        await self.redis_clear_changed_proxy()

    async def redis_clear_changed_proxy(self):
        return await self._delete(self.RedisMap.bili_proxy_changed_hm.value)


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
        GLOBAL_SCHEDULER.add_job(
            self.sync_proxy_database_redis,
            "interval",
            seconds=1 * 60 * 60,
            next_run_time=datetime.datetime.now(),
            misfire_grace_time=600,
        )
        self.sub_redis_store = SubRedisStore()
        self.is_checking_redis_data = False

    @log_sql_retry_wrapper()
    async def sync_2_database(self, chunk_size: int = DEFAULT_CHUNK_SIZE):
        """
        Fetches changed proxy info from Redis and bulk updates them in the database.
        Processes data in chunks to manage memory for very large datasets.
        """
        try:
            all_proxy_infos = await self.sub_redis_store.redis_get_all_changed_proxy()
            if not all_proxy_infos:
                sql_log.info("No proxy data changes found in Redis to sync.")
                return

            # Prepare data efficiently (using a generator expression initially if memory is a concern)
            # However, bulk methods need a list, so we'll chunk the final list.
            update_mappings = [
                {
                    "proxy_id": proxy_info.proxy_id,
                    "status": proxy_info.status,
                    "update_ts": proxy_info.update_ts,
                    "score": proxy_info.score,
                    "success_times": proxy_info.success_times,
                    # Add other fields relevant to ProxyTab here
                }
                for proxy_info in all_proxy_infos
                if proxy_info.proxy_id and proxy_info.proxy  # Your filter condition
            ]
            # Clear the large list from memory if it helps (Python's GC usually handles this)
            del all_proxy_infos
            total_proxies_to_update = len(update_mappings)
            if not total_proxies_to_update:
                sql_log.info("Filtered proxy list is empty. No updates to perform.")
                return

            async def _update_single(cur_chunk: int):
                chunk = update_mappings[_ : _ + chunk_size]
                if not chunk:
                    return
                while 1:
                    try:
                        async with self.async_session() as session:
                            # sql_log.critical(
                            #     f"Processing chunk {cur_chunk // chunk_size + 1}/{total_proxies_to_update // chunk_size}: {len(chunk)}")
                            # Use run_sync for the synchronous bulk operation
                            await session.run_sync(
                                lambda s: s.bulk_update_mappings(ProxyTab, chunk)
                            )
                            await session.commit()
                            # sql_log.critical(
                            #     f"Processing chunk {cur_chunk // chunk_size + 1}/{total_proxies_to_update // chunk_size}: {len(chunk)} success!")
                        break
                    except Exception as e:
                        sql_log.error(f"An unexpected error occurred: {e}")
                        await asyncio.sleep(10)

            try:
                tasks = set()
                for _ in range(0, total_proxies_to_update, chunk_size):
                    tasks.add(asyncio.create_task(_update_single(_)))
                await asyncio_gather(*tasks, log=sql_log)
            except Exception as err:
                # Catch other potential errors during processing
                sql_log.error(f"An unexpected error occurred: {err}")
        except Exception as err:
            # Catch errors during Redis fetch or initial data processing
            sql_log.error(f"Error during sync_2_database setup: {err}")
            # Handle error appropriately

    async def check_redis_data(self, force=False):
        if not self.sub_redis_store.sync_ts:
            async with self._lock:
                if not self.sub_redis_store.sync_ts:
                    self.sub_redis_store.sync_ts = (
                        await self.sub_redis_store.get_sync_ts()
                    )
        if not self.is_checking_redis_data:
            async with self._lock:
                if not self.is_checking_redis_data:
                    if (
                        int(time.time()) - self.sub_redis_store.sync_sep_ts
                        > self.sub_redis_store.sync_ts
                        or force
                        or self.sub_redis_store.sync_ts == 0
                    ):
                        self.is_checking_redis_data = True
                        try:
                            redis_sync_ts = await self.sub_redis_store.get_sync_ts()
                            if (
                                redis_sync_ts
                                < int(time.time()) - self.sub_redis_store.sync_sep_ts
                                or force
                            ):
                                await self.sync_2_database()
                                # 只清变更缓冲；zset 作为热缓存保留，由下面重建
                                await self.sub_redis_store.redis_clear_all_proxy()
                                await self.clear_unusable_proxy()
                                # 用 MySQL 的「可用代理」重建 zset（临时 key + 原子
                                # RENAME），使 zset 不再像旧版 available_hm 那样只增不减
                                await self.refresh_proxy_zset()
                                await self.sub_redis_store.set_sync_ts()
                        except Exception as e:
                            sql_log.exception(f"同步redis和mysql数据库失败！{e}")
                            raise e
                        finally:
                            self.is_checking_redis_data = False
                    else:
                        sql_log.debug(
                            f'上次同步时间：{datetime.datetime.fromtimestamp(self.sub_redis_store.sync_ts, tz=ZoneInfo("Asia/Shanghai"))}\n距离上次同步时间小于{self.sub_redis_store.sync_sep_ts}秒，无需同步'
                        )

    @log_sql_retry_wrapper()
    async def clear_unusable_proxy(self):
        """
        清除数据库中的不可用的代理，根据score和success_times判断
        主要用于同步到redis之前，将不可用的清理掉
        :return:
        """
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

            if not proxy_ids_to_delete:
                return 0
            # 先删除子表数据
            delete_available_proxy = delete(AvailableProxy).where(
                AvailableProxy.proxy_tab_id.in_(proxy_ids_to_delete)
            )
            await session.execute(delete_available_proxy)

            # 再删除父表数据
            delete_proxy_tab = delete(ProxyTab).where(
                ProxyTab.proxy_id.in_(proxy_ids_to_delete)
            )
            res = await session.execute(delete_proxy_tab)

            await session.commit()
            return res.rowcount

    # ---------------- 方案 C：代理明细统一回 MySQL ----------------

    async def get_proxy_by_key(self, proxy_key: str) -> ProxyTab | None:
        """按代理串取整行。

        ``proxy_tab.computed_proxy_str`` 是持久化生成列且带索引，等值查走索引，
        单行毫秒级；这是方案 C 下读取代理明细的唯一入口。
        """
        if not proxy_key:
            return None
        sql = (
            select(ProxyTab).where(ProxyTab.computed_proxy_str == proxy_key).limit(1)
        )
        async with self.async_session() as session:
            res = await session.execute(sql)
        return res.scalars().first()

    async def _select_proxy_via_zset(self, *, top: bool) -> ProxyTab | None:
        """zset 取代理串 → MySQL 取整行。

        zset 与 MySQL 可能短暂不一致（MySQL 已删除的代理仍留在 zset），
        取到这类「僵尸」时直接从 zset 摘掉并换下一个，避免反复被选中。
        """
        for _ in range(ZSET_ZOMBIE_MAX_RETRY):
            proxy_key = (
                await self.sub_redis_store.redis_select_top_proxy_key()
                if top
                else await self.sub_redis_store.redis_select_one_proxy_key()
            )
            if not proxy_key:
                return None
            proxy_tab = await self.get_proxy_by_key(proxy_key)
            if proxy_tab:
                return proxy_tab
            sql_log.warning(
                f"zset 中的代理在 MySQL 已不存在，已从 zset 移除：{proxy_key}"
            )
            await self.sub_redis_store.redis_zrem_proxy_key(proxy_key)
        return None

    async def select_available_proxy_key_pairs(self) -> list[tuple[str, int]]:
        """取「可用」代理的 (代理串, 分数) 列表，供整点重建 zset。

        筛选条件与 ``select_proxy(mode="all")`` 一致，但只取两列，
        不再把 165 万行整行 ORM 对象拉进内存。
        """
        available_status = 0
        available_score = 0
        _412_status = -412
        sql = select(ProxyTab.computed_proxy_str, ProxyTab.score).where(
            or_(
                and_(
                    ProxyTab.status == available_status,
                    ProxyTab.score >= available_score,
                ),
                and_(
                    ProxyTab.status == _412_status,
                    ProxyTab.score >= available_score,
                    int(time.time()) - ProxyTab.update_ts >= self._412_sep_time,
                ),
                and_(
                    ProxyTab.score < available_score,
                    int(time.time()) - ProxyTab.update_ts >= self._underscore_spe_time,
                ),
            )
        )
        async with self.async_session() as session:
            res = await session.execute(sql)
            rows = res.all()
        return [(row[0], row[1] or 0) for row in rows if row[0]]

    async def refresh_proxy_zset(self):
        """整点用 MySQL 的「可用代理」重建 zset（临时 key + 原子 RENAME）。

        这样 zset 每轮与 MySQL 精确对齐，不会像旧版 available_hm 那样只增不减、
        把 MySQL 已删除的代理永远留在里面。
        重建期间代理事件对 zset 的增量会被随后覆盖，但变更本身已写入 changed_hm，
        下一轮即恢复一致；该窗口内最多多选到一次已失效的代理，取到时会被校验剔除。
        """
        pairs = await self.select_available_proxy_key_pairs()
        if len(pairs) < ZSET_MIN_REBUILD_SIZE:
            sql_log.warning(
                f"本轮可用代理仅 {len(pairs)} 条，低于 {ZSET_MIN_REBUILD_SIZE} 条，"
                f"跳过 zset 重建，避免把代理池清空"
            )
            return
        tmp_key = f"{self.sub_redis_store.RedisMap.bili_proxy_zset.value}:rebuild"
        await self.sub_redis_store._delete(tmp_key)
        await self.sub_redis_store.sync_2_redis(pairs, key=tmp_key)
        if await self.sub_redis_store.redis_replace_zset(tmp_key):
            sql_log.info(f"代理 zset 已重建，共 {len(pairs)} 条")

    @log_sql_retry_wrapper()
    async def select_score_top_proxy(self) -> ProxyTab:
        if redis_data := await self._select_proxy_via_zset(top=True):
            return redis_data
        sql = select(ProxyTab).order_by(ProxyTab.score.desc()).limit(1)
        async with self.async_session() as session:
            res = await session.execute(sql)
        ret_list_dict = res.scalars().first()
        return ret_list_dict

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
        if mode != "all":
            if mode == "single":
                return await self._select_proxy_via_zset(top=True)
            if mode == "rand":
                return await self._select_proxy_via_zset(top=False)
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
        try:
            # 基础值：优先用本同步周期内已变更过的缓冲值（避免同小时内反复回库），
            # 否则回 MySQL 读当前行。方案 C 下已无 available_hm 可读，基础值必须显式取。
            proxy_key = self.sub_redis_store.to_proxy_key(proxy_tab.proxy)
            if not proxy_key:
                sql_log.warning(f"代理串解析为空，跳过分数更新：{proxy_tab.proxy}")
                return False
            base = await self.sub_redis_store.redis_get_changed_proxy(proxy_key)
            if base is None:
                base = await self.get_proxy_by_key(proxy_key)
            if base is None:
                # MySQL 里也没有这条代理（已被清理）：无法更新，但要留下痕迹，
                # 原实现在这里直接 return False，静默丢弃了变更。
                sql_log.warning(f"代理在 MySQL 与 Redis 均不存在，跳过分数更新：{proxy_key}")
                return False
            return bool(
                await self.sub_redis_store.redis_update_proxy(
                    proxy_tab, change_score_num, base
                )
            )
        except Exception as e:
            sql_log.exception(e)
        # Redis 异常时回退为直接更新 MySQL
        succ_times_num = 1 if change_score_num >= 0 else -1
        sql = (
            update(ProxyTab)
            .where(ProxyTab.proxy_id == proxy_tab.proxy_id)
            .values(
                status=proxy_tab.status,
                score=ProxyTab.score + change_score_num,
                success_times=(proxy_tab.success_times or 0) + succ_times_num,
                update_ts=proxy_tab.update_ts,
                add_ts=proxy_tab.add_ts,
            )
        )
        async with self.async_session() as session:
            async with session.begin():
                # async with self.async_lock:
                await session.execute(sql)
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
        try:
            sql = select(ProxyTab).order_by(ProxyTab.add_ts.desc()).limit(1)
            async with self.async_session() as session:
                result = await session.execute(sql)
            res = result.scalars().first()
            if res:
                return res.add_ts
            else:
                return 0
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

    async def sync_proxy_database_redis(self):
        await self.check_redis_data()

    # endregion

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
        # 使用asyncio_gather并行获取MySQL和Redis中的代理状态
        (
            mysql_sync_redis_ts,
            proxy_black_count,
            proxy_unknown_count,
            free_proxy_fetch_ts,
            proxy_usable_count,
        ) = await asyncio_gather(
            self.sub_redis_store.get_sync_ts(),
            self.get_black_proxy_num(),
            self.sub_redis_store.redis_bili_proxy_zset_count(),
            SQLHelper.get_latest_add_ts(),
            sql_helper.get_num(True),  # 获取可用代理的数量
            log=sql_log,
        )
        # 将获取到的代理状态转换为ProxyStatusResp对象
        ret_model = ProxyStatusResp(
            mysql_sync_redis_ts=mysql_sync_redis_ts,
            proxy_total_count=proxy_black_count + proxy_unknown_count,
            proxy_black_count=proxy_black_count,
            proxy_unknown_count=proxy_unknown_count,
            proxy_usable_count=proxy_usable_count,
            free_proxy_fetch_ts=free_proxy_fetch_ts,
            sync_ts=int(time.time()),
        )
        return ret_model


SQLHelper = SQLHelperClass()

if __name__ == "__main__":

    async def _test_select_one_proxy():
        print(await SQLHelper.select_proxy("rand"))

    async def _test_get_proxy_by_ip():
        print(await SQLHelper.get_proxy_by_ip("http://116.203.206.103:8080"))

    async def _test_refresh_zset():
        await SQLHelper.refresh_proxy_zset()

    asyncio.run(_test_get_proxy_by_ip())
