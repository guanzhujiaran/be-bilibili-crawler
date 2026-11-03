"""冷数据归档「表适配器」：把不同库、不同时间字段的表，统一成同一种可归档对象。

通用能力（导出 / 校验 / 删除）由 `ColdDataArchiver` 实现；本模块只负责描述
「这张表按哪个字段切天、主键是什么、默认保留多久」这类表级差异。

时间字段选择原则：**归档与删除必须用同一个字段**，否则会出现「没导出却被删」。
- `bilidyndetail`：无 `created_at`，用 `dynamic_id_int`（动态 id 内嵌发布时间，
  单调递增，天然可按时间范围切分）。
- `t_lotdyninfo`：用 `created_at`（收录时间，有 idx_created_at 索引），
  即「很久以前收进来的第三方抽奖动态」属于冷数据。
- `t_lotuserspaceresp`：无任何时间字段，有轮次的行按 `t_lotmaininfo.created_at`
  归日，早期无轮次的历史行按 `spaceOffset`（动态 id 尺度）归日。
"""

import datetime
from abc import ABC
from collections.abc import AsyncIterator, Sequence
from typing import Any, ClassVar

from sqlalchemy import JSON, Text, cast, delete, func, inspect, select

from CONFIG import settings
from Service.GetOthersLotDyn.Sql.models import (
    TLotdyninfo,
    TLotmaininfo,
    TLotuserspaceresp,
)
from Service.GetOthersLotDyn.Sql.sql_helper import SqlHelper as OtherLotSqlHelper
from Service.GrpcModule.GrpcSrc.SQLObject.DynDetailSqlHelperMysqlVer import (
    grpc_sql_helper,
)
from Service.GrpcModule.GrpcSrc.SQLObject.models import Bilidyndetail
from Utils.通用.Common import log_sql_retry_wrapper
from Utils.通用.dynamic_id_caculate import dynamic_id_2_ts, ts_2_fake_dynamic_id
from dao.base.sqlHelperBase import SqlHelperBase


class BaseColdArchiveTarget(ABC):
    """单张表的归档适配器基类。"""

    table_name: ClassVar[str] = ""
    db_alias: ClassVar[str] = ""
    model: ClassVar[type] = object
    key_column_name: ClassVar[str] = ""
    # 删除时每批的主键数量，避免 in 列表过长触发 MySQL 语句长度 / 参数上限问题
    delete_chunk_size: ClassVar[int] = 500
    # 读取时每批行数。实测 aiomysql 的 session.stream() 并不会真正逐行释放内存
    # （2 万行带 JSON 大字段的行会吃到 1GB 常驻），因此改用「keyset 分页 + limit」，
    # 每批取完即释放，内存与单批行数成正比。
    fetch_batch_size: ClassVar[int] = 2000

    def __init__(self, sql_helper: SqlHelperBase):
        self.sql_helper = sql_helper

    # region 表结构描述
    @property
    def columns(self) -> list[str]:
        return [column.name for column in self.model.__table__.columns]

    @property
    def key_column(self):
        return getattr(self.model, self.key_column_name)

    @property
    def primary_key_name(self) -> str:
        return inspect(self.model).primary_key[0].name

    def get_default_keep_days(self) -> int:
        """该表默认保留多少天热数据，子类按业务查询窗口覆盖。"""
        raise NotImplementedError

    # endregion

    # region 时间与分片键互转
    def ts_to_key(self, ts: int) -> Any:
        """把秒级时间戳转成该表分片字段的取值。"""
        raise NotImplementedError

    def key_to_ts(self, key: Any) -> int:
        """把分片字段取值反解成秒级时间戳。"""
        raise NotImplementedError

    def condition_groups(self, start_ts: int, end_ts: int) -> list[list]:
        """圈出「时间落在 [start_ts, end_ts]」的行，返回若干**互斥**的条件组。

        组内是 AND、组间是 OR。拆成多组是为了让每组都能走索引：`OR` 跨不同列时
        MySQL 无法利用索引，会退化成「全表扫描 + 读大字段」，实测一天就要 10 分钟。

        默认只有一组（直接比较分片字段）；没有时间字段、只能靠关联表推导时间的表可以覆盖。
        归档读取与删除用的是同一份条件，避免「没导出却被删」。
        """
        return [
            [
                self.key_column >= self.ts_to_key(start_ts),
                self.key_column <= self.ts_to_key(end_ts),
            ]
        ]

    # endregion

    async def get_earliest_ts(self) -> int | None:
        """表内最早一条数据的时间戳，用于确定首次归档的起点。"""
        async with self.sql_helper.async_session() as session:
            result = await session.execute(select(func.min(self.key_column)))
            min_key = result.scalar()
        if min_key is None:
            return None
        return self.key_to_ts(min_key)

    async def iter_row_values(
        self, start_ts: int, end_ts: int
    ) -> AsyncIterator[tuple[object, ...]]:
        """按分片字段闭区间分批取数（整行，列顺序同 `self.columns`）。"""
        async for row in self.iter_column_values(start_ts, end_ts, self.columns):
            yield row

    async def iter_column_values(
        self, start_ts: int, end_ts: int, column_names: Sequence[str]
    ) -> AsyncIterator[tuple[object, ...]]:
        """按分片字段闭区间分批取指定列，内存占用与单批行数成正比。

        分批方式实测对比（53 万行、含 6KB JSON 的真实数据）：
        - `session.stream()`：aiomysql 上并不会真正逐行释放，常驻内存涨到 7GB；
        - `(分片字段, 主键) > (?, ?)` 行值比较不是 sargable 条件，MySQL 每次都从
          窗口起点重扫索引再过滤，10 万行要 5 分钟；
        - **按主键 keyset 翻页**（本次采用）：`主键 > 上一批最大主键 ORDER BY 主键
          LIMIT n`，主键有索引、条件 sargable，每批只扫一小段，且主键唯一不会重不漏。

        前提：主键随插入推进（`t_lotdyninfo.dynId`、`bilidyndetail.rid` 都满足），
        所以「按主键顺序扫 + 时间字段过滤」的扫描范围是单调前进的。

        「哪一天的行」由 `condition_groups` 决定，子类可覆盖（没有时间字段的表
        用别的维度圈行）。条件组互斥，所以分组翻页不会读到重复行。

        JSON 列显式 `CAST(... AS CHAR)` 取出原文：归档只需要原始 JSON 文本，
        经 SQLAlchemy 的 JSON 类型转换会白白 `json.loads` 一遍再 `json.dumps` 回去。
        """
        select_names = list(column_names)
        # 主键始终附带取回（用于 keyset 翻页），但不返回给调用方
        if self.primary_key_name not in select_names:
            select_names.append(self.primary_key_name)
        pk_is_extra = len(select_names) > len(column_names)
        # 翻页上界必须取「主键那一列」，不能想当然取最后一列：主键本来就在
        # 列清单里时（`iter_row_values` 传整表列）它不在末尾，取最后一列会拿
        # 到别的字段（如 `dynLotRound_id`）当上界，条件恒真 -> 每批都读回同样
        # 的行，文件无限膨胀（曾产生 14GB 的 .new.tmp）。
        pk_index = select_names.index(self.primary_key_name)
        column_objects = [
            cast(getattr(self.model, name), Text)
            if isinstance(self.model.__table__.columns[name].type, JSON)
            else getattr(self.model, name)
            for name in select_names
        ]
        pk_column = getattr(self.model, self.primary_key_name)

        for conditions in self.condition_groups(start_ts, end_ts):
            last_pk: Any = None
            while True:
                stmt = select(*column_objects).where(*conditions)
                if last_pk is not None:
                    stmt = stmt.where(pk_column > last_pk)
                stmt = stmt.order_by(pk_column).limit(self.fetch_batch_size)
                async with self.sql_helper.async_session() as session:
                    rows = (await session.execute(stmt)).all()
                if not rows:
                    break
                for row in rows:
                    yield tuple(row[:-1]) if pk_is_extra else tuple(row)
                if len(rows) < self.fetch_batch_size:
                    break
                next_pk = rows[-1][pk_index]
                # 防御：分页上界没有推进（主键值异常 / 类型转换后比较失效）时立刻
                # 收尾，避免「同一批行反复读」把归档文件写到撑爆磁盘。
                if last_pk is not None and next_pk <= last_pk:
                    break
                last_pk = next_pk

    @log_sql_retry_wrapper()
    async def delete_by_primary_keys(self, primary_keys: list[str | int]) -> int:
        """按主键分批删除（调用前必须已确认归档文件校验通过）。"""
        if not primary_keys:
            return 0
        pk_column = getattr(self.model, self.primary_key_name)
        deleted = 0
        for index in range(0, len(primary_keys), self.delete_chunk_size):
            chunk = primary_keys[index : index + self.delete_chunk_size]
            async with self.sql_helper.async_session() as session:
                await session.execute(delete(self.model).where(pk_column.in_(chunk)))
                await session.commit()
            deleted += len(chunk)
        return deleted


class DynDetailArchiveTarget(BaseColdArchiveTarget):
    """dyndetail 库 · 动态详情表（`dynData` 大字段是最主要的磁盘占用）。

    注意：`dynamic_id_int` 是 `cast(dynamic_id as signed)`，历史遗留的
    `dynamic_id = 0` 脏数据会落到 1970/2017 年附近的归档文件里。这类行既无法
    关联抽奖、也没有可用的发布时间，和原实现一样一并归档后清掉，属于预期行为。
    """

    table_name: ClassVar[str] = "bilidyndetail"
    db_alias: ClassVar[str] = "dyndetail"
    model: ClassVar[type] = Bilidyndetail
    key_column_name: ClassVar[str] = "dynamic_id_int"

    def get_default_keep_days(self) -> int:
        return settings.COLD_ARCHIVE_KEEP_DAYS_DYNDETAIL

    def ts_to_key(self, ts: int) -> int:
        return ts_2_fake_dynamic_id(ts)

    def key_to_ts(self, key: int) -> int:
        return dynamic_id_2_ts(int(key))


class OtherLotDynArchiveTarget(BaseColdArchiveTarget):
    """biliopusdb 库 · 第三方抽奖动态表（`rawJsonStr` JSON 大字段）。"""

    table_name: ClassVar[str] = "t_lotdyninfo"
    db_alias: ClassVar[str] = "biliopusdb"
    model: ClassVar[type] = TLotdyninfo
    key_column_name: ClassVar[str] = "created_at"

    def get_default_keep_days(self) -> int:
        return settings.COLD_ARCHIVE_KEEP_DAYS_LOTDYNINFO

    def ts_to_key(self, ts: int) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(ts)

    def key_to_ts(self, key: datetime.datetime) -> int:
        return int(key.timestamp())


class LotUserSpaceRespArchiveTarget(BaseColdArchiveTarget):
    """biliopusdb 库 · 第三方抽奖「用户空间响应」表（`spaceRespJson` JSON 大字段）。

    这张表存的是每个用户空间接口**每页**的原始响应，只用于本轮断点续爬，没有时间字段，
    所以「哪天」按两条规则确定：

    - 有轮次（`dynLotRound_id`）的行跟**轮次创建时间**走：同一轮可能翻到很早的历史分页，
      按内容时间归档会删掉当前轮次续爬还要用的分页，导致本轮漏抓；
    - 没有轮次的历史行（`dynLotRound_id IS NULL`，轮次功能上线前采集）按 `spaceOffset`
      反解时间：offset 就是该页最老一条动态的动态 id，与发布时间同一尺度。

    两条规则互斥，每一行只会落进一个日期分片，读取与删除条件一致。
    """

    table_name: ClassVar[str] = "t_lotuserspaceresp"
    db_alias: ClassVar[str] = "biliopusdb"
    model: ClassVar[type] = TLotuserspaceresp
    key_column_name: ClassVar[str] = "dynLotRound_id"

    def get_default_keep_days(self) -> int:
        return settings.COLD_ARCHIVE_KEEP_DAYS_LOTUSERSPACERESP

    def ts_to_key(self, ts: int) -> int:
        return ts_2_fake_dynamic_id(ts)

    def key_to_ts(self, key: int) -> int:
        return dynamic_id_2_ts(int(key))

    def condition_groups(self, start_ts: int, end_ts: int) -> list[list]:
        round_ids = select(TLotmaininfo.lotRound_id).where(
            TLotmaininfo.created_at >= datetime.datetime.fromtimestamp(start_ts),
            TLotmaininfo.created_at <= datetime.datetime.fromtimestamp(end_ts),
        )
        return [
            # 1) 本轮次当天创建的行：走 dynLotRound_id 外键索引
            [TLotuserspaceresp.dynLotRound_id.in_(round_ids)],
            # 2) 轮次功能上线前的历史行：走 spaceOffset 主键区间
            [
                TLotuserspaceresp.dynLotRound_id.is_(None),
                TLotuserspaceresp.spaceOffset >= self.ts_to_key(start_ts),
                TLotuserspaceresp.spaceOffset <= self.ts_to_key(end_ts),
            ],
        ]

    async def get_earliest_ts(self) -> int | None:
        async with self.sql_helper.async_session() as session:
            round_min = (
                await session.execute(
                    select(func.min(TLotmaininfo.created_at)).where(
                        TLotmaininfo.lotRound_id.in_(
                            select(TLotuserspaceresp.dynLotRound_id).where(
                                TLotuserspaceresp.dynLotRound_id.is_not(None)
                            )
                        )
                    )
                )
            ).scalar()
            legacy_min = (
                await session.execute(
                    select(func.min(TLotuserspaceresp.spaceOffset)).where(
                        TLotuserspaceresp.dynLotRound_id.is_(None)
                    )
                )
            ).scalar()
        candidates: list[int] = []
        if round_min is not None:
            candidates.append(int(round_min.timestamp()))
        if legacy_min is not None:
            candidates.append(self.key_to_ts(int(legacy_min)))
        return min(candidates) if candidates else None


def build_default_targets() -> list[BaseColdArchiveTarget]:
    """构建全部可归档表（顺序即执行顺序）。"""
    return [
        DynDetailArchiveTarget(grpc_sql_helper),
        OtherLotDynArchiveTarget(OtherLotSqlHelper),
        LotUserSpaceRespArchiveTarget(OtherLotSqlHelper),
    ]
