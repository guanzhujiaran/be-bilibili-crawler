"""表空间维护：回收冷数据归档删库后残留的 InnoDB 空洞。

为什么需要单独的步骤（而不是归档时顺手做）：

- InnoDB 的 `DELETE` 只把页标记为可复用，`innodb_file_per_table=ON` 时 `.ibd`
  文件不会缩小，`data_free` 会长期占着磁盘；
- 三张归档表仍在持续写入，空洞会被新行复用，所以**不需要每次删库都重建**；
  只有一次删掉大量历史数据、`data_free` 长期偏高时才值得整表重写；
- 整表重建（`ALTER TABLE ... FORCE`，等价于 `OPTIMIZE TABLE`）的代价是
  「重写整表 + 产生同量级 binlog + 需要接近表大小的临时磁盘」，放在每天 0 点的
  归档任务里做会把低峰窗口占满，因此改为每月跑一次。

每次维护对每张表做三件事：

1. `ANALYZE TABLE`：大 DELETE 后统计信息会失真（优化器可能选错索引，归档表靠
   主键 / 时间字段索引翻页，尤其敏感），同时 InnoDB 的 `DATA_FREE` 也依赖这份统计，
   不先刷新的话 `information_schema` 里的空洞经常恒为 0；
2. 查 `information_schema.TABLES` 的 `data_length` / `data_free`；
3. 空洞占比与绝对值**同时**超过阈值，且 `innodb_file_per_table=ON` 时，
   `ALTER TABLE ... FORCE` 重建表，把空洞还给操作系统。

注意：重建要求磁盘有接近表大小的余量，失败（如磁盘写满）只会记在结果里，
不会影响其他表；MySQL 可能留下临时文件，需要人工清理 datadir 下的 `#innodb_temp`
/ `.ibd` 中间文件。
"""

import re
import time

from sqlalchemy import text

from CONFIG import settings
from log.base_log import myfastapi_logger
from Models.database_archive.tablespace_maintenance_models import (
    TableTablespaceResult,
    TablespaceMaintenanceConf,
    TablespaceMaintenanceResult,
)
from Service.DataArchive.targets import BaseColdArchiveTarget, build_default_targets
from Utils.通用.Tool import format_duration

_BYTES_PER_GB = 1024**3
# DDL 等 MDL 的上限（秒）：宁可失败等下一次，也不要长时间把业务查询挡在表后面
_LOCK_WAIT_TIMEOUT_SECONDS = 30
# 表名来自 SQLAlchemy 模型的 __tablename__，这里只做一次「能安全拼进 DDL」的校验
_SAFE_TABLE_NAME = re.compile(r"^[A-Za-z0-9_]+$")

_STAT_SQL = text(
    """
    SELECT TABLE_SCHEMA AS db_name,
           TABLE_NAME   AS table_name,
           DATA_LENGTH  AS data_bytes,
           DATA_FREE    AS free_bytes
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = :table_name
    """
)
_FILE_PER_TABLE_SQL = text("SHOW VARIABLES LIKE 'innodb_file_per_table'")


class TablespaceMaintenance:
    """按阈值回收归档表空洞的统一入口。"""

    def __init__(self, conf: TablespaceMaintenanceConf | None = None):
        self.conf = conf or TablespaceMaintenanceConf()
        self.log = myfastapi_logger
        self.min_free_pct = (
            self.conf.min_free_pct
            if self.conf.min_free_pct is not None
            else settings.TABLESPACE_MAINTENANCE_MIN_FREE_PCT
        )
        self.min_free_bytes = (
            self.conf.min_free_bytes
            if self.conf.min_free_bytes is not None
            else int(settings.TABLESPACE_MAINTENANCE_MIN_FREE_GB * _BYTES_PER_GB)
        )
        self.targets = build_default_targets()

    # region 对外入口
    async def run_all(self) -> TablespaceMaintenanceResult:
        """维护所有选中的表；单表失败不影响其他表。"""
        selected = [t for t in self.targets if t.table_name in self.conf.tables]
        self.log.info(
            f"表空间维护开始：表={[t.table_name for t in selected]}"
            f" 阈值=空闲占比>={self.min_free_pct}% 且空闲>="
            f"{self.min_free_bytes / _BYTES_PER_GB:.2f}GB"
            f" dry_run={self.conf.is_dry_run}"
        )
        result = TablespaceMaintenanceResult()
        for target in selected:
            try:
                table_result = await self.maintain_table(target)
            except Exception as e:
                self.log.exception(f"[{target.table_name}] 表空间维护失败：{e}")
                table_result = TableTablespaceResult(
                    table=target.table_name,
                    db_alias=target.db_alias,
                    error=f"{e.__class__.__name__}: {e}",
                )
            result.tables.append(table_result)
            result.total_reclaimed_bytes += table_result.reclaimed_bytes
        self.log.info(
            f"表空间维护结束：重建 {result.optimized_count} 张表，"
            f"回收 {result.total_reclaimed_bytes / _BYTES_PER_GB:.2f}GB"
        )
        return result

    # endregion

    # region 单表维护
    async def maintain_table(
        self, target: BaseColdArchiveTarget
    ) -> TableTablespaceResult:
        table_name = target.table_name
        result = TableTablespaceResult(table=table_name, db_alias=target.db_alias)
        # 必须先 ANALYZE 再读 information_schema：InnoDB 的 DATA_FREE 来自缓存的
        # 表空间统计，大 DELETE 之后不会自动刷新，直接读经常恒为 0（永远不触发重建）。
        if not self.conf.is_dry_run:
            await self._analyze(target)

        stat = await self._query_stat(target)
        if stat is None:
            result.skip_reason = "表不存在"
            self.log.warning(f"[{table_name}] 在库中不存在，跳过")
            return result

        db_name, data_bytes, free_bytes = stat
        result.db_name = db_name
        result.data_bytes = data_bytes
        result.free_bytes = free_bytes
        total_bytes = data_bytes + free_bytes
        result.free_pct = (free_bytes / total_bytes * 100) if total_bytes else 0.0
        self.log.info(
            f"[{table_name}] 数据 {data_bytes / _BYTES_PER_GB:.2f}GB"
            f" 空洞 {free_bytes / _BYTES_PER_GB:.2f}GB"
            f"（{result.free_pct:.1f}%）"
        )

        if self.conf.is_dry_run:
            result.skip_reason = "dry_run（未刷新统计，空洞数值可能偏小）"
            return result

        if free_bytes < self.min_free_bytes:
            result.skip_reason = (
                f"空洞 {free_bytes / _BYTES_PER_GB:.2f}GB 未达阈值"
                f" {self.min_free_bytes / _BYTES_PER_GB:.2f}GB"
            )
        elif result.free_pct < self.min_free_pct:
            result.skip_reason = (
                f"空洞占比 {result.free_pct:.1f}% 未达阈值 {self.min_free_pct}%"
            )
        elif not await self._is_file_per_table(target):
            result.skip_reason = "innodb_file_per_table=OFF，重建不会释放磁盘"
        if result.skip_reason:
            self.log.info(f"[{table_name}] 不重建：{result.skip_reason}")
            return result

        result.is_optimized = True
        result.reclaimed_bytes = await self._rebuild(target, data_bytes + free_bytes)
        self.log.info(
            f"[{table_name}] 重建完成，回收 "
            f"{result.reclaimed_bytes / _BYTES_PER_GB:.2f}GB"
        )
        return result

    async def _query_stat(
        self, target: BaseColdArchiveTarget
    ) -> tuple[str, int, int] | None:
        async with target.sql_helper.async_session() as session:
            row = (
                await session.execute(_STAT_SQL, {"table_name": target.table_name})
            ).mappings().first()
        if row is None:
            return None
        return (
            str(row["db_name"]),
            int(row["data_bytes"] or 0),
            int(row["free_bytes"] or 0),
        )

    async def _is_file_per_table(self, target: BaseColdArchiveTarget) -> bool:
        async with target.sql_helper.async_session() as session:
            rows = (await session.execute(_FILE_PER_TABLE_SQL)).all()
        if not rows:
            return False
        return str(rows[0][1]).upper() == "ON"

    async def _analyze(self, target: BaseColdArchiveTarget) -> None:
        """刷新统计信息；失败只记录警告，后续用（可能滞后的）旧统计做判断。"""
        if not _SAFE_TABLE_NAME.match(target.table_name):
            return
        sql = f"ANALYZE TABLE `{target.table_name}`"
        try:
            async with target.sql_helper.async_session() as session:
                await session.execute(text(sql))
                await session.commit()
        except Exception as e:
            self.log.warning(f"[{target.table_name}] {sql} 失败：{e}")

    async def _rebuild(
        self, target: BaseColdArchiveTarget, before_total_bytes: int
    ) -> int:
        """整表重建（`ALTER TABLE ... FORCE`），返回估算的回收字节数。

        用 FORCE 而不是显式 `ALGORITHM=INPLACE, LOCK=NONE`：带 FULLTEXT 索引的表
        不允许 LOCK=NONE，交给服务端自己选算法更稳；8.0+ 的 InnoDB 重建本身就是
        online DDL，重建期间读写不阻塞，只在首尾有短暂的元数据锁。
        """
        if not _SAFE_TABLE_NAME.match(target.table_name):
            raise ValueError(f"表名不安全，拒绝重建：{target.table_name!r}")
        sql = f"ALTER TABLE `{target.table_name}` FORCE"
        started_at = time.perf_counter()
        async with target.sql_helper.async_session() as session:
            await session.execute(
                text(f"SET SESSION lock_wait_timeout = {_LOCK_WAIT_TIMEOUT_SECONDS}")
            )
            await session.execute(text(sql))
            await session.commit()
        elapsed = time.perf_counter() - started_at
        self.log.info(f"[{target.table_name}] {sql} 耗时 {format_duration(elapsed)}")

        stat = await self._query_stat(target)
        if stat is None:
            return 0
        _, data_bytes, free_bytes = stat
        return max(0, before_total_bytes - (data_bytes + free_bytes))

    # endregion


async def run_monthly_maintenance() -> TablespaceMaintenanceResult:
    """定时任务入口：每月一次，按配置阈值回收归档表的 InnoDB 空洞。"""
    return await TablespaceMaintenance().run_all()
