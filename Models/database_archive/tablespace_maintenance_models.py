"""表空间维护（归档删库后的 InnoDB 空洞回收）相关的数据模型。

背景：冷数据归档（`Service/DataArchive/cold_data_archiver.py`）按主键分批
`DELETE`，InnoDB 只把页标记为可复用，**不会把空间还给操作系统**；持续写入时
这些空洞会被新插入的行复用，但一次删掉大量历史数据后 `data_free` 会长期占着磁盘。

因此单独抽出「表空间维护」动作（消费方：`Service/DataArchive/tablespace_maintenance.py`）：

- 每月跑一次（归档是每天跑，重建整表太贵，没必要跟着每天跑）；
- 只有空洞占比 / 空洞绝对值同时超过阈值才重建，避免无意义地整表重写；
- 重建前先 `ANALYZE TABLE`（大 DELETE 后统计信息会失真，优化器可能选错索引），
  重建本身也会重算统计信息，所以对跳过的表也有价值。
"""

from pydantic import Field

from Models.base.custom_pydantic import CustomBaseModel
from Models.database_archive.cold_data_archive_models import ColdArchiveTables


class TablespaceMaintenanceConf(CustomBaseModel):
    """一次表空间维护任务的运行参数。"""

    tables: list[str] = Field(
        default_factory=lambda: list(ColdArchiveTables.ALL),
        description="要检查的表名，取值见 ColdArchiveTables",
    )
    min_free_pct: float | None = Field(
        default=None,
        description="空洞占比阈值（%），None 表示取配置 TABLESPACE_MAINTENANCE_MIN_FREE_PCT",
    )
    min_free_bytes: int | None = Field(
        default=None,
        description="空洞绝对值阈值（字节），None 表示取配置 TABLESPACE_MAINTENANCE_MIN_FREE_GB",
    )
    is_dry_run: bool = Field(
        default=False, description="只统计不重建，用于上线前确认空洞大小"
    )


class TableTablespaceResult(CustomBaseModel):
    """单张表的表空间检查 / 维护结果。"""

    table: str
    db_alias: str
    db_name: str = Field(default="", description="MySQL 库名（连接上的 DATABASE()）")
    data_bytes: int = Field(default=0, description="重建前 data_length（字节）")
    free_bytes: int = Field(default=0, description="重建前 data_free（字节）")
    free_pct: float = Field(default=0, description="重建前空洞占比（%）")
    is_optimized: bool = Field(default=False, description="本次是否执行了重建")
    reclaimed_bytes: int = Field(default=0, description="重建后回收的字节数（估算值）")
    skip_reason: str = Field(default="", description="未重建的原因")
    error: str = Field(default="", description="执行失败时的异常信息")


class TablespaceMaintenanceResult(CustomBaseModel):
    """一次表空间维护任务的汇总结果。"""

    tables: list[TableTablespaceResult] = Field(default_factory=list)
    total_reclaimed_bytes: int = Field(default=0, description="所有表回收字节数之和")

    @property
    def optimized_count(self) -> int:
        return sum(1 for table in self.tables if table.is_optimized)
