"""定时任务入口：每日 dyn_backup（抽奖信息导出 + 冷数据归档）。

由 `Service/BackgroundService/CrawlerScheduler.py` 的 DYN_DETAIL_DATABASE_CLEANER
每天 0 点触发，两件事共用同一套产物格式与落盘根目录（`settings.DYN_BACKUP_DIR`）：

产物统一为「里层标准 CSV + 外层 gzip 压缩」（`.csv.gz`，见 `csv_gz_io`）：

1. **抽奖信息导出**（`Service/DataArchive/lot_dyn_info_export.py`）：
   `{root}/lot_dyn_info/{年}/{YYYY_MM}_lot_dyn_info.csv.gz`（按月分片），只读不删库；
2. **冷数据归档**（`Service/DataArchive/cold_data_archiver.py`）：
   - dyndetail 库 · `bilidyndetail`（动态详情，dynData 大字段）
   - biliopusdb 库 · `t_lotdyninfo`（第三方抽奖动态，rawJsonStr 大字段）
   - biliopusdb 库 · `t_lotuserspaceresp`（用户空间分页响应，spaceRespJson 大字段）
   产物按月分片：`{root}/{表名}/{年}/{YYYY_MM}_{表名}.csv.gz`，
   校验通过后按保留窗口删库，回收服务器磁盘。

顺序不能反：抽奖信息要读 `bilidyndetail.dynData`，所以先导业务数据再归档删库
（即便当天数据落在保留窗口内不会被删，先导出也能避免「删了才发现没导」）。
"""

from Models.database_archive.cold_data_archive_models import (
    ColdArchiveConf,
    TableArchiveResult,
)
from Models.database_archive.daily_backup_models import DailyBackupResult
from Models.database_archive.lot_dyn_info_models import (
    LotDynInfoExportConf,
    LotDynInfoExportResult,
)
from Service.DataArchive.cold_data_archiver import ColdDataArchiver
from Service.DataArchive.lot_dyn_info_export import LotDynInfoExportJob


async def do_clean(conf: ColdArchiveConf | None = None) -> list[TableArchiveResult]:
    """只跑冷数据归档（保留原入口，便于单独调用 / 手测）。"""
    archiver = ColdDataArchiver(conf)
    return await archiver.archive_all()


async def do_export_lot_dyn_info(
    conf: LotDynInfoExportConf | None = None,
) -> LotDynInfoExportResult:
    """只跑抽奖信息导出。"""
    return await LotDynInfoExportJob().export_all(conf)


async def run_daily_backup(
    archive_conf: ColdArchiveConf | None = None,
    lot_dyn_info_conf: LotDynInfoExportConf | None = None,
) -> DailyBackupResult:
    """定时任务主入口：先导出抽奖信息，再做冷数据归档 + 删库。"""
    lot_dyn_info_result = await do_export_lot_dyn_info(lot_dyn_info_conf)
    archive_results = await do_clean(archive_conf)
    return DailyBackupResult(
        lot_dyn_info=lot_dyn_info_result, archive=archive_results
    )
