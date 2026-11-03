"""每日 dyn_backup 任务（抽奖信息导出 + 冷数据归档）的汇总结果模型。"""

from pydantic import Field

from Models.base.custom_pydantic import CustomBaseModel
from Models.database_archive.cold_data_archive_models import TableArchiveResult
from Models.database_archive.lot_dyn_info_models import LotDynInfoExportResult


class DailyBackupResult(CustomBaseModel):
    """一次每日任务的完整结果。"""

    lot_dyn_info: LotDynInfoExportResult = Field(
        default_factory=LotDynInfoExportResult, description="抽奖信息导出结果"
    )
    archive: list[TableArchiveResult] = Field(
        default_factory=list, description="各表冷数据归档结果"
    )
