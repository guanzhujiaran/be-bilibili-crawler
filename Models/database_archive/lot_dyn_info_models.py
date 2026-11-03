"""抽奖信息导出（按月产出 csv.gz）相关的数据模型。

产物与冷数据归档**完全同构**（里层标准 CSV、外层 gzip、按月分片），
落在同一个根目录下：

    {DYN_BACKUP_DIR}/{任务名}/{年}/{YYYY_MM}_{任务名}.csv.gz

区别只在内容来源：这里不是「某张表的原始行」，而是 `bilidyndetail.dynData`
解析出来的抽奖信息（发布者、动态链接、抽奖类型…），属于业务导出，不删库。
"""

import time

from pydantic import Field

from CONFIG import settings
from Models.base.custom_pydantic import CustomBaseModel
from Models.database_archive.cold_data_archive_models import get_archive_root_dir

LOT_DYN_INFO_JOB_NAME = "lot_dyn_info"


class LotDynInfoExportConf(CustomBaseModel):
    """一次抽奖信息导出的运行参数。"""

    root_dir: str = Field(default_factory=get_archive_root_dir)
    months: int = Field(
        default_factory=lambda: settings.LOT_DYN_INFO_MONTHS,
        description=(
            "从截止时间往前导出多少个月（只含已过完的整月），默认取 "
            "settings.LOT_DYN_INFO_MONTHS，兼顾「漏跑自愈」与解析开销"
        ),
    )
    end_ts: int = Field(
        default_factory=lambda: int(time.time()),
        description="截止时间（秒），默认当前时间；导出的是它之前已结束的整月",
    )
    is_dry_run: bool = Field(
        default=False, description="只统计不落盘，用于确认每天能导出多少行"
    )


class LotDynInfoFileResult(CustomBaseModel):
    """单个月份分片的导出结果。"""

    period: str = Field(description="月份，格式 YYYY_MM")
    file_path: str = Field(default="", description="导出文件绝对路径，空表示未生成")
    row_count: int = Field(default=0, description="写入文件的数据行数")
    file_size: int = Field(default=0, description="压缩包字节数")
    is_verified: bool = Field(default=False, description="回读校验是否通过")
    is_skipped: bool = Field(default=False, description="当月没有动态，跳过")
    skip_reason: str = ""


class LotDynInfoExportResult(CustomBaseModel):
    """一次导出的汇总结果。"""

    files: list[LotDynInfoFileResult] = Field(default_factory=list)
    total_rows: int = 0
    total_file_size: int = 0
    skipped_periods: int = 0
