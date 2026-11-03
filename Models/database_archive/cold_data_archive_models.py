"""冷数据归档（导出 csv.gz + 可选删库）相关的数据模型。

设计要点（消费方：`Service/DataArchive/cold_data_archiver.py`）：

- **统一落盘根目录**：复用 `settings.DYN_BACKUP_DIR`（容器里挂载到宿主机
  `docker_vol/fastapi/dyn_backup`），空值回落到代码根目录下的
  `scripts/database/dyn_backup`。所有表的归档产物都放在这一个目录下，
  方便整体打包上传网盘。
- **按「表 + 日期」区分文件**：`{root}/{表名}/{年}/{YYYY_MM_DD}_{表名}.csv.gz`。
  文件名自带表名与日期，脱离目录层级也不会混淆；年目录只用来避免单目录文件过多。
- **保留天数按表配置**：三张表虽然是同一批动态的三份存储，但业务查询窗口不同
  （`bilidyndetail` 只服务最近抽奖信息导出，`t_lotdyninfo` 服务第三方抽奖列表的
  历史范围查询，`t_lotuserspaceresp` 只服务本轮断点续爬），因此保留天数不写死，
  由配置项分别给出。
"""

import os
import time
from pydantic import Field

from CONFIG import CONFIG, settings
from Models.base.custom_pydantic import CustomBaseModel

_DEFAULT_ARCHIVE_RELATIVE_DIR = "scripts/database/dyn_backup"


def get_archive_root_dir() -> str:
    """归档根目录：优先配置项，未配置时回落到代码根目录下的默认目录。"""
    return settings.DYN_BACKUP_DIR or os.path.join(
        CONFIG.root_dir, _DEFAULT_ARCHIVE_RELATIVE_DIR
    )


class ColdArchiveTables:
    """可归档的冷数据表（值即真实表名，同时作为归档文件名的一部分）。

    三张表存的是同一批动态原始 JSON 的三份副本（dynData / rawJsonStr / spaceRespJson），
    因此共用同一套归档产物格式、落盘根目录与保留窗口配置。
    """

    BILI_DYN_DETAIL = "bilidyndetail"
    OTHER_LOT_DYN_INFO = "t_lotdyninfo"
    LOT_USER_SPACE_RESP = "t_lotuserspaceresp"

    ALL: tuple[str, str, str] = (
        BILI_DYN_DETAIL,
        OTHER_LOT_DYN_INFO,
        LOT_USER_SPACE_RESP,
    )


class ArchiveFileResult(CustomBaseModel):
    """单个日期分片的归档结果。"""

    day: str = Field(description="归档日期，格式 YYYY_MM_DD")
    file_path: str = Field(default="", description="归档文件绝对路径，空表示未生成")
    row_count: int = Field(default=0, description="本次从库里导出（并删除）的行数")
    file_row_count: int = Field(default=0, description="归档包内总行数（含历史续写）")
    file_size: int = Field(default=0, description="压缩包字节数")
    is_verified: bool = Field(default=False, description="回读校验是否通过")
    is_deleted: bool = Field(default=False, description="该分片的数据是否已从库中删除")


class TableArchiveResult(CustomBaseModel):
    """单张表的归档汇总结果。"""

    table: str
    db_alias: str
    keep_days: int = Field(description="保留最近多少天不归档")
    cutoff_ts: int = Field(description="冷热分界时间戳（秒），早于它的数据才归档")
    files: list[ArchiveFileResult] = Field(default_factory=list)
    total_rows: int = 0
    total_file_size: int = 0
    deleted_rows: int = 0
    skipped_days: int = Field(default=0, description="无数据 / 已有归档而跳过的天数")
    is_skipped: bool = False
    skip_reason: str = ""


class ColdArchiveConf(CustomBaseModel):
    """一次冷数据归档任务的运行参数。"""

    root_dir: str = Field(default_factory=get_archive_root_dir)
    end_ts: int = Field(
        default_factory=lambda: int(time.time()),
        description="归档截止时间（秒），默认当前时间，便于回放历史任务",
    )
    tables: list[str] = Field(
        default_factory=lambda: list(ColdArchiveTables.ALL),
        description="要归档的表名，取值见 ColdArchiveTables",
    )
    keep_days_override: int | None = Field(
        default=None, description="覆盖所有表的保留天数，None 表示各表用各自配置"
    )
    is_delete_after_archive: bool | None = Field(
        default=None, description="归档校验通过后是否删库，None 表示取配置默认值"
    )
    is_dry_run: bool = Field(
        default=False, description="只统计不落盘、不删除，用于上线前核对数据量"
    )
    max_days_per_run: int = Field(
        default=0,
        description="单次运行最多处理多少天，0 表示不限制；首次处理历史数据时可用来分批推进",
    )
