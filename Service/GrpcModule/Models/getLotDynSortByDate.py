from Models.base.custom_pydantic import CustomBaseModel


class MainConf(CustomBaseModel):
    """手工按日期区间回补抽奖信息的运行参数。

    产物与定时任务完全一致（`{DYN_BACKUP_DIR}/lot_dyn_info/{年}/{日期}_lot_dyn_info.csv.gz`），
    保留窗口 / 是否删库等冷数据归档参数见 `Models.database_archive.ColdArchiveConf`。
    """

    between_ts: list[int] | None = None
