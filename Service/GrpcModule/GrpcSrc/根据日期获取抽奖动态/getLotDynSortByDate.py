# -*- coding: utf-8 -*-
"""按月份导出抽奖信息（历史入口，现已收敛到统一的 dyn_backup 任务）。

原来这里自己用 pandas 往 `result/{年}/{月}/*_抽奖信息.csv` 落盘，
产物格式和其它备份（冷数据归档）不一致、也不好整体打包上云；
现在统一由 `Service/DataArchive/lot_dyn_info_export.py` 产出：

    {DYN_BACKUP_DIR}/lot_dyn_info/{年}/{YYYY_MM}_lot_dyn_info.csv.gz

本模块只保留「手工按时间区间回补」的入口，解析逻辑在
`Service/DataArchive/lot_dyn_parser.py`。
"""

import datetime

from log.base_log import myfastapi_logger
from Models.database_archive.lot_dyn_info_models import LotDynInfoExportConf
from Service.DataArchive.csv_gz_io import month_bounds
from Service.DataArchive.lot_dyn_info_export import LotDynInfoExportJob
from Service.GrpcModule.Models.getLotDynSortByDate import MainConf


class LotDynSortByDate:
    """按日期区间导出抽奖信息（薄封装，产出与定时任务完全一致）。"""

    def get_split_ts(self, between_ts: list[int]) -> list[tuple[int, int]]:
        """把时间区间覆盖到的月份切成整月分片（产物一律按月落盘）。"""
        months: list[tuple[int, int]] = []
        cursor = between_ts[0]
        while cursor <= between_ts[1]:
            month_start, month_end = month_bounds(cursor)
            months.append((month_start, month_end))
            cursor = month_end + 1
        return months

    async def main(self, conf: MainConf = MainConf()):
        """默认回补最近 1 个整月（手工调用时用）。"""
        job = LotDynInfoExportJob()
        if conf.between_ts is None:
            await job.export_all(LotDynInfoExportConf(months=1))
            return
        myfastapi_logger.info(
            f"按区间导出抽奖信息：{datetime.date.fromtimestamp(conf.between_ts[0])}"
            f" ~ {datetime.date.fromtimestamp(conf.between_ts[1])}"
        )
        export_conf = LotDynInfoExportConf(end_ts=conf.between_ts[1])
        for month_start, month_end in self.get_split_ts(conf.between_ts):
            await job.export_month(month_start, month_end, export_conf)
