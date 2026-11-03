# -*- coding: utf-8 -*-
"""按日期导出抽奖信息（历史入口，现已收敛到统一的 dyn_backup 任务）。

原来这里自己用 pandas 往 `result/{年}/{月}/*_抽奖信息.csv` 落盘，
产物格式和其它备份（冷数据归档）不一致、也不好整体打包上云；
现在统一由 `Service/DataArchive/lot_dyn_info_export.py` 产出：

    {DYN_BACKUP_DIR}/lot_dyn_info/{年}/{YYYY_MM_DD}_lot_dyn_info.csv.gz

本模块只保留「手工按时间区间回补」的入口，解析逻辑在
`Service/DataArchive/lot_dyn_parser.py`。
"""

import datetime

from log.base_log import myfastapi_logger
from Models.database_archive.lot_dyn_info_models import LotDynInfoExportConf
from Service.DataArchive.lot_dyn_info_export import LotDynInfoExportJob
from Service.GrpcModule.Models.getLotDynSortByDate import MainConf

_SECONDS_PER_DAY = 86400


class LotDynSortByDate:
    """按日期区间导出抽奖信息（薄封装，产出与定时任务完全一致）。"""

    def get_split_ts(self, between_ts: list[int]) -> list[list[int]]:
        """把时间区间切成整天（不含结束当天），与统一任务的日界一致。"""
        start_date_time = datetime.date.fromtimestamp(between_ts[0])
        end_date_time = datetime.date.fromtimestamp(between_ts[1])
        ret_list: list[list[int]] = []
        while end_date_time - start_date_time > datetime.timedelta(0):
            start_date_ts = int(
                datetime.datetime.combine(
                    start_date_time, datetime.time.min
                ).timestamp()
            )
            ret_list.append([start_date_ts, start_date_ts + _SECONDS_PER_DAY - 1])
            start_date_time += datetime.timedelta(days=1)
        return ret_list

    async def main(self, conf: MainConf = MainConf()):
        """默认回补最近 30 天（手工调用时用）。"""
        job = LotDynInfoExportJob()
        if conf.between_ts is None:
            export_conf = LotDynInfoExportConf(days=30)
            await job.export_all(export_conf)
            return
        myfastapi_logger.info(
            f"按区间导出抽奖信息：{datetime.date.fromtimestamp(conf.between_ts[0])}"
            f" ~ {datetime.date.fromtimestamp(conf.between_ts[1])}"
        )
        export_conf = LotDynInfoExportConf(end_ts=conf.between_ts[1])
        for day_start, _day_end in self.get_split_ts(conf.between_ts):
            await job.export_day(day_start, export_conf)
