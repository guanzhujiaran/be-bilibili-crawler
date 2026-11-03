"""抽奖信息导出任务：按天把 `bilidyndetail` 解析出的抽奖信息落成 csv.gz。

与冷数据归档**共用同一套产物格式与根目录**（见 `csv_gz_io`），只是内容来源不同：

    {DYN_BACKUP_DIR}/lot_dyn_info/{年}/{YYYY_MM_DD}_lot_dyn_info.csv.gz

特点：

- 只读不删库（`bilidyndetail` 的清理由冷数据归档负责）；
- 每天一个文件、同一天重跑直接覆盖（幂等），方便漏跑后回补；
- 解析是流式的：按主键分页读 `dynData`，边解析边攒结果行，不会把整天的
  几十万条动态明细一次性读进内存。
"""

import datetime
import os
from typing import ClassVar

from log.base_log import myfastapi_logger
from Models.database_archive.lot_dyn_info_models import (
    LOT_DYN_INFO_JOB_NAME,
    LotDynInfoExportConf,
    LotDynInfoExportResult,
    LotDynInfoFileResult,
)
from Service.DataArchive.csv_gz_io import (
    build_daily_file_path,
    verify_csv_gz,
    write_daily_csv_gz_atomic,
)
from Service.DataArchive.lot_dyn_parser import LotDynParser
from Service.DataArchive.targets import DynDetailArchiveTarget
from Service.GrpcModule.GrpcSrc.SQLObject.DynDetailSqlHelperMysqlVer import (
    grpc_sql_helper,
)

_SECONDS_PER_DAY = 86400


class LotDynInfoExportJob:
    """按天导出「抽奖信息」业务表。"""

    job_name: ClassVar[str] = LOT_DYN_INFO_JOB_NAME
    # 列定义与历史 CSV 保持一致，避免下游（人工分析 / 词云）读不了
    columns: ClassVar[list[str]] = [
        "发布者空间",
        "动态链接",
        "up昵称",
        "账号类型",
        "发布时间",
        "动态内容",
        "评论数",
        "转发数",
        "点赞数",
        "是否需要人工判断",
        "抽奖类型",
        "抽奖id",
        "需要携带的词",
    ]

    def __init__(
        self,
        target: DynDetailArchiveTarget | None = None,
        parser: LotDynParser | None = None,
    ):
        self.target = target or DynDetailArchiveTarget(grpc_sql_helper)
        self.parser = parser or LotDynParser()
        self.log = myfastapi_logger

    async def export_all(
        self, conf: LotDynInfoExportConf | None = None
    ) -> LotDynInfoExportResult:
        """导出「截止时间之前已结束的最近 N 天」（N>=1）。"""
        conf = conf or LotDynInfoExportConf()
        days = max(1, conf.days)
        result = LotDynInfoExportResult()
        # 只导出已经过完的整天，避免把还在写入的当天导成半截数据
        last_day = datetime.date.fromtimestamp(
            conf.end_ts
        ) - datetime.timedelta(days=1)
        self.log.info(
            f"抽奖信息导出开始：最近 {days} 天（{last_day} 及之前），"
            f"根目录={conf.root_dir} dry_run={conf.is_dry_run}"
        )
        for offset in range(days):
            day = last_day - datetime.timedelta(days=offset)
            day_start = int(
                datetime.datetime.combine(day, datetime.time.min).timestamp()
            )
            file_result = await self.export_day(day_start, conf)
            result.files.append(file_result)
            result.total_rows += file_result.row_count
            result.total_file_size += file_result.file_size
            if file_result.is_skipped:
                result.skipped_days += 1
        self.log.info(
            f"抽奖信息导出结束：文件 {len(result.files)} 个，行 {result.total_rows}，"
            f"跳过 {result.skipped_days} 天"
        )
        return result

    async def export_day(
        self, day_start: int, conf: LotDynInfoExportConf
    ) -> LotDynInfoFileResult:
        day_end = day_start + _SECONDS_PER_DAY - 1
        day_str = datetime.date.fromtimestamp(day_start).strftime("%Y_%m_%d")
        file_result = LotDynInfoFileResult(day=day_str)

        rows = await self._collect_rows(day_start, day_end)
        if not rows:
            file_result.is_skipped = True
            file_result.skip_reason = "当天没有抽奖动态"
            self.log.info(f"[{self.job_name}] {day_str} 没有抽奖动态，跳过")
            return file_result

        if conf.is_dry_run:
            file_result.row_count = len(rows)
            self.log.info(
                f"[{self.job_name}] {day_str} dry_run：待导出 {len(rows)} 行"
            )
            return file_result

        file_path = build_daily_file_path(conf.root_dir, self.job_name, day_start)
        row_count = write_daily_csv_gz_atomic(file_path, self.columns, rows)
        file_result.file_path = file_path
        file_result.row_count = row_count
        if row_count == 0:
            # 收集时有数据、写出却为 0 行，只可能是当天恰好全被过滤，按跳过处理
            file_result.is_skipped = True
            file_result.skip_reason = "写出 0 行"
            return file_result

        file_result.file_size = os.path.getsize(file_path)
        is_ok, reason = verify_csv_gz(file_path, row_count)
        file_result.is_verified = is_ok
        if not is_ok:
            self.log.error(f"[{self.job_name}] {day_str} 回读校验失败：{reason}")
        else:
            self.log.info(
                f"[{self.job_name}] {day_str} 导出 {row_count} 行 -> "
                f"{file_path}（{file_result.file_size} 字节）"
            )
        return file_result

    async def _collect_rows(
        self, day_start: int, day_end: int
    ) -> list[list[str]]:
        """流式读当天 `dynData` 并解析；只有抽奖动态会被收下来，量级很小。"""
        rows: list[list[str]] = []
        async for (dyn_data,) in self.target.iter_column_values(
            day_start, day_end, ["dynData"]
        ):
            lot = self.parser.parse_one(dyn_data)  # type: ignore[arg-type]
            if lot is None:
                continue
            rows.append(
                [
                    lot.author_space,
                    lot.dyn_url,
                    lot.author_name,
                    lot.official_verify_type,
                    lot.pub_time,
                    lot.dynamic_content,
                    lot.comment_count,
                    lot.forward_count,
                    lot.like_count,
                    lot.Manual_judge,
                    lot.lot_type,
                    lot.lot_rid,
                    lot.premsg,
                ]
            )
        return rows
