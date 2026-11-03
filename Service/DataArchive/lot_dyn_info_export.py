"""抽奖信息导出任务：按月把 `bilidyndetail` 解析出的抽奖信息落成 csv.gz。

与冷数据归档**共用同一套产物格式与根目录**（见 `csv_gz_io`），只是内容来源不同：

    {DYN_BACKUP_DIR}/lot_dyn_info/{年}/{YYYY_MM}_lot_dyn_info.csv.gz

特点：

- 只读不删库（`bilidyndetail` 的清理由冷数据归档负责）；
- 一个月一个文件，**只导出已经过完的整月**（当月还在写入，导出会是半截数据）；
- 同一轮里按 `months` 回溯重导最近 N 个整月，整月重导是幂等覆盖，
  这样某次漏跑 / 晚到的历史动态都能在下一轮自愈；
- 解析是流式的：按主键分页读 `dynData`，边解析边攒结果行，不会把整月的
  动态明细一次性读进内存（结果行只保留抽奖动态，量级很小）。
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
    build_monthly_file_path,
    iter_recent_months,
    verify_csv_gz,
    write_csv_gz_atomic,
)
from Service.DataArchive.lot_dyn_parser import LotDynParser
from Service.DataArchive.targets import DynDetailArchiveTarget
from Service.GrpcModule.GrpcSrc.SQLObject.DynDetailSqlHelperMysqlVer import (
    grpc_sql_helper,
)


def _month_str(month_ts: int) -> str:
    return datetime.date.fromtimestamp(month_ts).strftime("%Y_%m")


class LotDynInfoExportJob:
    """按月导出「抽奖信息」业务表。"""

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
        """导出「截止时间之前已过完的最近 N 个整月」（N>=1）。"""
        conf = conf or LotDynInfoExportConf()
        months = max(1, conf.months)
        result = LotDynInfoExportResult()
        periods = list(iter_recent_months(conf.end_ts, months))
        self.log.info(
            f"抽奖信息导出开始：最近 {months} 个整月"
            f"（{len(periods)} 个分片：{self._format_periods(periods)}），"
            f"根目录={conf.root_dir} dry_run={conf.is_dry_run}"
        )
        for month_start, month_end in periods:
            file_result = await self.export_month(month_start, month_end, conf)
            result.files.append(file_result)
            result.total_rows += file_result.row_count
            result.total_file_size += file_result.file_size
            if file_result.is_skipped:
                result.skipped_periods += 1
        self.log.info(
            f"抽奖信息导出结束：文件 {len(result.files)} 个，行 {result.total_rows}，"
            f"跳过 {result.skipped_periods} 个月"
        )
        return result

    async def export_month(
        self,
        month_start: int,
        month_end: int,
        conf: LotDynInfoExportConf,
    ) -> LotDynInfoFileResult:
        """导出单个月份分片（同月重跑直接覆盖，幂等）。"""
        month_str = _month_str(month_start)
        file_result = LotDynInfoFileResult(period=month_str)

        rows = await self._collect_rows(month_start, month_end)
        if not rows:
            file_result.is_skipped = True
            file_result.skip_reason = "当月没有抽奖动态"
            self.log.info(f"[{self.job_name}] {month_str} 没有抽奖动态，跳过")
            return file_result

        if conf.is_dry_run:
            file_result.row_count = len(rows)
            self.log.info(
                f"[{self.job_name}] {month_str} dry_run：待导出 {len(rows)} 行"
            )
            return file_result

        file_path = build_monthly_file_path(conf.root_dir, self.job_name, month_start)
        row_count = write_csv_gz_atomic(file_path, self.columns, rows)
        file_result.file_path = file_path
        file_result.row_count = row_count
        if row_count == 0:
            # 收集时有数据、写出却为 0 行，只可能是当月恰好全被过滤，按跳过处理
            file_result.is_skipped = True
            file_result.skip_reason = "写出 0 行"
            return file_result

        file_result.file_size = os.path.getsize(file_path)
        is_ok, reason = verify_csv_gz(file_path, row_count)
        file_result.is_verified = is_ok
        if not is_ok:
            self.log.error(f"[{self.job_name}] {month_str} 回读校验失败：{reason}")
        else:
            self.log.info(
                f"[{self.job_name}] {month_str} 导出 {row_count} 行 -> "
                f"{file_path}（{file_result.file_size} 字节）"
            )
        return file_result

    async def _collect_rows(
        self, month_start: int, month_end: int
    ) -> list[list[str]]:
        """流式读整月 `dynData` 并解析；只有抽奖动态会被收下来，量级很小。"""
        rows: list[list[str]] = []
        async for (dyn_data,) in self.target.iter_column_values(
            month_start, month_end, ["dynData"]
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

    @staticmethod
    def _format_periods(periods: list[tuple[int, int]]) -> str:
        if not periods:
            return "无可用整月"
        return "、".join(f"{_month_str(start)}" for start, _ in periods)
