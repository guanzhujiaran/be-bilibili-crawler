"""冷数据归档服务：按月把冷数据导出成 csv.gz，校验通过后可选删库。

整体流程（单表、单月）：

1. 按「归档分片字段」分批取该月所有行（主键 keyset 翻页，不整表进内存）；
2. 边读边写 `{root}/{表名}/{年}/{YYYY_MM}_{表名}.csv.gz.new.tmp`；
3. 关闭后原子改名为正式文件：避免半截压缩包被误当成有效归档；
4. 回读 gzip 统计行数，必须与写入行数一致，否则视为失败、**不删库**；
5. 校验通过且开启删除时，按主键分批删除这批行。

按月而不是按天：清理是「回收磁盘」的运维动作，没必要每天切一个文件，
月度分片能把小文件数量和上传网盘的对象数降一个量级，也让删除批次的提交
次数更少；代价是当前这个月要等到整月过完且越过保留窗口才会归档。

文件格式与路径由 `csv_gz_io` 统一提供（里层标准 CSV、外层 gzip），
与业务导出（抽奖信息）完全同构。

安全边界：
- 只归档「整月都已经冷掉」的月份（`month_end < cutoff_ts`），不碰保留窗口内的数据，
  也不碰还在写入的当月；
- 当月没有数据、或历史上已归档过（文件存在且本次读到 0 行）时跳过，绝不覆盖已有归档；
- 任意一步失败只影响当月，其他月继续；删库只会发生在该月校验通过之后。
"""

import csv
import datetime
import os
import zlib
from collections.abc import Iterator

from CONFIG import settings
from log.base_log import myfastapi_logger
from Models.database_archive.cold_data_archive_models import (
    ArchiveFileResult,
    ColdArchiveConf,
    TableArchiveResult,
)
from Service.DataArchive.csv_gz_io import (
    build_monthly_file_path,
    count_csv_gz_rows,
    format_csv_value,
    iter_csv_gz_rows,
    iter_month_ranges,
    open_csv_gz_write,
    verify_csv_gz,
    write_csv_gz,
)
from Service.DataArchive.targets import BaseColdArchiveTarget, build_default_targets

_SECONDS_PER_DAY = 86400


class ColdDataArchiver:
    """冷数据导出 / 校验 / 清理的统一入口。"""

    def __init__(self, conf: ColdArchiveConf | None = None):
        self.conf = conf or ColdArchiveConf()
        self.log = myfastapi_logger
        self.is_delete = (
            self.conf.is_delete_after_archive
            if self.conf.is_delete_after_archive is not None
            else bool(settings.COLD_ARCHIVE_DELETE_AFTER)
        )
        self.targets = build_default_targets()

    # region 对外入口
    async def archive_all(self) -> list[TableArchiveResult]:
        """归档所有选中表；单表失败不影响其他表。"""
        selected = [t for t in self.targets if t.table_name in self.conf.tables]
        self.log.info(
            f"冷数据归档开始：表={[t.table_name for t in selected]}"
            f" 根目录={self.conf.root_dir} 删除={self.is_delete}"
            f" dry_run={self.conf.is_dry_run}"
        )
        results: list[TableArchiveResult] = []
        for target in selected:
            try:
                results.append(await self.archive_table(target))
            except Exception as e:
                self.log.exception(f"[{target.table_name}] 冷数据归档失败：{e}")
                results.append(
                    TableArchiveResult(
                        table=target.table_name,
                        db_alias=target.db_alias,
                        keep_days=0,
                        cutoff_ts=0,
                        is_skipped=True,
                        skip_reason=f"归档异常：{e}",
                    )
                )
        total_rows = sum(r.total_rows for r in results)
        total_files = sum(len(r.files) for r in results)
        total_deleted = sum(r.deleted_rows for r in results)
        self.log.info(
            f"冷数据归档结束：文件 {total_files} 个，行 {total_rows}，删除 {total_deleted}"
        )
        return results

    # endregion

    # region 单表归档
    async def archive_table(
        self, target: BaseColdArchiveTarget
    ) -> TableArchiveResult:
        keep_days = (
            self.conf.keep_days_override
            if self.conf.keep_days_override is not None
            else target.get_default_keep_days()
        )
        cutoff_ts = self.conf.end_ts - keep_days * _SECONDS_PER_DAY
        result = TableArchiveResult(
            table=target.table_name,
            db_alias=target.db_alias,
            keep_days=keep_days,
            cutoff_ts=cutoff_ts,
        )
        earliest_ts = await target.get_earliest_ts()
        if earliest_ts is None:
            result.is_skipped = True
            result.skip_reason = "表内无数据"
            self.log.info(f"[{target.table_name}] 表内无数据，跳过")
            return result
        if earliest_ts >= cutoff_ts:
            result.is_skipped = True
            result.skip_reason = (
                f"最早数据 {datetime.datetime.fromtimestamp(earliest_ts)}"
                f" 落在保留窗口内（{keep_days} 天）"
            )
            self.log.info(f"[{target.table_name}] {result.skip_reason}，跳过")
            return result

        self.log.info(
            f"[{target.table_name}] 开始归档：最早数据="
            f"{datetime.datetime.fromtimestamp(earliest_ts)}"
            f" 冷热分界={datetime.datetime.fromtimestamp(cutoff_ts)}"
            f" 保留 {keep_days} 天"
        )
        processed_months = 0
        for month_start, month_end in iter_month_ranges(earliest_ts, cutoff_ts):
            # 首次上线可能要处理好几年历史，用 max_months_per_run 限制单次运行的月数，
            # 让任务可以在多次调度里分批推进，避免一次跑太久。
            if (
                self.conf.max_months_per_run
                and processed_months >= self.conf.max_months_per_run
            ):
                self.log.info(
                    f"[{target.table_name}] 达到单次运行上限 "
                    f"{self.conf.max_months_per_run} 个月，本轮先停在这里"
                )
                break
            processed_months += 1
            try:
                file_result = await self._archive_one_month(
                    target, month_start, month_end
                )
            except Exception as e:
                # 单月失败（校验不通过 / 删库出错）只影响当月，剩下的月继续
                self.log.exception(
                    f"[{target.table_name}] "
                    f"{datetime.date.fromtimestamp(month_start).strftime('%Y_%m')}"
                    f" 归档失败：{e}"
                )
                file_result = ArchiveFileResult(
                    period=datetime.date.fromtimestamp(month_start).strftime("%Y_%m"),
                    row_count=0,
                )
            result.files.append(file_result)
            result.total_rows += file_result.row_count
            result.total_file_size += file_result.file_size
            if file_result.is_deleted:
                result.deleted_rows += file_result.row_count
            if file_result.row_count == 0 and not file_result.file_path:
                result.skipped_periods += 1
        self.log.info(
            f"[{target.table_name}] 归档完成：文件 {len(result.files)} 个，"
            f"行 {result.total_rows}，压缩后 {result.total_file_size} 字节，"
            f"删除 {result.deleted_rows} 行，跳过 {result.skipped_periods} 个月"
        )
        return result

    async def _archive_one_month(
        self, target: BaseColdArchiveTarget, month_start: int, month_end: int
    ) -> ArchiveFileResult:
        month_str = datetime.date.fromtimestamp(month_start).strftime("%Y_%m")
        file_path = build_monthly_file_path(
            self.conf.root_dir, target.table_name, month_start
        )
        file_result = ArchiveFileResult(period=month_str)
        is_exists = os.path.exists(file_path)

        primary_keys: list[str | int] = []
        pk_index = target.columns.index(target.primary_key_name)
        need_pk = not self.conf.is_dry_run and self.is_delete
        row_count = 0
        # 先写新行到 .new.tmp：只有确认本次真的读到数据，才会去碰正式文件
        new_tmp_path = f"{file_path}.new.tmp"
        csv_file = None
        writer = None
        try:
            async for row in target.iter_row_values(month_start, month_end):
                if writer is None and not self.conf.is_dry_run:
                    os.makedirs(os.path.dirname(file_path), exist_ok=True)
                    csv_file = open_csv_gz_write(new_tmp_path)
                    writer = csv.writer(csv_file)
                    writer.writerow(target.columns)
                if writer is not None:
                    writer.writerow([format_csv_value(value) for value in row])
                row_count += 1
                if need_pk:
                    primary_keys.append(row[pk_index])  # type: ignore[arg-type]
        finally:
            if csv_file is not None:
                csv_file.close()

        if row_count == 0:
            if os.path.exists(new_tmp_path):
                os.remove(new_tmp_path)
            if is_exists:
                # 历史上已归档过（数据可能上次已删），保留原文件不覆盖
                self.log.debug(
                    f"[{target.table_name}] {month_str} 无数据，已有归档保留"
                )
            return file_result

        if self.conf.is_dry_run:
            file_result.row_count = row_count
            self.log.info(
                f"[{target.table_name}] {month_str} dry_run：待归档 {row_count} 行"
            )
            return file_result

        expected_rows = row_count
        if is_exists:
            # 断点续跑：上次可能「写完归档但删库中断了」，此时库里还剩一部分行。
            # 直接把本次结果覆盖上去会丢掉已删行的归档，所以要把旧归档追加进来。
            expected_rows = self._merge_into_existing_archive(
                target, file_path, new_tmp_path, pk_index
            )
        else:
            os.replace(new_tmp_path, file_path)

        file_result.file_path = file_path
        file_result.row_count = row_count
        file_result.file_row_count = expected_rows
        file_result.file_size = os.path.getsize(file_path)
        is_ok, reason = verify_csv_gz(file_path, expected_rows)
        file_result.is_verified = is_ok
        if not is_ok:
            raise RuntimeError(
                f"{file_path} 回读校验失败：{reason}，已保留文件但不删库"
            )
        self.log.info(
            f"[{target.table_name}] {month_str} 归档 {expected_rows} 行 -> "
            f"{file_path}（{file_result.file_size} 字节）"
        )
        if self.is_delete:
            await target.delete_by_primary_keys(primary_keys)
            file_result.is_deleted = True
            self.log.info(
                f"[{target.table_name}] {month_str} 已删除 {row_count} 行冷数据"
            )
        return file_result

    # endregion

    # region 工具
    def _merge_into_existing_archive(
        self,
        target: BaseColdArchiveTarget,
        file_path: str,
        new_tmp_path: str,
        pk_index: int,
    ) -> int:
        """把本次新增行续写到已有归档包末尾（按主键去重），返回合并后的总行数。

        为什么需要去重：若配置成「只归档不删库」，每次跑都会把同一分片的数据再导出
        一遍，直接追加会让归档包重复膨胀；这里按主键跳过已存在行。

        旧归档不可读时退化为「只用本次数据重建」，并把旧文件备份成 `.broken`。
        """
        existing_primary_keys: set[str] = set()
        try:
            for row in iter_csv_gz_rows(file_path):
                existing_primary_keys.add(row[pk_index])
        except (
            OSError,
            EOFError,
            zlib.error,
            UnicodeDecodeError,
            csv.Error,
            IndexError,
        ) as e:
            self.log.error(
                f"已有归档不可读，改用本次数据重建：{file_path}，{e}；"
                f"旧文件备份为 {file_path}.broken"
            )
            os.replace(file_path, f"{file_path}.broken")
            os.replace(new_tmp_path, file_path)
            return count_csv_gz_rows(file_path)

        existing_rows = len(existing_primary_keys)
        appended = 0

        def _merged_rows() -> Iterator[list[str]]:
            nonlocal appended
            for row in iter_csv_gz_rows(file_path):
                yield row
            for row in iter_csv_gz_rows(new_tmp_path):
                if row[pk_index] in existing_primary_keys:
                    continue
                appended += 1
                yield row

        merge_tmp_path = f"{file_path}.merge.tmp"
        write_csv_gz(merge_tmp_path, target.columns, _merged_rows())
        os.remove(new_tmp_path)
        os.replace(merge_tmp_path, file_path)
        if appended == 0:
            self.log.info(f"{file_path} 已包含本次全部数据（主键重复），未追加新行")
        return existing_rows + appended

    # endregion
