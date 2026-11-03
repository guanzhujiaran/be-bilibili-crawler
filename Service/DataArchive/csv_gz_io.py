"""按「标准 CSV + gzip」落盘的统一读写工具（冷数据归档 / 业务导出共用）。

统一约定（所有 dyn_backup 产物都长这样，方便整目录打包上传网盘）：

- 路径（**一律按月分片**）：`{root}/{任务名}/{年}/{YYYY_MM}_{任务名}.csv.gz`
- 内容：**最里层永远是标准 CSV 文本**（UTF-8 无 BOM、首行表头、逗号分隔），
  外层只做 gzip 压缩，解压后可直接用任意 CSV 工具打开。
- 压缩格式固定 gzip：与历史归档文件（`*.csv.gz`）保持一致，不引入第二种后缀；
  `compresslevel=9` 是体积与速度的常规折中。
- 写入：先写 `{目标}.new.tmp`，写完再原子改名，避免半截压缩包被当成有效归档。
- 校验：回读 gzip 数一遍数据行，行数对不上就判定失败（调用方据此决定是否删库）。
"""

import csv
import datetime
import gzip
import json
import os
import zlib
from collections.abc import Iterable, Iterator, Sequence
from typing import IO, Final

# 归档/导出产物的统一后缀：外层 gzip、里层 csv
CSV_SUFFIX: Final[str] = ".csv.gz"
_COMPRESS_LEVEL: Final[int] = 9
_TMP_SUFFIX: Final[str] = ".new.tmp"


def format_csv_value(value: object) -> str:
    """把数据库字段值转成 CSV 单元格文本（JSON 文本原样保留）。"""
    if value is None:
        return ""
    if isinstance(value, datetime.datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, default=str)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def build_monthly_file_path(root_dir: str, job_name: str, month_ts: int) -> str:
    """`{root}/{任务名}/{年}/{YYYY_MM}_{任务名}.csv.gz`。"""
    day = datetime.date.fromtimestamp(month_ts)
    return os.path.join(
        root_dir,
        job_name,
        str(day.year),
        f"{day.strftime('%Y_%m')}_{job_name}{CSV_SUFFIX}",
    )


def month_bounds(month_ts: int) -> tuple[int, int]:
    """返回 `month_ts` 所属月份的闭区间 `[月初 00:00:00, 月末 23:59:59]`（秒）。"""
    month = datetime.date.fromtimestamp(month_ts).replace(day=1)
    start = int(datetime.datetime.combine(month, datetime.time.min).timestamp())
    next_month = (month.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
    end = (
        int(datetime.datetime.combine(next_month, datetime.time.min).timestamp()) - 1
    )
    return start, end


def iter_month_ranges(start_ts: int, cutoff_ts: int) -> Iterator[tuple[int, int]]:
    """把 `[start_ts, cutoff_ts)` 切成整月，只产出完全早于 `cutoff_ts` 的月份。

    以本地时区 00:00:00 为日界、每月 1 日为月界；`month_end >= cutoff_ts` 的月份
    （含当月）不产出，避免把保留窗口内或还在写入的数据当成可归档/可导出的完整月份。
    """
    month_start, month_end = month_bounds(start_ts)
    while True:
        if month_start >= cutoff_ts or month_end >= cutoff_ts:
            return
        yield month_start, month_end
        month_start, month_end = month_bounds(month_end + 1)


def iter_recent_months(end_ts: int, count: int) -> Iterator[tuple[int, int]]:
    """产出 `end_ts` 之前已过完的最近 `count` 个整月，按时间从早到晚。"""
    current_start, _ = month_bounds(end_ts)
    months: list[tuple[int, int]] = []
    cursor = current_start - 1
    for _ in range(max(0, count)):
        start, end = month_bounds(cursor)
        months.append((start, end))
        cursor = start - 1
    yield from reversed(months)


def open_csv_gz_write(file_path: str) -> IO[str]:
    """以写模式打开 csv.gz（调用方负责 close）。"""
    return gzip.open(
        file_path,
        "wt",
        encoding="utf-8",
        newline="",
        compresslevel=_COMPRESS_LEVEL,
    )


def open_csv_gz_read(file_path: str) -> IO[str]:
    """以读模式打开 csv.gz（调用方负责 close）。"""
    return gzip.open(file_path, "rt", encoding="utf-8", newline="")


def iter_csv_gz_rows(file_path: str) -> Iterator[list[str]]:
    """逐行读回 csv.gz（不含表头）。"""
    with open_csv_gz_read(file_path) as file:
        reader = csv.reader(file)
        next(reader, None)  # 跳过表头
        yield from reader


def count_csv_gz_rows(file_path: str) -> int:
    return sum(1 for _ in iter_csv_gz_rows(file_path))


def write_csv_gz(
    file_path: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[object]],
) -> int:
    """把数据行写成 csv.gz（直接写目标路径，不做原子改名），返回数据行数。"""
    directory = os.path.dirname(file_path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    row_count = 0
    with open_csv_gz_write(file_path) as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        for row in rows:
            writer.writerow([format_csv_value(value) for value in row])
            row_count += 1
    return row_count


def write_csv_gz_atomic(
    file_path: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[object]],
) -> int:
    """写 `{目标}.new.tmp` 确认有数据后原子改名；没有数据则清理临时文件、不产生文件。"""
    tmp_path = f"{file_path}{_TMP_SUFFIX}"
    row_count = write_csv_gz(tmp_path, columns, rows)
    if row_count == 0:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        return 0
    os.replace(tmp_path, file_path)
    return row_count


def verify_csv_gz(file_path: str, expected_rows: int) -> tuple[bool, str]:
    """回读校验：文件存在、可解压、数据行数与期望一致。返回 (是否通过, 失败原因)。"""
    if not os.path.exists(file_path):
        return False, "文件不存在"
    if os.path.getsize(file_path) == 0:
        return False, "文件为空"
    try:
        actual_rows = count_csv_gz_rows(file_path)
    except (
        OSError,
        EOFError,
        zlib.error,
        UnicodeDecodeError,
        csv.Error,
    ) as e:
        return False, f"解压/解析失败：{e}"
    if actual_rows != expected_rows:
        return False, f"行数不一致（期望 {expected_rows} 实际 {actual_rows}）"
    return True, ""
