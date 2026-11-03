"""按天落 csv.gz 的统一读写工具（冷数据归档 / 业务导出共用同一套格式）。

统一约定（所有 dyn_backup 产物都长这样，方便整目录打包上传网盘）：

- 路径：`{root}/{任务名}/{年}/{YYYY_MM_DD}_{任务名}.csv.gz`
- 内容：UTF-8（无 BOM）、首行表头、其余为数据行、逗号分隔、gzip 压缩
- 写入：先写 `{目标}.new.tmp`，写完再原子改名，避免半截压缩包被当成有效归档
- 校验：回读 gzip 数一遍数据行，行数对不上就判定失败（调用方据此决定是否删库）
"""

import csv
import datetime
import gzip
import json
import os
from collections.abc import Iterable, Iterator, Sequence

_TMP_SUFFIX = ".new.tmp"


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


def build_daily_file_path(root_dir: str, job_name: str, day_ts: int) -> str:
    """`{root}/{任务名}/{年}/{YYYY_MM_DD}_{任务名}.csv.gz`。"""
    day = datetime.date.fromtimestamp(day_ts)
    return os.path.join(
        root_dir,
        job_name,
        str(day.year),
        f"{day.strftime('%Y_%m_%d')}_{job_name}.csv.gz",
    )


def iter_csv_gz_rows(file_path: str) -> Iterator[list[str]]:
    """逐行读回 csv.gz（不含表头）。"""
    with gzip.open(file_path, "rt", encoding="utf-8", newline="") as file:
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
    with gzip.open(file_path, "wt", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        for row in rows:
            writer.writerow([format_csv_value(value) for value in row])
            row_count += 1
    return row_count


def write_daily_csv_gz_atomic(
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
    except (OSError, UnicodeDecodeError, csv.Error) as e:
        return False, f"解压/解析失败：{e}"
    if actual_rows != expected_rows:
        return False, f"行数不一致（期望 {expected_rows} 实际 {actual_rows}）"
    return True, ""
