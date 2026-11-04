"""手动执行表空间维护（定时任务之外的人工入口）。

定时任务每月跑一次（见 `Service/BackgroundService/CrawlerScheduler.py` 的
TABLESPACE_MAINTENANCE），这个脚本用于「临时想立刻回收磁盘」或「先干跑看看空洞多大」。

用法（在 be-bilibili-crawler 目录下）：

    # 只统计，不重建
    python -m scripts.database.tablespace_maintenance --dry-run

    # 只维护空洞最大的那张表
    python -m scripts.database.tablespace_maintenance --tables t_lotuserspaceresp

    # 放宽阈值：空洞超过 10% 且大于 0.5GB 就重建
    python -m scripts.database.tablespace_maintenance \
        --min-free-pct 10 --min-free-gb 0.5
"""

import argparse
import asyncio
import json

from Models.database_archive.cold_data_archive_models import ColdArchiveTables
from Models.database_archive.tablespace_maintenance_models import (
    TablespaceMaintenanceConf,
)
from Service.DataArchive.tablespace_maintenance import TablespaceMaintenance


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="表空间维护：按阈值回收冷数据归档删库后残留的 InnoDB 空洞"
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        choices=[*ColdArchiveTables.ALL],
        default=list(ColdArchiveTables.ALL),
        help="要检查的表，默认全部",
    )
    parser.add_argument(
        "--min-free-pct",
        type=float,
        default=None,
        help="空洞占比阈值（%%），默认取配置 TABLESPACE_MAINTENANCE_MIN_FREE_PCT",
    )
    parser.add_argument(
        "--min-free-gb",
        type=float,
        default=None,
        help="空洞绝对值阈值（GB），默认取配置 TABLESPACE_MAINTENANCE_MIN_FREE_GB",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="只统计空洞，不执行 ANALYZE / 重建"
    )
    return parser.parse_args()


async def _main() -> None:
    args = _parse_args()
    conf = TablespaceMaintenanceConf(
        tables=args.tables,
        min_free_pct=args.min_free_pct,
        min_free_bytes=(
            int(args.min_free_gb * 1024**3) if args.min_free_gb is not None else None
        ),
        is_dry_run=args.dry_run,
    )
    result = await TablespaceMaintenance(conf).run_all()
    print(json.dumps(result.model_dump(mode="json"), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_main())
