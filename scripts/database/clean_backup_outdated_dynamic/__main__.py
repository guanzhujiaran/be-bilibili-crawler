"""手动执行 dyn_backup（定时任务之外的人工入口）。

用法（在 be-bilibili-crawler 目录下）：

    # 先干跑：抽奖信息 + 冷数据归档都只统计，不落盘不删库
    python -m scripts.database.clean_backup_outdated_dynamic --dry-run

    # 只跑抽奖信息导出，回补最近 30 天
    python -m scripts.database.clean_backup_outdated_dynamic \
        --jobs lot_dyn_info --lot-dyn-days 30

    # 只归档 dyndetail 的 bilidyndetail，保留 10 天，归档后删库
    python -m scripts.database.clean_backup_outdated_dynamic \
        --jobs archive --tables bilidyndetail --keep-days 10

    # 只导出不删除（先观察几个归档包再开删）
    python -m scripts.database.clean_backup_outdated_dynamic --no-delete
"""

import argparse
import asyncio
import json

from Models.database_archive.cold_data_archive_models import (
    ColdArchiveConf,
    ColdArchiveTables,
)
from Models.database_archive.lot_dyn_info_models import LotDynInfoExportConf
from scripts.database.clean_backup_outdated_dynamic.cleaner import (
    do_clean,
    do_export_lot_dyn_info,
)

_JOB_ARCHIVE = "archive"
_JOB_LOT_DYN_INFO = "lot_dyn_info"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="dyn_backup：抽奖信息导出 + 冷数据归档（csv.gz），可选删库"
    )
    parser.add_argument(
        "--jobs",
        nargs="+",
        choices=[_JOB_ARCHIVE, _JOB_LOT_DYN_INFO],
        default=[_JOB_LOT_DYN_INFO, _JOB_ARCHIVE],
        help="要跑的任务，默认两个都跑（顺序固定：先导出抽奖信息再归档）",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        choices=[*ColdArchiveTables.ALL],
        default=list(ColdArchiveTables.ALL),
        help="要归档的表，默认全部",
    )
    parser.add_argument(
        "--keep-days", type=int, default=None, help="覆盖所有表的保留天数"
    )
    parser.add_argument(
        "--end-ts", type=int, default=None, help="截止时间戳（秒），默认当前时间"
    )
    parser.add_argument("--root-dir", type=str, default="", help="覆盖归档根目录")
    parser.add_argument(
        "--max-days",
        type=int,
        default=0,
        help="单次最多处理多少天（0=不限），首次处理历史数据时建议限制",
    )
    parser.add_argument(
        "--lot-dyn-days",
        type=int,
        default=None,
        help="抽奖信息回补天数，默认取配置 LOT_DYN_INFO_DAYS",
    )
    parser.add_argument(
        "--no-delete", action="store_true", help="只导出归档，不删除数据库记录"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="只统计不落盘、不删除"
    )
    return parser.parse_args()


async def _main() -> None:
    args = _parse_args()
    archive_conf = ColdArchiveConf(
        tables=args.tables,
        keep_days_override=args.keep_days,
        is_delete_after_archive=False if args.no_delete else None,
        is_dry_run=args.dry_run,
        max_days_per_run=args.max_days,
    )
    if args.end_ts:
        archive_conf.end_ts = args.end_ts
    if args.root_dir:
        archive_conf.root_dir = args.root_dir

    output: dict[str, object] = {}
    if _JOB_LOT_DYN_INFO in args.jobs:
        lot_conf = LotDynInfoExportConf(is_dry_run=args.dry_run)
        if args.lot_dyn_days is not None:
            lot_conf.days = args.lot_dyn_days
        if args.end_ts:
            lot_conf.end_ts = args.end_ts
        if args.root_dir:
            lot_conf.root_dir = args.root_dir
        lot_result = await do_export_lot_dyn_info(lot_conf)
        output[_JOB_LOT_DYN_INFO] = [
            file.model_dump(mode="json") for file in lot_result.files
        ]
    if _JOB_ARCHIVE in args.jobs:
        archive_results = await do_clean(archive_conf)
        output[_JOB_ARCHIVE] = [
            result.model_dump(mode="json") for result in archive_results
        ]
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_main())
