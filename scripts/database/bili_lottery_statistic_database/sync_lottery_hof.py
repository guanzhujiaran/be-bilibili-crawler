"""
手动同步 lottery_database/bili/lottery_hof 的数据。

lottery_hof（抽奖名人堂 / 中奖榜）接口读取的数据来自 MySQL 的
BiliAtariInfo / BiliUserInfo 两张表，它们由 lotData.lottery_result
（开奖结果）解析而来。本脚本用于手动触发该同步，而不依赖定时爬虫。

典型用法:
  # 全量同步（重算所有 lotData.lottery_result -> BiliAtariInfo/BiliUserInfo）
  uv run python -m scripts.database.bili_lottery_statistic_database.sync_lottery_hof

  # 仅同步某一个抽奖的开奖结果
  uv run python -m scripts.database.bili_lottery_statistic_database.sync_lottery_hof --lottery-id 123456

  # 只统计待同步的源数据条数，不做任何写入
  uv run python -m scripts.database.bili_lottery_statistic_database.sync_lottery_hof --count

  # 全量同步但不同步 Redis 的 sync_ts（接口返回的“最后同步时间”）
  uv run python -m scripts.database.bili_lottery_statistic_database.sync_lottery_hof --skip-sync-ts
"""

import argparse
import asyncio
import os
import sys

# 确保项目根目录在 sys.path 中
_project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from sqlalchemy import select, func

from log.base_log import myfastapi_logger
from Models.lottery_database.bili.LotteryDataModels import BiliLotStatisticLotTypeEnum
from Service.GrpcModule.GrpcSrc.SQLObject.DynDetailSqlHelperMysqlVer import grpc_sql_helper
from Service.GrpcModule.GrpcSrc.SQLObject.models import Lotdata
from dao.biliLotteryStatisticRedisObj import lottery_data_statistic_redis


async def count_source_rows(lottery_id: int | None = None) -> int:
    """统计 lotData 中存在开奖结果（lottery_result）的源数据条数。"""
    where = [Lotdata.lottery_result.isnot(None)]
    if lottery_id is not None:
        where.append(Lotdata.lottery_id == lottery_id)
    async with grpc_sql_helper.async_session() as session:
        stmt = select(func.count(Lotdata.lottery_id)).where(*where)
        res = await session.execute(stmt)
        return res.scalar() or 0


async def refresh_sync_ts() -> None:
    """刷新 Redis 中 lottery_hof 各 lot_type 的 sync_ts，使接口报告最新的同步时间。

    即便 Redis 不可用也不影响核心的 MySQL 同步，因此失败仅告警。
    """
    import time

    try:
        for lot_type in BiliLotStatisticLotTypeEnum:
            await lottery_data_statistic_redis.set_sync_ts(
                lot_type=lot_type, ts=int(time.time())
            )
        myfastapi_logger.info("已刷新 Redis 中 lottery_hof 各 lot_type 的 sync_ts")
    except Exception as e:  # noqa: BLE001
        myfastapi_logger.warning(f"刷新 Redis sync_ts 失败（不影响 MySQL 数据同步）: {e}")


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="手动同步 lottery_database/bili/lottery_hof 的中奖榜数据"
    )
    parser.add_argument(
        "--lottery-id", type=int, default=None,
        help="仅同步指定抽奖ID的开奖结果；不传则全量同步",
    )
    parser.add_argument(
        "--skip-sync-ts", action="store_true",
        help="全量同步后不同步 Redis 的 sync_ts（仅对全量同步生效）",
    )
    parser.add_argument(
        "--count", action="store_true",
        help="仅统计待同步的源数据条数，不做任何写入",
    )
    args = parser.parse_args()

    if args.count:
        total = await count_source_rows(lottery_id=args.lottery_id)
        scope = f"lottery_id={args.lottery_id}" if args.lottery_id else "全部"
        myfastapi_logger.info(f"[{scope}] 含有开奖结果的 lotData 源数据条数: {total}")
        return

    scope = f"lottery_id={args.lottery_id}" if args.lottery_id else "全量"
    myfastapi_logger.info(f"开始手动同步 lottery_hof 数据（{scope}）...")
    await grpc_sql_helper.sync_all_lottery_result_2_bili_user_info(
        lottery_id=args.lottery_id
    )
    myfastapi_logger.info(f"lottery_hof 数据同步完成（{scope}）")

    # 仅全量同步时刷新 sync_ts（单抽奖增量同步不更新“最后全量同步时间”）
    if args.lottery_id is None and not args.skip_sync_ts:
        await refresh_sync_ts()


if __name__ == "__main__":
    asyncio.run(main())
