# -*- coding: utf-8 -*-
"""按 id 区间补数（容器停机状态下也能跑）。

为什么需要它：各爬虫的断点只是「最后成功的 id」，代理池整体故障期间失败的 id
会被后续成功越过，形成永久缺口（2026-11-16 dyn_detail 丢了 8 条 rid，断点已推进
到 345845731）。本脚本按 id 区间逐条重跑**既有爬虫的 handle_fetch / pipeline**，
保证解析与写库与线上完全一致，写入全部走幂等 upsert，重复跑不产生脏数据。

设计约束：
- **不启动调度器、不注册 MQ 消费者**：入口独立 ``asyncio.run``；
- 单条失败只记日志 + 落 ``crawl_failed_task``（pending），不中断整批；
- 全部走各爬虫自己的实现，不复制解析逻辑。

用法（容器已停，跑一次性容器）::

    docker compose -f docker-compose.dev.yml run --rm be-bilibili-crawler \\
        python -m scripts.backfill.backfill_missing --task dyn_detail \\
        --start-rid 345841700 --end-rid 345845731 --concurrency 5

    # 只预览（不发请求、不写库）
    ... --task dyn_detail --start-rid X --end-rid Y --dry-run

    # 预约抽奖（reserve_id 区间） / 预约关系信息（按 sid 刷新并写库）
    ... --task reserve_scrapy --start-rid X --end-rid Y
    ... --task reserve_info   --start-sid X --end-sid Y

    # 官方抽奖动态 / 预约抽奖 notice（写库走 MQ，需要 RabbitMQ 在线）
    ... --task lottery_dyn     --start-id X --end-id Y
    ... --task lottery_reserve --start-id X --end-id Y
    # lottery_dyn 也可以用秒级时间戳反算雪花 id
    ... --task lottery_dyn --start-ts 1763225640 --end-ts 1763300000

退出码：0 = 全部成功（或 dry-run）；1 = 有单条失败（明细见日志与 crawl_failed_task）。
"""

import argparse
import asyncio
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Awaitable, Callable, Protocol

# 确保项目根目录在 sys.path 中，否则导入 CONFIG / Service 会失败
_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

# protobuf 生成的模块内部使用 `bilibili.xxx` 绝对导入，必须把 GrpcProto 加入 sys.path
# （与 main.py / query_main.py 的启动头保持一致，否则 import grpc_api 会失败）
_grpc_proto_path = str(_project_root / "Service" / "GrpcModule" / "Grpc" / "GrpcProto")
if _grpc_proto_path not in sys.path:
    sys.path.insert(0, _grpc_proto_path)

from alembic import command  # noqa: E402
from alembic.config import Config as AlembicConfig  # noqa: E402
from bili_common.core import StrEnumAutoDoc  # noqa: E402
from pydantic import BaseModel, Field  # noqa: E402

from log.base_log import (  # noqa: E402
    myfastapi_logger,
    official_lot_logger,
    reserve_lot_logger,
)
from Service.BaseCrawler.model.base import WorkerStatus  # noqa: E402
from Service.BaseCrawler.retry_queue import (  # noqa: E402
    RetryTaskType,
    crawl_failed_task_dao,
)
from Service.GrpcModule.GrpcSrc.getDynDetail import (  # noqa: E402
    DynDetailParams,
    dyn_detail_scrapy,
)
from Service.opus新版官方抽奖.bili_lottery_api.scrapyLotteryDataFromBapi import (  # noqa: E402
    LotteryApiRobot,
)
from Service.opus新版官方抽奖.预约抽奖.etc.scrapyReserveJsonData import (  # noqa: E402
    ReserveParams,
    reserve_robot,
)
from Utils.通用.dynamic_id_caculate import (  # noqa: E402
    dynamic_id_2_ts,
    ts_2_fake_dynamic_id,
)

#: 单条任务最多跑多久（秒）：超过就记失败，避免一条卡死拖住整批
ITEM_TIMEOUT_SECONDS: int = 300


class BackfillTask(StrEnumAutoDoc):
    """可补的任务类型（与各爬虫的 retry_task_type 同名）。"""

    DYN_DETAIL = RetryTaskType.DYN_DETAIL.value
    RESERVE_INFO = RetryTaskType.RESERVE_INFO.value
    RESERVE_SCRAPY = RetryTaskType.RESERVE_SCRAPY.value
    LOTTERY_DYN = RetryTaskType.LOTTERY_DYN.value
    LOTTERY_RESERVE = RetryTaskType.LOTTERY_RESERVE.value


class BackfillOptions(BaseModel):
    """一次补数运行的参数（argparse 解析后的形态，避免到处传裸 dict）。"""

    task: BackfillTask
    start_id: int
    end_id: int
    concurrency: int = Field(default=5, ge=1, le=50)
    limit: int = Field(default=0, ge=0)
    dry_run: bool = False

    @property
    def ids(self) -> list[int]:
        """闭区间的 id 列表（按 limit 截断；limit<=0 表示不限制）。"""
        if self.end_id < self.start_id:
            return []
        upper = (
            self.end_id
            if self.limit <= 0
            else min(self.end_id, self.start_id + self.limit - 1)
        )
        return list(range(self.start_id, upper + 1))


class BackfillStats(BaseModel):
    """批次统计（结尾汇总用）。"""

    total: int = 0
    ok: int = 0
    failed: int = 0
    failed_ids: list[int] = Field(default_factory=list)


class BackfillRunner(Protocol):
    """补数执行器：每种任务只需声明「怎么跑一条」。"""

    task: BackfillTask

    async def run_one(self, business_id: int) -> WorkerStatus: ...


@dataclass(frozen=True)
class CrawlerRunner:
    """通用实现：直接调爬虫实例的 handle_fetch / pipeline（同一套解析与写库）。"""

    task: BackfillTask
    label: str
    fetch: Callable[[int], Awaitable[WorkerStatus]]

    async def run_one(self, business_id: int) -> WorkerStatus:
        return await self.fetch(business_id)


# ---------------------------------------------------------------------------
# 各任务的执行器（全部复用线上爬虫，不复制业务逻辑）
# ---------------------------------------------------------------------------


async def _fetch_dyn_detail(rid: int) -> WorkerStatus:
    return await dyn_detail_scrapy.handle_fetch(DynDetailParams(rid=rid))


async def _fetch_reserve_scrapy(sid: int) -> WorkerStatus:
    return await reserve_robot.handle_fetch(ReserveParams(reserve_id=sid))


async def _fetch_reserve_info(sid: int) -> WorkerStatus:
    """强制走 B 站接口刷新预约关系信息并写回本地库（幂等 upsert）。"""
    _, resp_dict = await reserve_robot.handle_fetch_reserve_info(sid, True)
    return WorkerStatus.complete if resp_dict.get("code") == 0 else WorkerStatus.nullData


def _build_lottery_fetch(business_type: int) -> Callable[[int], Awaitable[WorkerStatus]]:
    """构造抽奖 notice 的补数函数（business_type 2=官方抽奖动态，10=预约）。"""
    crawler = LotteryApiRobot(
        log=official_lot_logger if business_type == 2 else reserve_lot_logger,
        business_type=business_type,
    )
    crawler.crawler_name = f"BACKFILL_LOTTERY_{business_type}"
    # 补数不能动生产游标：否则会把 Redis 里的推进位置拉回到历史区间
    crawler.freeze_cursor()

    async def _fetch(business_id: int) -> WorkerStatus:
        return await crawler.pipeline(business_type, business_id)

    return _fetch


def build_runner(task: BackfillTask) -> BackfillRunner:
    """按任务类型构造执行器。"""
    match task:
        case BackfillTask.DYN_DETAIL:
            return CrawlerRunner(
                task=task, label="动态详情(bilidyndetail)", fetch=_fetch_dyn_detail
            )
        case BackfillTask.RESERVE_SCRAPY:
            return CrawlerRunner(
                task=task, label="预约抽奖(t_up_reserve_relation_info)", fetch=_fetch_reserve_scrapy
            )
        case BackfillTask.RESERVE_INFO:
            return CrawlerRunner(
                task=task, label="预约关系信息(refresh)", fetch=_fetch_reserve_info
            )
        case BackfillTask.LOTTERY_DYN:
            return CrawlerRunner(
                task=task, label="抽奖 notice(type=2)", fetch=_build_lottery_fetch(2)
            )
        case BackfillTask.LOTTERY_RESERVE:
            return CrawlerRunner(
                task=task, label="抽奖 notice(type=10)", fetch=_build_lottery_fetch(10)
            )
    raise ValueError(f"未支持的补数任务：{task}")


# ---------------------------------------------------------------------------
# id 区间解析
# ---------------------------------------------------------------------------


def _fmt_dyn_ts(dynamic_id: int) -> str:
    """雪花动态 id → 可读时间（仅日志用）。"""
    try:
        return time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(dynamic_id_2_ts(dynamic_id))
        )
    except (TypeError, ValueError, OverflowError):
        return "<非法动态 id>"


def resolve_id_range(args: argparse.Namespace) -> tuple[int, int]:
    """按任务类型解析 id 区间（lottery_dyn 支持用秒级时间戳反算雪花 id）。"""
    match args.task:
        case BackfillTask.DYN_DETAIL:
            if args.start_rid is None or args.end_rid is None:
                raise SystemExit("--task dyn_detail 必须同时给 --start-rid 与 --end-rid")
            return args.start_rid, args.end_rid
        case BackfillTask.RESERVE_SCRAPY:
            if args.start_rid is None or args.end_rid is None:
                raise SystemExit(
                    "--task reserve_scrapy 必须同时给 --start-rid 与 --end-rid"
                )
            return args.start_rid, args.end_rid
        case BackfillTask.RESERVE_INFO:
            if args.start_sid is None or args.end_sid is None:
                raise SystemExit(
                    "--task reserve_info 必须同时给 --start-sid 与 --end-sid"
                )
            return args.start_sid, args.end_sid
        case _:
            # lottery_dyn / lottery_reserve：id 优先，其次用秒级时间戳反算
            if args.start_id is not None and args.end_id is not None:
                if args.task is BackfillTask.LOTTERY_DYN:
                    myfastapi_logger.info(
                        f"lottery_dyn id 区间对应时间：{_fmt_dyn_ts(args.start_id)}"
                        f" ~ {_fmt_dyn_ts(args.end_id)}"
                    )
                return args.start_id, args.end_id
            if args.start_ts is not None and args.end_ts is not None:
                if args.task is not BackfillTask.LOTTERY_DYN:
                    raise SystemExit(
                        f"--task {args.task.value} 只支持 --start-id / --end-id"
                        "（时间反算只对 lottery_dyn 有意义）"
                    )
                return (
                    ts_2_fake_dynamic_id(args.start_ts),
                    ts_2_fake_dynamic_id(args.end_ts),
                )
            raise SystemExit(
                f"--task {args.task.value} 需要 --start-id / --end-id"
                "（lottery_dyn 也可以用 --start-ts / --end-ts 反算）"
            )


# ---------------------------------------------------------------------------
# 执行
# ---------------------------------------------------------------------------


async def _run_one(
    runner: BackfillRunner,
    options: BackfillOptions,
    business_id: int,
    semaphore: asyncio.Semaphore,
    stats: BackfillStats,
) -> None:
    """跑一条并记账：成功 → done；失败 → pending（不动游标，不影响整批）。"""
    error = ""
    async with semaphore:
        try:
            async with asyncio.timeout(ITEM_TIMEOUT_SECONDS):
                status = await runner.run_one(business_id)
        except asyncio.TimeoutError:
            status = WorkerStatus.timeoutError
            error = f"补数超时（{ITEM_TIMEOUT_SECONDS}s）"
        except Exception as e:  # noqa: BLE001 单条失败不中断整批
            status = WorkerStatus.fail
            error = f"{type(e).__name__}: {e}"

    if status in (WorkerStatus.complete, WorkerStatus.nullData):
        stats.ok += 1
        mark = "OK" if status is WorkerStatus.complete else "NULL"
        myfastapi_logger.info(f"[{options.task.value}] {business_id} {mark}")
        if not options.dry_run:
            await crawl_failed_task_dao.mark_done(runner.task.value, business_id)
        return

    stats.failed += 1
    stats.failed_ids.append(business_id)
    myfastapi_logger.warning(
        f"[{options.task.value}] {business_id} 失败（{status.name}）：{error}"
    )
    if not options.dry_run:
        await crawl_failed_task_dao.mark_pending(
            runner.task.value, business_id, last_error=error or status.name
        )


async def _ensure_schema() -> None:
    """确保 ``crawl_failed_task`` 已建（走现有 alembic 机制，幂等）。

    容器停机状态下 ``python -m`` 不会触发 lifespan 的自动迁移，因此这里显式跑一次
    ``alembic -n dyndetail upgrade head``；已经升级过则是一次空操作。
    """
    cfg = AlembicConfig(str(_project_root / "alembic.ini"), ini_section="dyndetail")
    await asyncio.to_thread(command.upgrade, cfg, "head")
    myfastapi_logger.info("dyndetail alembic upgrade head 完成（crawl_failed_task 就绪）")


async def async_main(options: BackfillOptions) -> int:
    ids = options.ids
    myfastapi_logger.warning(
        f"开始补数：task={options.task.value} range={options.start_id}~{options.end_id} "
        f"共 {len(ids)} 条，concurrency={options.concurrency}"
        f"{'（dry-run：不发请求 / 不写库）' if options.dry_run else ''}"
    )
    if not ids:
        myfastapi_logger.warning("id 区间为空，退出")
        return 0
    if options.dry_run:
        myfastapi_logger.warning(
            f"dry-run：将处理 {len(ids)} 条（{ids[0]}~{ids[-1]}），"
            "不连接数据库、不发请求"
        )
        return 0

    await _ensure_schema()
    runner = build_runner(options.task)
    myfastapi_logger.warning(f"补数目标：{runner.label}")
    stats = BackfillStats(total=len(ids))
    semaphore = asyncio.Semaphore(options.concurrency)
    await asyncio.gather(
        *[
            _run_one(runner, options, business_id, semaphore, stats)
            for business_id in ids
        ]
    )

    myfastapi_logger.warning(
        f"补数结束：task={options.task.value} total={stats.total} ok={stats.ok} "
        f"failed={stats.failed}"
    )
    if stats.failed:
        myfastapi_logger.warning(
            f"失败 id（已落 crawl_failed_task pending，下一轮爬虫会重跑）：{stats.failed_ids}"
        )
        return 1
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="按 id 区间补数（复用线上爬虫的解析与写库，幂等）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--task",
        type=BackfillTask,
        choices=list(BackfillTask),
        required=True,
        help="补数任务类型",
    )
    parser.add_argument(
        "--start-rid", type=int, default=None, help="dyn_detail / reserve_scrapy 起始 rid"
    )
    parser.add_argument(
        "--end-rid", type=int, default=None, help="dyn_detail / reserve_scrapy 结束 rid（含）"
    )
    parser.add_argument("--start-sid", type=int, default=None, help="reserve_info 起始 sid")
    parser.add_argument("--end-sid", type=int, default=None, help="reserve_info 结束 sid（含）")
    parser.add_argument(
        "--start-id", type=int, default=None, help="lottery_dyn / lottery_reserve 起始 id"
    )
    parser.add_argument(
        "--end-id", type=int, default=None, help="lottery_dyn / lottery_reserve 结束 id（含）"
    )
    parser.add_argument(
        "--start-ts",
        type=int,
        default=None,
        help="lottery_dyn 起始秒级时间戳（与 --end-ts 一起用，反算雪花 id）",
    )
    parser.add_argument(
        "--end-ts", type=int, default=None, help="lottery_dyn 结束秒级时间戳"
    )
    parser.add_argument(
        "--concurrency", type=int, default=5, help="并发条数（默认 5，范围 1~50）"
    )
    parser.add_argument("--limit", type=int, default=0, help="最多处理多少条（0 = 不限制）")
    parser.add_argument(
        "--dry-run", action="store_true", help="只打印将处理的范围，不发请求、不写库"
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    start_id, end_id = resolve_id_range(args)
    options = BackfillOptions(
        task=args.task,
        start_id=start_id,
        end_id=end_id,
        concurrency=max(1, min(50, args.concurrency)),
        limit=max(0, args.limit),
        dry_run=args.dry_run,
    )
    try:
        return asyncio.run(async_main(options))
    except KeyboardInterrupt:
        myfastapi_logger.warning("补数被中断")
        return 130


if __name__ == "__main__":
    sys.exit(main())
