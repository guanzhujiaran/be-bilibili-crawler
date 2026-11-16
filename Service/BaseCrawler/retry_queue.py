# -*- coding: utf-8 -*-
"""爬虫「至少一次」重试队列（``dyndetail.crawl_failed_task``）。

背景（2026-11-16 代理池故障）：任务队列在内存里，进程重启 / 调度器「杀旧开新」/
代理池整体不可用 / 重入队次数到上限，任何一条都会让「还没写入成功」的业务 id 直接
消失；而各爬虫的断点游标只记「最后成功的 id」，后续成功会越过失败 id，形成永久缺口。

本模块只做**机制**（按 ``task_type`` 记账 + 取任务 + 推进判定），不含任何站点 / 业务
判断：
- 各爬虫用 ``CrawlerConfig`` / 类属性声明 ``retry_task_type`` 后，**任务一失败**就把
  ``(task_type, business_id)`` upsert 成 ``pending``（先落库，再决定本轮重入队）；
- **重试发生在当前这一轮**：失败后由 ``_handle_requeue`` 立刻延迟重入队继续试，
  写库只是「进程重启 / 杀旧开新 / 本轮用尽」时的持久化兜底，不是重试通道；
- 本轮重试用尽或进程重启后，下一轮 ``main()`` 取 ``pending & next_run_at<=now``
  接着跑，成功置 ``done``，失败 ``attempts+1`` + 退避
  （**永不因为 attempts 超限置 dead**，dead 只留给人工确认）；
- 游标只允许推进到「该 id 及之前没有 pending」的位置（见 ``min_pending_business_id``）。
"""

import time
from typing import Final

from bili_common.core import StrEnumAutoDoc
from pydantic import Field
from sqlalchemy import and_, func, select, update
from sqlalchemy.dialects.mysql import insert

from CONFIG import CONFIG
from Models.base.custom_pydantic import CustomBaseModel
from Service.GrpcModule.GrpcSrc.SQLObject.models import CrawlFailedTask
from Utils.通用.Common import log_sql_retry_wrapper
from dao.base.sqlHelperBase import SqlHelperBase

#: 重试退避：``300 × 2^attempts``，封顶 1 小时（与需求口径一致）
RETRY_BACKOFF_BASE_SECONDS: Final[int] = 300
RETRY_BACKOFF_MAX_SECONDS: Final[int] = 3600
#: 单轮最多取多少条 pending 任务（防止一次启动把队列灌爆）
RETRY_BATCH_LIMIT: Final[int] = 2000


class RetryTaskType(StrEnumAutoDoc):
    """重试队列的 ``task_type``（= 各爬虫声明的 ``retry_task_type``）。

    与 ``--task`` 参数同名，补数脚本与线上爬虫共用同一份字面量，避免两边写歪。
    """

    DYN_DETAIL = "dyn_detail"
    RESERVE_SCRAPY = "reserve_scrapy"
    RESERVE_INFO = "reserve_info"
    TOPIC = "topic"
    LOTTERY_DYN = "lottery_dyn"
    LOTTERY_RESERVE = "lottery_reserve"


class RetryTaskStatus(StrEnumAutoDoc):
    """``crawl_failed_task.status``：pending 待重试 / done 已成功 / dead 人工确认。"""

    PENDING = "pending"
    DONE = "done"
    DEAD = "dead"


class RetryTask(CustomBaseModel):
    """一条待重试任务（只带调度需要的字段）。"""

    id: int
    business_id: int
    attempts: int = 0


class RetryTaskStats(CustomBaseModel):
    """按状态的计数快照（用于日志，不做任何业务判断）。"""

    pending: int = 0
    done: int = 0
    dead: int = 0

    @property
    def total(self) -> int:
        return self.pending + self.done + self.dead

    def to_log_str(self) -> str:
        return (
            f"pending={self.pending} done={self.done} "
            f"dead={self.dead} total={self.total}"
        )


def retry_backoff_seconds(attempts: int) -> int:
    """第 ``attempts`` 次失败后的退避秒数：``min(300 × 2^attempts, 3600)``。"""
    exponent = max(0, min(int(attempts), 8))
    return min(RETRY_BACKOFF_BASE_SECONDS * (2**exponent), RETRY_BACKOFF_MAX_SECONDS)


class CrawlFailedTaskDao(SqlHelperBase):
    """``crawl_failed_task`` 的读写（全部幂等：重复 upsert / 重复置 done 都安全）。"""

    def __init__(self):
        super().__init__(CONFIG.database.MYSQL.dyn_detail_URI, is_crawler=True)

    @log_sql_retry_wrapper()
    async def mark_pending(
        self,
        task_type: str,
        business_id: int,
        last_error: str = "",
        next_run_at: int | None = None,
    ) -> None:
        """把任务置为「待重试」（不存在则插入）。

        - 新行：``attempts=0``、``next_run_at=now``（下一轮就跑）；
        - 已存在：只把 ``status`` 拉回 ``pending`` 并刷新 ``last_error`` / ``next_run_at``，
          **不动 attempts**（attempts 只由 ``record_retry_failure`` 递增，代表「真的重跑过
          又失败了」，这样退避曲线才对应真实的执行次数）。
        """
        now = int(time.time()) if next_run_at is None else int(next_run_at)
        error = (last_error or "")[:2000]
        stmt = insert(CrawlFailedTask).values(
            task_type=task_type,
            business_id=int(business_id),
            status=RetryTaskStatus.PENDING.value,
            attempts=0,
            last_error=error,
            next_run_at=now,
        )
        stmt = stmt.on_duplicate_key_update(
            status=RetryTaskStatus.PENDING.value,
            last_error=error,
            next_run_at=now,
            updated_at=func.now(),
        )
        async with self.async_session() as session:
            await session.execute(stmt)
            await session.commit()

    @log_sql_retry_wrapper()
    async def mark_done(self, task_type: str, business_id: int) -> bool:
        """标记成功；返回是否真的改动了行（没记录过该 id 时返回 False）。"""
        stmt = (
            update(CrawlFailedTask)
            .where(
                and_(
                    CrawlFailedTask.task_type == task_type,
                    CrawlFailedTask.business_id == int(business_id),
                    CrawlFailedTask.status != RetryTaskStatus.DONE.value,
                )
            )
            .values(
                status=RetryTaskStatus.DONE.value,
                last_error=None,
                updated_at=func.now(),
            )
        )
        async with self.async_session() as session:
            res = await session.execute(stmt)
            await session.commit()
        return bool(getattr(res, "rowcount", 0))

    @log_sql_retry_wrapper()
    async def record_retry_failure(
        self, task_type: str, business_id: int, last_error: str = ""
    ) -> None:
        """重跑又失败：``attempts+1`` 并按 ``min(300×2^attempts, 3600)`` 退避。

        永远不会把 ``status`` 改成 ``dead``：未写入成功的业务 id 必须一直被重试。
        """
        now = int(time.time())
        error = (last_error or "")[:2000]
        backoff = func.least(
            RETRY_BACKOFF_BASE_SECONDS
            * func.pow(2, func.least(CrawlFailedTask.attempts, 8)),
            RETRY_BACKOFF_MAX_SECONDS,
        )
        stmt = (
            update(CrawlFailedTask)
            .where(
                and_(
                    CrawlFailedTask.task_type == task_type,
                    CrawlFailedTask.business_id == int(business_id),
                )
            )
            .values(
                status=RetryTaskStatus.PENDING.value,
                attempts=CrawlFailedTask.attempts + 1,
                last_error=error,
                next_run_at=now + backoff,
                updated_at=func.now(),
            )
        )
        async with self.async_session() as session:
            res = await session.execute(stmt)
            if not getattr(res, "rowcount", 0):
                # 没有记录（正常任务第一次就失败）：补一条 pending，等价于「最终失败」口径
                await session.execute(
                    insert(CrawlFailedTask).values(
                        task_type=task_type,
                        business_id=int(business_id),
                        status=RetryTaskStatus.PENDING.value,
                        attempts=1,
                        last_error=error,
                        next_run_at=now + RETRY_BACKOFF_BASE_SECONDS,
                    )
                )
            await session.commit()

    @log_sql_retry_wrapper()
    async def fetch_pending(
        self, task_type: str, limit: int = RETRY_BATCH_LIMIT, now: int | None = None
    ) -> list[RetryTask]:
        """取 ``pending`` 且到了重试时间的任务（按 id 升序，老任务优先）。"""
        ts = int(time.time()) if now is None else int(now)
        stmt = (
            select(
                CrawlFailedTask.id,
                CrawlFailedTask.business_id,
                CrawlFailedTask.attempts,
            )
            .where(
                and_(
                    CrawlFailedTask.task_type == task_type,
                    CrawlFailedTask.status == RetryTaskStatus.PENDING.value,
                    CrawlFailedTask.next_run_at <= ts,
                )
            )
            .order_by(CrawlFailedTask.id)
            .limit(max(1, int(limit)))
        )
        async with self.async_session() as session:
            res = await session.execute(stmt)
            rows = res.all()
        return [
            RetryTask(id=int(row[0]), business_id=int(row[1]), attempts=int(row[2] or 0))
            for row in rows
        ]

    @log_sql_retry_wrapper()
    async def min_pending_business_id(self, task_type: str) -> int | None:
        """该爬虫**最小**的待重试业务 id；没有 pending 返回 ``None``。

        游标推进的唯一判据：只有「最小 pending」都比当前 id 大，才允许把游标推过去，
        否则失败 id 会被后续成功越过（2026-11-16 丢 8 条 rid 的直接原因）。
        """
        stmt = select(func.min(CrawlFailedTask.business_id)).where(
            and_(
                CrawlFailedTask.task_type == task_type,
                CrawlFailedTask.status == RetryTaskStatus.PENDING.value,
            )
        )
        async with self.async_session() as session:
            res = await session.execute(stmt)
            value = res.scalar()
        return int(value) if value is not None else None

    @log_sql_retry_wrapper()
    async def get_stats(self, task_type: str) -> RetryTaskStats:
        """按状态计数（只用于日志 / 观测）。"""
        stmt = (
            select(CrawlFailedTask.status, func.count(CrawlFailedTask.id))
            .where(CrawlFailedTask.task_type == task_type)
            .group_by(CrawlFailedTask.status)
        )
        stats = RetryTaskStats()
        async with self.async_session() as session:
            res = await session.execute(stmt)
            for status_value, count in res.all():
                match str(status_value):
                    case "pending":
                        stats.pending = int(count)
                    case "done":
                        stats.done = int(count)
                    case "dead":
                        stats.dead = int(count)
        return stats


crawl_failed_task_dao = CrawlFailedTaskDao()


class RetryTaskCursorKeeper(CustomBaseModel):
    """游标推进判定结果（纯数据，避免把裸 bool + 裸 int 混着传）。"""

    business_id: int
    pending_min: int | None = Field(
        default=None, description="该爬虫当前最小的待重试业务 id；None = 没有 pending"
    )

    @property
    def can_advance(self) -> bool:
        """该 id 及之前没有 pending 时才允许把游标推到这个 id。"""
        return self.pending_min is None or self.pending_min > self.business_id
