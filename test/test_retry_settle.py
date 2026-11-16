# -*- coding: utf-8 -*-
"""``_settle_retry_task`` 的重试队列口径单测（不连库、不跑爬虫循环）。

关键口径（2026-11-16 线上问题后确定）：
- **失败即落库**：不管本轮是否立刻重入队，先 upsert ``crawl_failed_task.pending``，
  这样进程被杀 / 杀旧开新也不会丢；写库不是重试通道。
- **重试在本轮**：``should_requeue=True`` 时由 ``_handle_requeue`` 继续重试，
  落库与重入队互不冲突。
- 成功（含 nullData）→ ``done``，游标才能推进。
"""

import asyncio
import logging

import pytest

from Service.BaseCrawler.CrawlerType import UnlimitedCrawler
from Service.BaseCrawler.model.base import WorkerModel, WorkerStatus
from Service.BaseCrawler.retry_queue import RetryTaskType
from Service.opus新版官方抽奖.活动抽奖.话题抽奖.robot import TopicParams


class _Host:
    """最小宿主：只提供 ``_settle_retry_task`` 需要的属性 / 方法。"""

    retry_task_type = RetryTaskType.TOPIC.value
    crawler_name = "TEST_HOST"
    log = logging.getLogger("test_retry_settle")

    def __init__(self, dao, final_fail_calls: list[int]):
        self._dao = dao
        self._final_fail_calls = final_fail_calls
        self.requeued: list[int] = []
        self.task_queue = asyncio.Queue()
        self._pending_requeues: set = set()

    @property
    def _retry_dao(self):
        return self._dao

    def format_log(self, msg: str) -> str:
        return msg

    def retry_business_id(self, params) -> int | None:
        return params.topic_id if params is not None else None

    async def _upsert_retry_task(self, worker_model) -> bool:
        # 复用基类实现（用本宿主的 _retry_dao / retry_business_id）
        return await UnlimitedCrawler._upsert_retry_task(self, worker_model)

    # 直接复用基类的静态方法（插队首）
    _put_front = staticmethod(UnlimitedCrawler._put_front)

    async def on_task_requeue(self, worker_model) -> None:
        self.requeued.append(worker_model.params.topic_id)

    async def on_task_final_fail(self, worker_model) -> None:
        self._final_fail_calls.append(worker_model.params.topic_id)

    def _worker(self, status: WorkerStatus, topic_id: int = 42) -> WorkerModel:
        model = WorkerModel(params=TopicParams(topic_id=topic_id), seqId=0)
        model.fetchStatus = status
        return model


class _Dao:
    def __init__(self):
        self.pending: list[tuple[int, str]] = []
        self.done: list[int] = []

    async def mark_pending(self, task_type, business_id, last_error="", next_run_at=None):
        self.pending.append((business_id, last_error))

    async def mark_done(self, task_type, business_id):
        self.done.append(business_id)
        return True


@pytest.fixture
def host():
    final_fail_calls: list[int] = []
    dao = _Dao()
    return _Host(dao, final_fail_calls), dao, final_fail_calls


async def test_success_marks_done(host):
    h, dao, final_fail_calls = host

    await UnlimitedCrawler._settle_retry_task(
        h, h._worker(WorkerStatus.complete), should_requeue=False
    )

    assert dao.done == [42]
    assert dao.pending == []
    assert final_fail_calls == []


async def test_null_data_also_marks_done(host):
    """nullData 是终态（动态已删 / 话题不存在），也要摘掉 pending。"""
    h, dao, _ = host

    await UnlimitedCrawler._settle_retry_task(
        h, h._worker(WorkerStatus.nullData), should_requeue=False
    )

    assert dao.done == [42]
    assert dao.pending == []


async def test_failure_is_persisted_even_when_requeued_this_round(host):
    """失败即落库：本轮会重入队，也同样先写 pending（进程被杀也不丢）。"""
    h, dao, final_fail_calls = host

    await UnlimitedCrawler._settle_retry_task(
        h, h._worker(WorkerStatus.fail), should_requeue=True
    )

    assert [bid for bid, _ in dao.pending] == [42]
    assert dao.done == []
    # 本轮还会重试，不算「最终失败」钩子
    assert final_fail_calls == []


async def test_failure_after_round_budget_calls_final_fail_hook(host):
    """本轮重试用尽：仍然落库，并触发最终失败钩子（留给下一轮补跑）。"""
    h, dao, final_fail_calls = host

    await UnlimitedCrawler._settle_retry_task(
        h, h._worker(WorkerStatus.timeoutError), should_requeue=False
    )

    assert [bid for bid, _ in dao.pending] == [42]
    assert final_fail_calls == [42]


async def test_put_front_makes_retry_jump_the_queue():
    """重试任务必须插到**队首**：队列有积压时放队尾等于「拖到最后」。

    dyn_detail 的生产者不限速，队列可能有几千条待执行；而业务数据有连贯性
    （前面的 rid 缺失会影响后面的结束判断），所以失败任务要抢在新任务之前。
    """
    queue: asyncio.Queue[str | None] = asyncio.Queue()
    await queue.put("new-1")
    await queue.put("new-2")

    UnlimitedCrawler._put_front(queue, "retry")

    assert await queue.get() == "retry"
    assert await queue.get() == "new-1"
    assert queue.qsize() == 1
    # 未完成计数要跟着 +1，否则 run() 收尾的 join() 会提前返回
    assert queue._unfinished_tasks == 3
    for _ in range(3):
        queue.task_done()
    await asyncio.wait_for(queue.join(), timeout=1)


async def test_handle_requeue_puts_failed_task_at_the_front(host):
    """「至少一次」爬虫失败后：**立即插到第一个**，不经过任何退避计算。

    用户口径：失败就马上插回队列、放到第一个（数据有连贯性，前面的 id 缺失会
    影响后面的结束判断；队列积压时排队尾等于拖到最后）。
    """
    h, _, _ = host
    queue: asyncio.Queue = asyncio.Queue()
    await queue.put("pending-new-task-1")
    await queue.put("pending-new-task-2")
    model = h._worker(WorkerStatus.fail)

    await UnlimitedCrawler._handle_requeue(h, model, queue)

    # 队首就是刚失败的任务，且队列里没有挂起的「延迟重入队」task
    assert queue._queue[0] is model
    assert h.requeued == [42]
    assert model.retry_count == 1
    assert model.fetchStatus is WorkerStatus.pending
    assert h._pending_requeues == set()


async def test_db_error_does_not_break_worker(host):
    """记账失败只记日志，不能影响 worker 本轮重试。"""
    h, dao, _ = host

    async def _boom(*args, **kwargs):
        raise RuntimeError("mysql down")

    dao.mark_pending = _boom

    await UnlimitedCrawler._settle_retry_task(
        h, h._worker(WorkerStatus.fail), should_requeue=True
    )
