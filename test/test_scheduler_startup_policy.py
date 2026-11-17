"""任务首跑策略回归测试。

三态必须同时成立：
- 高频任务（*/10 * * * *、0 */5 * * *）：默认启动抖动；
- 日级数据爬取（0 2 * * * 之类）：在 ``CrawlerScheduler`` 里显式
  ``run_on_startup=True``，重启后仍在抖动窗内补跑一次，但必须继续走
  ``run() → is_need_to_execute()`` 节流，不能绕过判断直接抓取；
- 重活（DYN_DETAIL_DATABASE_CLEANER 0 0 * * *、TABLESPACE_MAINTENANCE
  30 4 1 * *）：``run_on_startup=False``，首跑取 cron 下次触发时间。

背景：2026-11-15 先用「cron 频率自动判定」修掉了归档造成的启动 240s 卡死，
但副作用是所有日级爬虫都被推到次日 cron，数据新鲜度最坏延迟近 24h；
本轮把「重活延后」与「日级补跑」拆开。
"""

import asyncio
import datetime
from types import SimpleNamespace

import pytest
from apscheduler.triggers.cron import CronTrigger
from loguru import logger as default_logger

import Service.BaseCrawler.launcher.scheduler_launcher as scheduler_launcher
from Service.BaseCrawler.launcher.scheduler_launcher import (
    POLICY_CRON_ONLY,
    POLICY_HIGH_FREQ_JITTER,
    POLICY_STARTUP_CATCHUP,
    BaseScheduler,
    CrawlerExecutionInfo,
    GenericCrawlerScheduler,
    _first_run_time,
    _should_run_on_startup_by_cron,
)
from Utils.通用.Common import GLOBAL_SCHEDULER
from log.base_log import myfastapi_logger


@pytest.mark.parametrize(
    "cron_expr, expected",
    [
        ("*/10 * * * *", True),
        ("0 */5 * * *", True),
        ("0 * * * *", True),
        ("0 0 * * *", False),  # 每日 0 点：冷数据归档
        ("30 4 1 * *", False),  # 每月 1 号 04:30：表空间维护
        ("0 2 * * *", False),  # 每日 2 点：日级爬虫（默认安全值 = 不补跑）
    ],
)
def test_cron_default_startup_policy(cron_expr, expected):
    assert _should_run_on_startup_by_cron(cron_expr) is expected


def _trigger(expr: str) -> CronTrigger:
    minute, hour, day, month, day_of_week = expr.split()
    return CronTrigger(
        minute=minute, hour=hour, day=day, month=month, day_of_week=day_of_week
    )


@pytest.mark.parametrize(
    "cron_expr",
    ["0 1 * * *", "0 2 * * *", "0 3 * * *", "0 7 * * *", "0 8 * * *"],
)
def test_daily_crawler_explicit_true_first_run_is_in_jitter_window(cron_expr):
    """日级爬虫显式 True：首跑仍在 now + startup_jitter 抖动窗内，不是次日 cron。"""
    before = datetime.datetime.now()
    actual = _first_run_time(_trigger(cron_expr), run_on_startup=True)
    assert before <= actual <= datetime.datetime.now() + datetime.timedelta(seconds=61)


@pytest.mark.parametrize("cron_expr", ["0 0 * * *", "30 4 1 * *"])
def test_maintenance_jobs_first_run_is_cron_next_fire_time(cron_expr):
    """归档 / 表空间维护：首跑必须是 cron 的下一次触发时间。"""
    trigger = _trigger(cron_expr)
    now = datetime.datetime.now(trigger.timezone)
    expected = trigger.get_next_fire_time(None, now)
    assert _first_run_time(trigger, run_on_startup=False) == expected
    assert expected > now + datetime.timedelta(minutes=1)


def test_high_frequency_first_run_unchanged():
    trigger = _trigger("*/10 * * * *")
    before = datetime.datetime.now()
    actual = _first_run_time(trigger, run_on_startup=True)
    assert before <= actual <= datetime.datetime.now() + datetime.timedelta(seconds=61)


async def _noop() -> None:
    return None


class _FakeCrawler:
    """只提供 GenericCrawlerScheduler 需要的属性，不触发任何业务导入。"""

    crawler_name = ""
    log = default_logger

    async def main(self) -> None:
        return None


def _remove_job(job_id: str) -> None:
    if GLOBAL_SCHEDULER.get_job(job_id):
        GLOBAL_SCHEDULER.remove_job(job_id)


@pytest.mark.asyncio
async def test_three_startup_policies_are_distinguishable_in_warning_log():
    """三类策略的 WARNING 文案能一眼区分，且都落 myfastapi_logger（业务日志）。"""
    lines: list[str] = []
    sink_id = myfastapi_logger.add(
        lambda message: lines.append(str(message)),
        level="WARNING",
        filter=lambda record: "event=crawler_job_added" in record["message"],
    )
    job_ids: list[str] = []
    try:
        daily = BaseScheduler(
            func=_noop,
            cron_expr="0 2 * * *",
            crawler_name="test_daily_catchup",
            run_on_startup=True,
        )
        cleaner = BaseScheduler(
            func=_noop,
            cron_expr="0 0 * * *",
            crawler_name="test_cleaner_cron_only",
            run_on_startup=False,
        )
        fast = BaseScheduler(
            func=_noop,
            cron_expr="*/10 * * * *",
            crawler_name="test_fast_jitter",
        )
        generic = GenericCrawlerScheduler(
            crawler=_FakeCrawler(),
            cron_expr="0 5 * * *",
            crawler_name="test_generic_catchup",
            run_on_startup=True,
        )
        job_ids = [
            s.job_id for s in (daily, cleaner, fast, generic)
        ]
        assert daily.startup_policy == POLICY_STARTUP_CATCHUP
        assert cleaner.startup_policy == POLICY_CRON_ONLY
        assert fast.startup_policy == POLICY_HIGH_FREQ_JITTER
        assert generic.startup_policy == POLICY_STARTUP_CATCHUP
        assert generic.run_on_startup is True

        log_text = "\n".join(lines)
        assert f"policy={POLICY_STARTUP_CATCHUP}" in log_text
        assert f"policy={POLICY_CRON_ONLY}" in log_text
        assert f"policy={POLICY_HIGH_FREQ_JITTER}" in log_text
        assert "日级爬虫启动补跑" in log_text
        assert "等 cron 下次触发" in log_text
        assert "高频任务启动抖动" in log_text
    finally:
        myfastapi_logger.remove(sink_id)
        for job_id in job_ids:
            _remove_job(job_id)


@pytest.mark.asyncio
async def test_startup_catchup_still_goes_through_is_need_to_execute():
    """补跑不等于立即抓取：仍由 run() 问 is_need_to_execute()，不满足就跳过。"""
    calls = 0

    async def func() -> None:
        nonlocal calls
        calls += 1

    class StubExecInfo:
        def __init__(self, allowed: bool):
            self.allowed = allowed
            self.saved = 0

        info = SimpleNamespace(
            crawler_name="test_throttle",
            default_interval_seconds=15 * 3600,
            last_attempt_time=None,
        )

        async def is_need_to_execute(self) -> bool:
            return self.allowed

        async def save_last_exec_time(self) -> None:
            self.saved += 1

    async def fake_push(crawler_name, stage, content) -> bool:
        return False

    scheduler = BaseScheduler(
        func=func,
        cron_expr="0 2 * * *",
        crawler_name="test_throttle",
        run_on_startup=True,
    )
    scheduler._push_stage = fake_push
    skipped_lines: list[str] = []
    sink_id = myfastapi_logger.add(
        lambda message: skipped_lines.append(str(message)),
        level="WARNING",
        filter=lambda record: "event=crawler_startup_catchup_skipped"
        in record["message"],
    )
    try:
        # 刚抓过：节流判定不通过，不能创建抓取任务；首次跳过要留 WARNING 痕迹
        recent = StubExecInfo(False)
        scheduler.exec_info = recent
        await scheduler.run()
        await asyncio.sleep(0)
        assert calls == 0
        assert scheduler.crawler_asyncio_task is None
        assert len(skipped_lines) == 1
        assert "is_need_to_execute 节流跳过" in skipped_lines[0]

        # 之后同一任务再被触发而跳过，不该刷 WARNING
        scheduler.exec_info = StubExecInfo(False)
        await scheduler.run()
        assert len(skipped_lines) == 1

        # 距上次执行足够久：正常补跑一次，并保存执行时间
        stale = StubExecInfo(True)
        scheduler.exec_info = stale
        await scheduler.run()
        assert scheduler.crawler_asyncio_task is not None
        await scheduler.crawler_asyncio_task
        assert calls == 1
        assert stale.saved == 1
    finally:
        myfastapi_logger.remove(sink_id)
        _remove_job(scheduler.job_id)


class _FakeRedis:
    """只实现节流键读写，避免单测依赖真实 Redis。"""

    def __init__(self):
        self.store: dict[str, str] = {}

    async def get_val(self, key_name: str):
        return self.store.get(key_name)

    async def set_val(self, key_name: str, val: str):
        self.store[key_name] = val

    async def del_val(self, key_name: str):
        self.store.pop(key_name, None)


@pytest.fixture
def fake_redis(monkeypatch):
    fake = _FakeRedis()
    monkeypatch.setattr(scheduler_launcher, "comm_storage_redis_obj", fake)
    return fake


async def _false_push(crawler_name, stage, content) -> bool:
    """推送一律「成功」，避免失败分支再走 a_push_error 真实推送链路。"""
    return True


@pytest.mark.asyncio
async def test_cancel_rolls_back_attempt_so_restart_can_catch_up(fake_redis):
    """跑到一半被 cancel：回滚 attempt，重启后的启动补跑仍能按上一轮结束时间补跑。"""
    crawler_name = "test_cancel_rollback"
    now = datetime.datetime.now()
    prev_attempt = now - datetime.timedelta(hours=20)
    last_exec = now - datetime.timedelta(hours=16)

    exec_info = CrawlerExecutionInfo(
        crawler_name=crawler_name, default_interval_seconds=15 * 3600
    )
    fake_redis.store[exec_info._exec_info_redis_key] = str(last_exec.timestamp())
    fake_redis.store[exec_info._attempt_info_redis_key] = str(
        prev_attempt.timestamp()
    )

    started = asyncio.Event()

    async def slow_func() -> None:
        started.set()
        await asyncio.sleep(3600)

    scheduler = BaseScheduler(
        func=slow_func,
        cron_expr="0 2 * * *",
        default_interval_seconds=15 * 3600,
        crawler_name=crawler_name,
        run_on_startup=True,
    )
    scheduler._push_stage = _false_push
    try:
        await scheduler.run()
        task = scheduler.crawler_asyncio_task
        assert task is not None
        await asyncio.wait_for(started.wait(), timeout=2)
        # 开跑后 attempt 已推进到当前时刻
        assert abs(
            float(fake_redis.store[exec_info._attempt_info_redis_key])
            - now.timestamp()
        ) < 10

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # 被 cancel 的一轮必须把 attempt 回滚到开跑前，而不是停在开跑时刻
        assert abs(
            float(fake_redis.store[exec_info._attempt_info_redis_key])
            - prev_attempt.timestamp()
        ) < 1

        # 模拟进程重启：新的 CrawlerExecutionInfo 从 Redis 重新加载后允许补跑
        restarted = CrawlerExecutionInfo(
            crawler_name=crawler_name, default_interval_seconds=15 * 3600
        )
        assert await restarted.is_need_to_execute() is True
    finally:
        _remove_job(scheduler.job_id)


@pytest.mark.asyncio
async def test_failed_round_still_advances_attempt_and_is_throttled(fake_redis):
    """执行失败不写 last_exec_time，但仍推进 attempt，重启后 15h 内不得立刻重跑。"""
    crawler_name = "test_failure_throttle"
    now = datetime.datetime.now()
    last_exec = now - datetime.timedelta(days=2)

    exec_info = CrawlerExecutionInfo(
        crawler_name=crawler_name, default_interval_seconds=15 * 3600
    )
    fake_redis.store[exec_info._exec_info_redis_key] = str(last_exec.timestamp())

    async def failing_func() -> None:
        raise RuntimeError("boom")

    scheduler = BaseScheduler(
        func=failing_func,
        cron_expr="0 2 * * *",
        default_interval_seconds=15 * 3600,
        crawler_name=crawler_name,
        run_on_startup=True,
    )
    scheduler._push_stage = _false_push
    try:
        await scheduler.run()
        task = scheduler.crawler_asyncio_task
        assert task is not None
        await task  # 失败已被 _run_and_save_exec_time 内部消化，不会向上抛

        # 失败轮推进 attempt
        assert abs(
            float(fake_redis.store[exec_info._attempt_info_redis_key])
            - now.timestamp()
        ) < 10
        # 失败轮不推进 last_exec_time（键值原样保留）
        assert (
            fake_redis.store[exec_info._exec_info_redis_key]
            == str(last_exec.timestamp())
        )

        # 模拟进程重启：失败轮的 attempt 仍在节流窗口内
        restarted = CrawlerExecutionInfo(
            crawler_name=crawler_name, default_interval_seconds=15 * 3600
        )
        assert await restarted.is_need_to_execute() is False
    finally:
        _remove_job(scheduler.job_id)


@pytest.mark.asyncio
async def test_redis_key_deletion_is_visible_without_restart(fake_redis):
    """运维用 redis-cli 删掉节流键后，运行中的实例下一次判定就能感知。"""
    crawler_name = "test_redis_delete"
    now = datetime.datetime.now()
    exec_info = CrawlerExecutionInfo(
        crawler_name=crawler_name, default_interval_seconds=15 * 3600
    )
    fake_redis.store[exec_info._exec_info_redis_key] = str(now.timestamp())
    fake_redis.store[exec_info._attempt_info_redis_key] = str(now.timestamp())

    assert await exec_info.is_need_to_execute() is False

    # 等价于 redis-cli DEL 两个节流键；同一实例不得继续使用内存缓存
    fake_redis.store.pop(exec_info._exec_info_redis_key)
    fake_redis.store.pop(exec_info._attempt_info_redis_key)
    assert await exec_info.is_need_to_execute() is True
