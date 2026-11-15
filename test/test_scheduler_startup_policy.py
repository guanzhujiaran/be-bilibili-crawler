"""长周期任务「不许启动即跑」的策略测试（2026-11-15 启动卡死 238.97s 的修复）。"""

import datetime

import pytest
from apscheduler.triggers.cron import CronTrigger

from Service.BaseCrawler.launcher.scheduler_launcher import (
    BaseScheduler,
    _first_run_time,
    _is_high_frequency_cron,
)
from Utils.通用.Common import GLOBAL_SCHEDULER


@pytest.mark.parametrize(
    "cron_expr, expected",
    [
        ("*/10 * * * *", True),
        ("0 */5 * * *", True),
        ("0 * * * *", True),
        ("0 0 * * *", False),  # 每日 0 点：冷数据归档
        ("30 4 1 * *", False),  # 每月 1 号 04:30：表空间维护
        ("0 2 * * *", False),  # 每日 2 点：日级爬虫
    ],
)
def test_cron_frequency_classification(cron_expr, expected):
    assert _is_high_frequency_cron(cron_expr) is expected


def _trigger(expr: str) -> CronTrigger:
    minute, hour, day, month, day_of_week = expr.split()
    return CronTrigger(
        minute=minute, hour=hour, day=day, month=month, day_of_week=day_of_week
    )


def test_long_period_first_run_is_cron_next_fire_time():
    trigger = _trigger("0 0 * * *")
    now = datetime.datetime.now(trigger.timezone)
    expected = trigger.get_next_fire_time(None, now)

    actual = _first_run_time(trigger, run_on_startup=False)

    assert actual == expected
    assert actual > now + datetime.timedelta(minutes=1)


def test_high_frequency_first_run_is_within_jitter_window():
    trigger = _trigger("*/10 * * * *")
    before = datetime.datetime.now()

    actual = _first_run_time(trigger, run_on_startup=True)

    assert before <= actual <= datetime.datetime.now() + datetime.timedelta(seconds=61)


async def _noop() -> None:
    return None


@pytest.mark.asyncio
async def test_base_scheduler_adds_long_period_job_without_immediate_run():
    """BaseScheduler 对日级任务自动用 cron 下次触发时间，高频任务保留抖动。"""
    names: list[str] = []
    try:
        for name, expr in [
            ("test_daily_cleaner", "0 0 * * *"),
            ("test_monthly_maintenance", "30 4 1 * *"),
            ("test_fast_checker", "*/10 * * * *"),
        ]:
            scheduler = BaseScheduler(
                func=_noop, cron_expr=expr, crawler_name=name
            )
            names.append(scheduler.job_id)
            job = GLOBAL_SCHEDULER.get_job(scheduler.job_id)
            assert job is not None
            if expr.startswith("*/"):
                assert scheduler.run_on_startup is True
                assert job.next_run_time <= datetime.datetime.now(
                    job.next_run_time.tzinfo
                ) + datetime.timedelta(seconds=61)
            else:
                assert scheduler.run_on_startup is False
                assert job.next_run_time > datetime.datetime.now(
                    job.next_run_time.tzinfo
                ) + datetime.timedelta(seconds=61)
    finally:
        for job_id in names:
            if GLOBAL_SCHEDULER.get_job(job_id):
                GLOBAL_SCHEDULER.remove_job(job_id)
