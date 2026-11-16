# -*- coding: utf-8 -*-
"""补数入口 ``scripts/backfill/backfill_missing.py`` 的单测（不连 MySQL / Redis / MQ）。

覆盖：
1. id 区间解析（dyn_detail / reserve_scrapy / reserve_info / lottery_*，含时间反算）；
2. ``--limit`` 截断与空区间；
3. 单条结果记账：成功 → ``mark_done``，失败 → ``mark_pending``（不中断整批）；
4. dry-run 不触发任何写库 / 请求。
"""

import argparse
import asyncio

import pytest

from Service.BaseCrawler.model.base import WorkerStatus
from Utils.通用.dynamic_id_caculate import ts_2_fake_dynamic_id
from scripts.backfill import backfill_missing as bf


def _ns(**kwargs) -> argparse.Namespace:
    """构造 argparse 结果（缺省全部为 None，与真实解析一致）。"""
    base = {
        "task": None,
        "start_rid": None,
        "end_rid": None,
        "start_sid": None,
        "end_sid": None,
        "start_id": None,
        "end_id": None,
        "start_ts": None,
        "end_ts": None,
        "concurrency": 5,
        "limit": 0,
        "dry_run": False,
    }
    base.update(kwargs)
    return argparse.Namespace(**base)


# ---------------------------------------------------------------------------
# id 区间
# ---------------------------------------------------------------------------


def test_resolve_dyn_detail_range():
    start, end = bf.resolve_id_range(
        _ns(task=bf.BackfillTask.DYN_DETAIL, start_rid=345841742, end_rid=345845731)
    )
    assert (start, end) == (345841742, 345845731)


def test_resolve_dyn_detail_requires_both_bounds():
    with pytest.raises(SystemExit):
        bf.resolve_id_range(_ns(task=bf.BackfillTask.DYN_DETAIL, start_rid=1))


def test_resolve_reserve_scrapy_range():
    start, end = bf.resolve_id_range(
        _ns(task=bf.BackfillTask.RESERVE_SCRAPY, start_rid=5000000, end_rid=5000100)
    )
    assert (start, end) == (5000000, 5000100)


def test_resolve_reserve_info_range():
    start, end = bf.resolve_id_range(
        _ns(task=bf.BackfillTask.RESERVE_INFO, start_sid=4234284, end_sid=4234290)
    )
    assert (start, end) == (4234284, 4234290)


def test_resolve_lottery_dyn_from_timestamps():
    """lottery_dyn 的 id 区间可以用秒级时间戳反算（雪花 id 前 32 位是时间戳）。"""
    start_ts, end_ts = 1763225640, 1763300000
    start, end = bf.resolve_id_range(
        _ns(task=bf.BackfillTask.LOTTERY_DYN, start_ts=start_ts, end_ts=end_ts)
    )
    assert start == ts_2_fake_dynamic_id(start_ts)
    assert end == ts_2_fake_dynamic_id(end_ts)
    assert start < end


def test_resolve_lottery_reserve_rejects_timestamps():
    with pytest.raises(SystemExit):
        bf.resolve_id_range(
            _ns(
                task=bf.BackfillTask.LOTTERY_RESERVE,
                start_ts=1763225640,
                end_ts=1763300000,
            )
        )


# ---------------------------------------------------------------------------
# 区间 / limit
# ---------------------------------------------------------------------------


def test_options_ids_are_inclusive_and_limited():
    options = bf.BackfillOptions(
        task=bf.BackfillTask.DYN_DETAIL, start_id=100, end_id=110, limit=3
    )
    assert options.ids == [100, 101, 102]


def test_options_ids_empty_when_end_before_start():
    options = bf.BackfillOptions(
        task=bf.BackfillTask.DYN_DETAIL, start_id=110, end_id=100
    )
    assert options.ids == []


# ---------------------------------------------------------------------------
# 单条结果记账
# ---------------------------------------------------------------------------


class _FakeRunner:
    task = bf.BackfillTask.DYN_DETAIL
    label = "fake"

    def __init__(self, result: WorkerStatus | Exception):
        self._result = result

    async def run_one(self, business_id: int) -> WorkerStatus:
        if isinstance(self._result, Exception):
            raise self._result
        return self._result


def _stub_dao(monkeypatch) -> tuple[list[int], list[tuple[int, str]]]:
    done: list[int] = []
    pending: list[tuple[int, str]] = []

    async def _mark_done(task_type, business_id):
        done.append(business_id)
        return True

    async def _mark_pending(task_type, business_id, last_error="", next_run_at=None):
        pending.append((business_id, last_error))

    monkeypatch.setattr(bf.crawl_failed_task_dao, "mark_done", _mark_done, raising=False)
    monkeypatch.setattr(
        bf.crawl_failed_task_dao, "mark_pending", _mark_pending, raising=False
    )
    return done, pending


async def test_run_one_marks_done_on_success(monkeypatch):
    done, pending = _stub_dao(monkeypatch)
    options = bf.BackfillOptions(
        task=bf.BackfillTask.DYN_DETAIL, start_id=1, end_id=1
    )
    stats = bf.BackfillStats(total=1)

    await bf._run_one(
        _FakeRunner(WorkerStatus.complete), options, 345841742, asyncio.Semaphore(1), stats
    )

    assert stats.ok == 1
    assert stats.failed == 0
    assert done == [345841742]
    assert pending == []


async def test_run_one_marks_pending_on_exception(monkeypatch):
    done, pending = _stub_dao(monkeypatch)
    options = bf.BackfillOptions(
        task=bf.BackfillTask.DYN_DETAIL, start_id=1, end_id=1
    )
    stats = bf.BackfillStats(total=1)

    await bf._run_one(
        _FakeRunner(RuntimeError("proxy boom")),
        options,
        345841745,
        asyncio.Semaphore(1),
        stats,
    )

    assert stats.failed == 1
    assert stats.failed_ids == [345841745]
    assert done == []
    assert pending and pending[0][0] == 345841745
    assert "proxy boom" in pending[0][1]


async def test_run_one_dry_run_never_writes(monkeypatch):
    done, pending = _stub_dao(monkeypatch)
    options = bf.BackfillOptions(
        task=bf.BackfillTask.DYN_DETAIL, start_id=1, end_id=1, dry_run=True
    )
    stats = bf.BackfillStats(total=1)

    await bf._run_one(
        _FakeRunner(WorkerStatus.fail), options, 345841746, asyncio.Semaphore(1), stats
    )

    assert stats.failed == 1
    assert done == [] and pending == []
