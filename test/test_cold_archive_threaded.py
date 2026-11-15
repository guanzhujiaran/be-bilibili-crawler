"""冷数据归档「同步 gzip IO 下沉线程池」回归测试。

线上事故（2026-11-15，blocked=238.97s）：每日 0 点的归档任务在每次重启时被
启动抖动提前触发，`_merge_into_existing_archive` 里 `iter_csv_gz_rows` 解压
历史大归档包占住了事件循环，uvicorn 端口 listen 但 HTTP 全部超时。

这里锁住三件事：
1. 所有 gzip 读 / 写 / 回读校验都不在事件循环线程里执行；
2. 归档期间事件循环仍能调度其它协程（模拟线程里慢解压）；
3. 按主键去重续写、回读校验、校验失败不删库的语义保持不变。
"""

import asyncio
import datetime
import threading
import time

import pytest

from Models.database_archive.cold_data_archive_models import ColdArchiveConf
from Service.DataArchive import cold_data_archiver, csv_gz_io
from Service.DataArchive.cold_data_archiver import ColdDataArchiver
from Service.DataArchive.csv_gz_io import (
    build_monthly_file_path,
    count_csv_gz_rows,
    iter_csv_gz_rows,
)

_JAN_10 = int(datetime.datetime(2025, 1, 10).timestamp())
_APR_15 = int(datetime.datetime(2025, 4, 15).timestamp())


class FakeTarget:
    """假表适配器：行的读取是 async generator，删除留痕便于断言。"""

    table_name = "fake_cold_table"
    db_alias = "fake_db"
    columns = ["id", "payload"]
    primary_key_name = "id"

    def __init__(self, rows: list[tuple[object, ...]]):
        self.rows = rows
        self.deleted_batches: list[list[object]] = []

    def get_default_keep_days(self) -> int:
        return 0

    async def get_earliest_ts(self) -> int:
        return _JAN_10

    async def iter_row_values(self, start_ts: int, end_ts: int):
        for row in self.rows:
            yield row
            await asyncio.sleep(0)  # 模拟 async DB 分页

    async def delete_by_primary_keys(self, keys) -> None:
        self.deleted_batches.append(list(keys))


def _build_archiver(root_dir: str, target: FakeTarget) -> ColdDataArchiver:
    conf = ColdArchiveConf(
        root_dir=root_dir,
        end_ts=_APR_15,
        tables=[target.table_name],
        keep_days_override=0,
        is_delete_after_archive=True,
        max_months_per_run=1,
    )
    archiver = ColdDataArchiver(conf)
    # 只替换表适配器，归档流程本身一行不改
    archiver.targets = [target]  # type: ignore[assignment]
    return archiver


def _archive_path(root_dir: str) -> str:
    return build_monthly_file_path(root_dir, FakeTarget.table_name, _JAN_10)


def _spy_on_gzip_io(monkeypatch) -> set[int]:
    """记录 gzip 文件打开发生在哪些线程；返回线程 ident 集合。"""
    thread_idents: set[int] = set()
    original_write = csv_gz_io.open_csv_gz_write
    original_read = csv_gz_io.open_csv_gz_read

    def spy_write(*args, **kwargs):
        thread_idents.add(threading.get_ident())
        return original_write(*args, **kwargs)

    def spy_read(*args, **kwargs):
        thread_idents.add(threading.get_ident())
        return original_read(*args, **kwargs)

    monkeypatch.setattr(csv_gz_io, "open_csv_gz_write", spy_write)
    monkeypatch.setattr(csv_gz_io, "open_csv_gz_read", spy_read)
    return thread_idents


@pytest.mark.asyncio
async def test_gzip_io_never_runs_on_event_loop_thread(tmp_path, monkeypatch):
    """写 gzip、回读校验的 gzip 都必须在线程池里打开。"""
    thread_idents = _spy_on_gzip_io(monkeypatch)
    loop_thread_ident = threading.get_ident()
    target = FakeTarget([(1, "a"), (2, "b"), (3, "c")])
    archiver = _build_archiver(str(tmp_path), target)

    result = await archiver.archive_all()
    # 立刻取快照：下面用例自己也会读 gzip 断言行数，别把测试线程的 ident 混进来
    idents_during_archive = set(thread_idents)

    file_path = _archive_path(str(tmp_path))
    assert count_csv_gz_rows(file_path) == 3
    assert result[0].files[0].is_verified is True
    assert result[0].files[0].is_deleted is True
    assert target.deleted_batches == [[1, 2, 3]]
    # 至少一次写、一次读（回读校验），且都不是事件循环线程
    assert len(idents_during_archive) >= 1
    assert loop_thread_ident not in idents_during_archive


@pytest.mark.asyncio
async def test_event_loop_stays_responsive_during_slow_gzip_read(
    tmp_path, monkeypatch
):
    """模拟线程里 0.3s 的大包解压：期间事件循环必须继续调度其它协程。"""
    original_read = csv_gz_io.open_csv_gz_read

    def slow_read(*args, **kwargs):
        time.sleep(0.3)  # 只发生在线程池，不能卡住 loop
        return original_read(*args, **kwargs)

    monkeypatch.setattr(csv_gz_io, "open_csv_gz_read", slow_read)
    archiver = _build_archiver(str(tmp_path), FakeTarget([(1, "a"), (2, "b")]))

    ticks = 0

    async def heartbeat() -> None:
        nonlocal ticks
        while True:
            await asyncio.sleep(0.01)
            ticks += 1

    heartbeat_task = asyncio.create_task(heartbeat())
    try:
        await archiver.archive_all()
    finally:
        heartbeat_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await heartbeat_task

    assert ticks >= 5, "事件循环被同步 gzip 读占住了"


@pytest.mark.asyncio
async def test_merge_keeps_primary_key_dedup_and_no_overwrite(tmp_path):
    """断点续跑：旧归档里的主键不能重复写入，也不能被覆盖成只有本次数据。"""
    root = str(tmp_path)
    first = FakeTarget([(1, "a"), (2, "b")])
    result = await _build_archiver(root, first).archive_all()
    assert result[0].files[0].file_row_count == 2
    assert count_csv_gz_rows(_archive_path(root)) == 2

    # 模拟「上次归档写完但删库中断」：库里还有 id=2，另外多了 id=3
    second = FakeTarget([(2, "b"), (3, "c")])
    result2 = await _build_archiver(root, second).archive_all()

    rows = list(iter_csv_gz_rows(_archive_path(root)))
    keys = [row[0] for row in rows]
    assert keys == ["1", "2", "3"], "按主键去重的续写语义变了"
    assert result2[0].files[0].file_row_count == 3
    assert result2[0].files[0].is_verified is True
    assert result2[0].files[0].is_deleted is True
    assert second.deleted_batches == [[2, 3]]


@pytest.mark.asyncio
async def test_verify_failure_never_deletes_rows(tmp_path, monkeypatch):
    """回读校验失败时保留归档文件、绝不删库。"""
    async def fake_verify(file_path: str, expected_rows: int) -> tuple[bool, str]:
        return False, "模拟回读校验失败"

    monkeypatch.setattr(cold_data_archiver, "verify_csv_gz_async", fake_verify)
    target = FakeTarget([(1, "a"), (2, "b")])
    result = await _build_archiver(str(tmp_path), target).archive_all()

    assert target.deleted_batches == []
    assert count_csv_gz_rows(_archive_path(str(tmp_path))) == 2
    assert result[0].files[0].is_verified is False
    assert result[0].files[0].is_deleted is False
