"""事件循环卡顿取证自测（``Utils/FastAPI/loop_monitor.py``）。

核心诉求：**事件循环被同步调用占住时，必须还能拿到「占住它的那个调用栈」**。
所以这里的用例故意用 ``time.sleep``（同步阻塞，只在测试里出现）把事件循环卡住，
然后检查独立守护线程是否把「卡在 ``_blocking_call_site`` 这一帧」的栈打了出来。
"""

import asyncio
import contextlib
import gc
import re
import threading
import time

import pytest

from Utils.FastAPI.loop_monitor import EventLoopStallMonitor
from log.base_log import myfastapi_logger

#: 阻塞时长（秒）：必须明显大于 threshold，又要保证用例本身足够快
_BLOCK_SECONDS = 1.5


def _blocking_call_site() -> None:
    """模拟「同步阻塞事件循环」的业务代码（真实场景是同步 SQL / 大文件 read）。

    独立成模块级函数是为了让抓到的栈里能出现这个名字，用例才可断言。
    """
    time.sleep(_BLOCK_SECONDS)


def _blocking_call_site_with_gc() -> None:
    """阻塞期间强制做一次 gen2 回收，用于验证恢复日志里的 GC 增量。

    回收放在 sleep 之后：确保取证线程已经进入本轮卡顿、记录了起点统计。
    """
    time.sleep(_BLOCK_SECONDS)
    gc.collect(2)


class _LogCollector:
    """收集 ``myfastapi_logger`` 的 WARNING 记录（loguru sink 不阻塞事件循环）。"""

    def __init__(self) -> None:
        self.lines: list[str] = []
        self._sink_id = myfastapi_logger.add(
            self._write,
            level="WARNING",
            filter=lambda record: "event_loop" in record["message"],
        )

    def _write(self, message) -> None:
        self.lines.append(str(message).rstrip())

    @property
    def text(self) -> str:
        return "\n".join(self.lines)

    def close(self) -> None:
        myfastapi_logger.remove(self._sink_id)


async def _run_monitor_then_block(monitor: EventLoopStallMonitor) -> asyncio.Task:
    task = asyncio.create_task(monitor.run())
    # 先让心跳建立基线，否则第一次检查时心跳时间戳还是构造时的
    await asyncio.sleep(0.2)
    _blocking_call_site()
    # 恢复后留出足够时间给取证线程打「已恢复」
    await asyncio.sleep(0.6)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    return task


@pytest.mark.asyncio
async def test_watchdog_thread_captures_blocking_stack():
    """卡顿期间：独立线程能抓到事件循环线程的同步调用栈。"""
    collector = _LogCollector()
    monitor = EventLoopStallMonitor(
        threshold=0.3,
        interval=0.1,
        alert_interval=60.0,
        capture_stack=True,
    )
    try:
        await _run_monitor_then_block(monitor)
    finally:
        collector.close()

    log_text = collector.text
    # 1) 取证线程确实抓了栈，且栈指向测试里那个 time.sleep 调用点
    assert "event=event_loop_stall_stack" in log_text, log_text
    assert "in _blocking_call_site" in log_text, log_text
    assert "_blocking_call_site" in log_text, log_text
    # 2) 心跳侧的卡顿告警仍在（口径与历史一致）
    assert "event=event_loop_stall blocked=" in log_text, log_text
    # 3) 恢复后补一条恢复日志
    assert "event=event_loop_stall_recovered" in log_text, log_text
    # 4) 抓栈有计数
    assert monitor.stack_dump_count >= 1


@pytest.mark.asyncio
async def test_watchdog_thread_is_daemon_and_stops_on_cancel():
    """取证线程必须是 daemon，且 cancel 之后要真的退出（不能拖住进程退出）。"""
    monitor = EventLoopStallMonitor(threshold=0.2, interval=0.1, alert_interval=60.0)
    collector = _LogCollector()
    try:
        task = asyncio.create_task(monitor.run())
        await asyncio.sleep(0.1)
        assert monitor.is_watchdog_alive
        watchdog = monitor._watchdog
        assert watchdog is not None
        assert watchdog.daemon is True
        assert watchdog.name == "loop-stall-watchdog"
        # 只让事件循环线程停掉它
        assert watchdog is not threading.current_thread()

        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        # run() 的 finally 会 join，所以这里应当已经退出
        assert not monitor.is_watchdog_alive
    finally:
        collector.close()


@pytest.mark.asyncio
async def test_watchdog_is_restarted_when_it_dies():
    """取证线程静默死掉时必须被 loop 侧重新拉起（否则「卡顿无人取证」也是静默的）。"""
    monitor = EventLoopStallMonitor(threshold=0.2, interval=0.1)
    collector = _LogCollector()
    try:
        # 不调用 start()：模拟「线程已经不在」
        assert not monitor.is_watchdog_alive
        monitor._ensure_watchdog_alive()
        assert monitor.is_watchdog_alive
        assert "事件循环卡顿取证线程已意外退出" in collector.text
        # 主动 stop 之后不应该再被拉起（否则 shutdown 后线程会一直活着）
        monitor.stop()
        assert not monitor.is_watchdog_alive
        monitor._ensure_watchdog_alive()
        assert not monitor.is_watchdog_alive
    finally:
        monitor.stop()
        collector.close()


def test_monitor_reads_config_defaults():
    """默认阈值 / 采样 / 节流保持历史值（避免这次改动把告警变噪音或藏掉真卡顿）。"""
    from CONFIG import CONFIG

    cfg = CONFIG.loop_monitor
    assert cfg.enabled is True
    assert cfg.threshold == 1.0
    assert cfg.interval == 0.5
    assert cfg.alert_interval == 600.0
    assert cfg.capture_stack is True
    # 新增取证通道的默认值：100ms 连续采样、单轮最多 60 次、GC 观测默认开
    assert cfg.sample_interval == 0.1
    assert cfg.sample_max_samples == 60
    assert cfg.sample_top_frames == 3
    assert cfg.gc_observe is True
    assert cfg.gc_pause_threshold == 0.1
    assert cfg.gc_freeze_on_startup is True
    assert cfg.gc_gen2_threshold == 50


@pytest.mark.asyncio
async def test_sampled_stack_channel_aggregates_repeated_innermost_frame():
    """卡顿期间连续采样：恢复后把「最内层帧重复出现」的采样聚合成一条日志。"""
    collector = _LogCollector()
    monitor = EventLoopStallMonitor(
        threshold=0.3,
        interval=0.1,
        alert_interval=600.0,
        capture_stack=True,
        sample_interval=0.05,
        sample_max_samples=20,
        gc_observe=False,
    )
    try:
        await _run_monitor_then_block(monitor)
    finally:
        collector.close()

    log_text = collector.text
    assert "event=event_loop_stall_stack_sampled" in log_text, log_text
    assert "in _blocking_call_site" in log_text, log_text
    assert "samples=" in log_text, log_text
    # 采样次数足够，且同一条最内层帧被重复采到（[n/m] n >= 2）
    assert monitor.stack_sample_count >= 2, log_text
    assert re.search(r"\[(\d+)/\d+\] File .*in _blocking_call_site", log_text), log_text
    counts = [
        int(match)
        for match in re.findall(
            r"\[(\d+)/\d+\] File .*in _blocking_call_site", log_text
        )
    ]
    assert max(counts) >= 2, log_text


@pytest.mark.asyncio
async def test_stack_dump_happens_once_per_stall_round():
    """单点抓栈按「每轮 1 次」节流：alert_interval=0 也不能在持续卡顿时反复抓。"""
    collector = _LogCollector()
    monitor = EventLoopStallMonitor(
        threshold=0.3,
        interval=0.1,
        alert_interval=0.0,
        capture_stack=True,
        gc_observe=False,
    )
    try:
        await _run_monitor_then_block(monitor)
    finally:
        collector.close()

    # 注意采样日志的关键字是 ..._stack_sampled，不会命中下面这条精确前缀
    assert collector.text.count("event=event_loop_stall_stack blocked=") == 1, (
        collector.text
    )
    assert monitor.stack_dump_count == 1


@pytest.mark.asyncio
async def test_recovered_log_includes_gen2_collections_delta():
    """恢复日志要带本轮 gen2 collections 增量（区分「GC 导致」还是「业务同步阻塞」）。"""
    collector = _LogCollector()
    monitor = EventLoopStallMonitor(
        threshold=0.3,
        interval=0.1,
        alert_interval=0.0,
        capture_stack=False,
        gc_observe=True,
        gc_pause_threshold=1.0,
    )
    task = asyncio.create_task(monitor.run())
    try:
        await asyncio.sleep(0.2)
        _blocking_call_site_with_gc()
        await asyncio.sleep(0.6)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        collector.close()

    assert monitor.last_gc_gen2_delta >= 1
    assert "GC gen2 collections 增量" in collector.text, collector.text


@pytest.mark.asyncio
async def test_gc_pause_event_logged_and_callback_unregistered():
    """单次 GC 超过阈值记 event_loop_gc_pause；stop() 必须把全局回调摘掉。"""
    collector = _LogCollector()
    monitor = EventLoopStallMonitor(
        threshold=10.0,
        interval=0.1,
        capture_stack=False,
        gc_observe=True,
        gc_pause_threshold=0.0,
    )
    monitor.start()
    try:
        gc.collect(2)
        assert "event=event_loop_gc_pause" in collector.text, collector.text
        assert "generation=2" in collector.text, collector.text
        assert monitor.gc_pause_count >= 1
    finally:
        monitor.stop()
        collector.close()

    assert monitor._gc_callback not in gc.callbacks
