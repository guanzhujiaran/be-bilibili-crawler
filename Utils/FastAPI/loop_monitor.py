"""事件循环阻塞（慢回调）监控。

背景：线上连续 3 次重启都出现「``Application startup complete`` 之后 HTTP 约 4.5 分钟
无响应、CPU 100%」，但 uvicorn 端口已监听 —— 典型的**事件循环被同步/忙等代码饿死**。
事后只能靠日志时间戳猜，缺少直接的证据链。

做法（不打开 asyncio debug，避免全量回调开销）：

- 起一个 watchdog 协程，每 ``interval`` 秒 ``await asyncio.sleep(interval)`` 一次；
  实际睡眠时间比预期多出来的部分，就是事件循环被单个回调占住的时间；
- 超过 ``threshold`` 记 warning（带阻塞时长 + 是否发生推送），并**按 ``alert_interval``
  节流**推送一条告警，避免卡顿持续时刷屏；
- 同时把 ``loop.slow_callback_duration`` 设成同一阈值：需要更细的回调级堆栈时，
  打开 ``PYTHONASYNCIODEBUG=1`` 即可由 asyncio 自己记录慢回调（阈值与本模块一致）。
"""

import asyncio
import time
from typing import Awaitable, Callable

from log.base_log import myfastapi_logger
from Utils.推送.PushMe import a_push_error

#: 单次回调阻塞超过该秒数即视为「卡顿」（默认 1s：HTTP 正常 P99 远小于它）
DEFAULT_THRESHOLD_SECONDS = 1.0
#: watchdog 的采样间隔
DEFAULT_INTERVAL_SECONDS = 0.5
#: 卡顿告警的最小间隔（秒）：持续卡顿时只保留第一条与周期性的续报
DEFAULT_ALERT_INTERVAL_SECONDS = 600.0


class EventLoopStallMonitor:
    """周期性测量事件循环调度延迟；超阈值时记日志并（节流）推送告警。"""

    def __init__(
        self,
        *,
        threshold: float = DEFAULT_THRESHOLD_SECONDS,
        interval: float = DEFAULT_INTERVAL_SECONDS,
        alert_interval: float = DEFAULT_ALERT_INTERVAL_SECONDS,
        alerter: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        self.threshold = max(0.05, threshold)
        self.interval = max(0.05, interval)
        self.alert_interval = max(0.0, alert_interval)
        self._alerter = alerter
        self._last_alert_ts = 0.0
        #: 进程内累计的卡顿次数（供接口 / 测试观察）
        self.stall_count = 0

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        # 与 watchdog 阈值保持一致：开启 asyncio debug 时两边口径相同
        loop.slow_callback_duration = self.threshold
        myfastapi_logger.info(
            f"事件循环卡顿监控已启动：阈值 {self.threshold:.2f}s，"
            f"采样 {self.interval:.2f}s"
        )
        while True:
            started = loop.time()
            await asyncio.sleep(self.interval)
            delayed = loop.time() - started - self.interval
            if delayed <= self.threshold:
                continue
            self.stall_count += 1
            myfastapi_logger.warning(
                f"event=event_loop_stall blocked={delayed:.2f}s "
                f"threshold={self.threshold:.2f}s count={self.stall_count} "
                f"（事件循环在此次采样窗口内被占住，HTTP / MQ 处理都会顺延）"
            )
            await self._alert(delayed)

    async def _alert(self, delayed: float) -> None:
        if self._alerter is None:
            return
        now = time.monotonic()
        if now - self._last_alert_ts < self.alert_interval:
            return
        self._last_alert_ts = now
        try:
            await self._alerter(
                f"事件循环卡顿 {delayed:.1f}s（阈值 {self.threshold:.1f}s，"
                f"进程内累计 {self.stall_count} 次）"
            )
        except Exception as e:  # noqa: BLE001 —— 告警失败不能影响监控本身
            myfastapi_logger.warning(f"事件循环卡顿告警发送失败：{e!r}")


def start_event_loop_stall_monitor(
    *, threshold: float = DEFAULT_THRESHOLD_SECONDS
) -> asyncio.Task:
    """在当前事件循环里启动监控协程，返回 Task（由调用方负责在 shutdown 时 cancel）。"""

    async def _alert(message: str) -> None:
        await a_push_error(subject="事件循环卡顿", content=message)

    monitor = EventLoopStallMonitor(threshold=threshold, alerter=_alert)
    task = asyncio.get_running_loop().create_task(monitor.run())
    task.set_name("event-loop-stall-monitor")
    return task


__all__ = ["EventLoopStallMonitor", "start_event_loop_stall_monitor"]
