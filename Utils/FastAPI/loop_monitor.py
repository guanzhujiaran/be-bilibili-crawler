"""事件循环阻塞（慢回调）监控 + **卡顿现场取证**。

背景：线上连续多次重启都出现「``Application startup complete`` 之后 HTTP 约 4.5 分钟
无响应、CPU 100%」，但 uvicorn 端口已监听 —— 典型的**事件循环被同步 / 忙等代码饿死**。
一开始只能靠日志时间戳猜，后来加了 watchdog 协程，能报出「被卡了多久」，
却报不出「卡在哪个函数」：协程自己也被卡住，等它恢复时栈早已不在事故现场。

本模块用两条独立通道解决这个问题（不打开 ``PYTHONASYNCIODEBUG``，避免全量回调开销）：

1. **loop 侧心跳协程**（``run``）：每 ``interval`` 秒更新一次心跳（``time.monotonic()``
   + 递增计数，纯内存，无文件 IO）。它同时测量自己的 ``asyncio.sleep`` 延迟，
   超过 ``threshold`` 就记一条 ``event=event_loop_stall blocked=...``（口径与历史一致）。
2. **独立守护线程**（``_watch``）：按固定节奏检查心跳。心跳超过 ``threshold`` 没更新，
   说明事件循环**此刻**正被占住 —— 于是从线程里 ``sys._current_frames()`` 取
   「事件循环所在线程」的**同步调用栈**并 WARNING 落盘（``event=event_loop_stall_stack``）。
   线程不依赖事件循环，所以卡死期间依然能取证；写入走 loguru 的队列 sink，
   不在被卡住的线程里做 IO。
   每轮卡顿只抓 1 次栈（同一轮内不再重复），避免 10 分钟节流把中间轮次的证据吞掉。
   此外**卡顿期间按 ``sample_interval`` 连续采样**：uvloop 的 C 回调会让 ``f_back``
   链断裂、单点抓栈经常只有 1~3 帧；把多次采样的**最内层帧**聚合后
   （``event=event_loop_stall_stack_sampled``），重复出现的那一帧就是阻塞点。
   心跳恢复后再补一条 ``event=event_loop_stall_recovered blocked=...``，
   并带上本轮 gen2 GC collections 的增量。

3. **GC 观测**（``gc.callbacks``）：单次回收超过 ``gc_pause_threshold`` 记一条
   ``event=event_loop_gc_pause``（含世代、耗时、发生线程）。GC 是 stop-the-world 的，
   即使发生在工作线程也会暂停事件循环，是「卡顿但业务栈不明」时的常见元凶。

兜底：``faulthandler.dump_traceback_later(repeat=True)`` 默认关闭，置
``LOOP_MONITOR__FAULTHANDLER_ENABLED=true`` 可额外打开（它连 GIL 被 C 扩展占住的
情况也能尽力打印，但输出到 stderr 而非业务日志文件，故不作为默认手段）。

线程为 daemon，``stop()`` 通过 ``threading.Event`` 立即唤醒退出，
协程 ``run`` 的 ``finally`` 保证 cancel（shutdown）时一定停掉线程，不阻止进程退出。
loop 侧每 60s 巡检一次线程存活（``_ensure_watchdog_alive``），线程意外退出会被重新拉起 ——
否则「卡顿无人取证」这件事本身也是静默的。
"""

import asyncio
import faulthandler
import gc
import sys
import threading
import time
import traceback
from collections import Counter
from collections.abc import Awaitable, Callable

from CONFIG import CONFIG, LoopMonitorConfig
from log.base_log import myfastapi_logger
from Utils.推送.PushMe import a_push_error

#: 单次回调阻塞超过该秒数即视为「卡顿」（默认 1s：HTTP 正常 P99 远小于它）
DEFAULT_THRESHOLD_SECONDS = 1.0
#: 心跳采样间隔（loop 侧心跳协程的 sleep，也是线程侧检查节奏的上限）
DEFAULT_INTERVAL_SECONDS = 0.5
#: 卡顿告警（外部推送）的最小间隔（秒）：持续卡顿时只保留第一条与周期性续报。
#: **抓栈不再受它节流**：每轮卡顿固定抓 1 次，卡顿期间由连续采样通道补足细节。
DEFAULT_ALERT_INTERVAL_SECONDS = 600.0
#: 卡顿期间的连续采样间隔（秒）：uvloop C 回调导致单点抓栈帧数太少，靠多次采样聚合
DEFAULT_SAMPLE_INTERVAL_SECONDS = 0.1
#: 单轮卡顿最多保留的采样数（长时间卡死时防止无限占内存）
DEFAULT_SAMPLE_MAX_SAMPLES = 60
#: 采样聚合日志里最多列出的「最内层帧」条数
DEFAULT_SAMPLE_TOP_FRAMES = 3
#: 单次 GC 超过该秒数才记 event=event_loop_gc_pause
DEFAULT_GC_PAUSE_THRESHOLD_SECONDS = 0.1
#: 单条栈最多保留的帧数（只保留靠近阻塞点的尾部，避免一次刷出几百行）
DEFAULT_STACK_MAX_FRAMES = 40
#: 取证线程的存活自检间隔（秒）
#: 看门狗自己死掉时没人会发现（loop 侧只写心跳、线程侧只抓栈），
#: 所以由 loop 侧每 60s 巡检一次，发现线程没了就重新拉起并记一条 WARNING。
_WATCHDOG_LIVENESS_CHECK_SECONDS = 60.0


class EventLoopStallMonitor:
    """测量事件循环调度延迟；超阈值时**从独立线程**抓主线程同步栈并（节流）告警。"""

    def __init__(
        self,
        *,
        threshold: float = DEFAULT_THRESHOLD_SECONDS,
        interval: float = DEFAULT_INTERVAL_SECONDS,
        alert_interval: float = DEFAULT_ALERT_INTERVAL_SECONDS,
        alerter: Callable[[str], Awaitable[None]] | None = None,
        capture_stack: bool = True,
        stack_max_frames: int = DEFAULT_STACK_MAX_FRAMES,
        sample_interval: float = DEFAULT_SAMPLE_INTERVAL_SECONDS,
        sample_max_samples: int = DEFAULT_SAMPLE_MAX_SAMPLES,
        sample_top_frames: int = DEFAULT_SAMPLE_TOP_FRAMES,
        gc_observe: bool = True,
        gc_pause_threshold: float = DEFAULT_GC_PAUSE_THRESHOLD_SECONDS,
        faulthandler_enabled: bool = False,
        faulthandler_timeout: float = 30.0,
    ) -> None:
        self.threshold = max(0.05, threshold)
        self.interval = max(0.05, interval)
        self.alert_interval = max(0.0, alert_interval)
        self.capture_stack = capture_stack
        self.stack_max_frames = max(5, stack_max_frames)
        self.sample_interval = max(0.02, sample_interval)
        self.sample_max_samples = max(1, sample_max_samples)
        self.sample_top_frames = max(1, sample_top_frames)
        self.gc_observe = gc_observe
        self.gc_pause_threshold = max(0.0, gc_pause_threshold)
        self.faulthandler_enabled = faulthandler_enabled
        self.faulthandler_timeout = max(1.0, faulthandler_timeout)
        self._alerter = alerter
        self._last_alert_ts = 0.0
        #: 进程内累计的卡顿次数（供接口 / 测试观察）
        self.stall_count = 0
        #: 进程内累计的「抓栈」次数
        self.stack_dump_count = 0
        #: 进程内累计的「连续采样」次数
        self.stack_sample_count = 0
        #: 进程内累计的 >gc_pause_threshold 的 GC 次数
        self.gc_pause_count = 0
        #: 最近一轮卡顿期间的 gen2 collections 增量
        self.last_gc_gen2_delta = 0
        #: 最近一次观测到的 gen2 collections 累计值
        self.last_gc_gen2_total = 0

        # ---- loop 侧心跳（线程与协程共享，用锁保护一次读写）----
        self._hb_lock = threading.Lock()
        self._hb_monotonic = time.monotonic()
        self._hb_seq = 0

        # ---- 取证线程状态（只在 watchdog 线程里改，除 _loop_thread_ident 外）----
        self._stop_event = threading.Event()
        self._watchdog: threading.Thread | None = None
        self._loop_thread_ident: int | None = None
        self._loop_thread_name = ""
        self._stall_started_at: float | None = None
        #: 本轮是否已经抓过单点栈（每轮只抓 1 次，不再受 alert_interval 限制）
        self._dumped_current_round = False
        self._faulthandler_armed = False
        # 连续采样：只存最内层帧签名 + 每个签名的一份栈文本，避免 60 份完整栈占内存
        self._sample_signatures: list[str] = []
        self._sample_stacks: dict[str, str] = {}
        self._last_sample_ts = 0.0
        # GC 观测（gc.callbacks 可能从任意线程进入，用锁保护）
        self._gc_lock = threading.Lock()
        self._gc_registered = False
        self._gc_started: dict[int, float] = {}
        self._gc_stall_start_stats: list[dict] | None = None

    # ------------------------------------------------------------------ 生命周期
    def start(self) -> None:
        """启动取证线程（必须在事件循环线程里调用：靠 ``get_ident()`` 认准目标线程）。"""
        self._loop_thread_ident = threading.get_ident()
        self._loop_thread_name = threading.current_thread().name
        self._beat()
        if self.gc_observe:
            self._register_gc_callback()
        if self.faulthandler_enabled:
            try:
                # repeat=True：持续卡顿时每隔 timeout 秒再 dump 一次（输出到 stderr）
                faulthandler.dump_traceback_later(
                    self.faulthandler_timeout, repeat=True
                )
                self._faulthandler_armed = True
            except Exception as e:  # noqa: BLE001 —— 兜底手段失败不能影响主监控
                myfastapi_logger.warning(f"faulthandler 兜底抓栈开启失败：{e!r}")
        self._spawn_watchdog()
        myfastapi_logger.warning(
            f"事件循环卡顿取证已启动：阈值 {self.threshold:.2f}s，采样 {self.interval:.2f}s，"
            f"抓栈={'开' if self.capture_stack else '关'}，"
            f"连续采样 {self.sample_interval:.2f}s/{self.sample_max_samples} 次，"
            f"GC观测={'开' if self.gc_observe else '关'}"
            f"（>{self.gc_pause_threshold:.2f}s 记 event_loop_gc_pause），"
            f"告警节流 {self.alert_interval:.0f}s，loop_thread={self._loop_thread_name}"
            f"({self._loop_thread_ident})"
        )

    def _spawn_watchdog(self) -> None:
        """（重新）拉起取证线程。"""
        self._watchdog = threading.Thread(
            target=self._watch,
            name="loop-stall-watchdog",
            daemon=True,
        )
        self._watchdog.start()

    def _ensure_watchdog_alive(self) -> None:
        """取证线程自愈：它静默死掉时 loop 侧仍能发现并重新拉起。

        为什么需要：两条通道各管一头 —— loop 侧只写心跳、线程侧只抓栈。
        线程若因未捕获异常退出（例如未来改动引入了 bug），『卡顿无人取证』这件事
        不会有任何日志，等于又退回只能看时间戳的状态，所以必须自己巡检。
        """
        if self._stop_event.is_set() or self.is_watchdog_alive:
            return
        myfastapi_logger.warning(
            "event=event_loop_watchdog_restart 事件循环卡顿取证线程已意外退出，"
            f"正在重新拉起 dumps={self.stack_dump_count}"
        )
        self._spawn_watchdog()

    def stop(self) -> None:
        """通知取证线程退出并（尽力）等待它结束。可重复调用。"""
        self._stop_event.set()
        self._unregister_gc_callback()
        if self._faulthandler_armed:
            self._faulthandler_armed = False
            try:
                faulthandler.cancel_dump_traceback_later()
            except Exception:  # noqa: BLE001 —— 退出路径不抛异常
                pass
        thread = self._watchdog
        if thread is None or not thread.is_alive():
            return
        if thread is threading.current_thread():
            return
        # 正常情况下线程阻塞在 Event.wait(timeout) 上，set() 后立刻返回；
        # 给 2s 上限只是兜底（shutdown 阶段，不能再让事件循环无限等）。
        thread.join(timeout=2.0)

    # ------------------------------------------------------------------ loop 侧
    def _beat(self) -> None:
        """更新心跳：纯内存操作，绝不做 IO / 绝不 await。"""
        with self._hb_lock:
            self._hb_monotonic = time.monotonic()
            self._hb_seq += 1

    def _read_heartbeat(self) -> tuple[int, float]:
        with self._hb_lock:
            return self._hb_seq, self._hb_monotonic

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        # 与 watchdog 阈值保持一致：开启 asyncio debug 时两边口径相同
        loop.slow_callback_duration = self.threshold
        self.start()
        next_liveness_check = time.monotonic() + _WATCHDOG_LIVENESS_CHECK_SECONDS
        try:
            while True:
                self._beat()
                started = loop.time()
                await asyncio.sleep(self.interval)
                delayed = loop.time() - started - self.interval
                if time.monotonic() >= next_liveness_check:
                    next_liveness_check = (
                        time.monotonic() + _WATCHDOG_LIVENESS_CHECK_SECONDS
                    )
                    self._ensure_watchdog_alive()
                if delayed <= self.threshold:
                    continue
                self.stall_count += 1
                myfastapi_logger.warning(
                    f"event=event_loop_stall blocked={delayed:.2f}s "
                    f"threshold={self.threshold:.2f}s count={self.stall_count} "
                    f"（事件循环在此次采样窗口内被占住，HTTP / MQ 处理都会顺延；"
                    f"占住它的同步调用栈见同期的 event=event_loop_stall_stack）"
                )
                await self._alert(delayed)
        finally:
            # cancel（shutdown）或异常退出都要把取证线程带走，否则进程无法正常退出
            self.stop()

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

    # ------------------------------------------------------------------ 取证线程
    def _watch(self) -> None:
        """守护线程主循环：只读心跳 + 抓栈 + 打日志，不碰事件循环对象。"""
        # 检查节奏：不超过 heartbeat 采样间隔，也不超过阈值的一半，
        # 保证「刚开始卡」就能在一个很小的延迟内被发现。
        check_interval = max(0.02, min(self.interval, self.threshold / 2.0))
        while not self._stop_event.is_set():
            # 卡顿期间切到更细的采样节奏（连续采样通道）；空闲时保持原检查节奏。
            wait_seconds = (
                self.sample_interval
                if self._stall_started_at is not None
                else check_interval
            )
            if self._stop_event.wait(wait_seconds):
                break
            try:
                self._check_once()
            except Exception as e:  # noqa: BLE001 —— 取证线程绝不能因为异常退出
                myfastapi_logger.warning(f"事件循环卡顿取证线程异常（已忽略）：{e!r}")

    def _check_once(self) -> None:
        _seq, heartbeat_at = self._read_heartbeat()
        now = time.monotonic()
        blocked = now - heartbeat_at
        if blocked <= self.threshold:
            if self._stall_started_at is not None:
                total = now - self._stall_started_at
                self._stall_started_at = None
                self._dumped_current_round = False
                # 先落采样聚合（卡顿期间的最内层帧序列），再落「已恢复」作为本轮终点
                self._emit_sampled_stack(total)
                gc_gen2_delta = self._finish_gc_observation()
                myfastapi_logger.warning(
                    f"event=event_loop_stall_recovered blocked={total:.2f}s "
                    f"threshold={self.threshold:.2f}s "
                    f"（事件循环已恢复调度；累计抓栈 {self.stack_dump_count} 次，"
                    f"累计连续采样 {self.stack_sample_count} 次，"
                    f"本轮 GC gen2 collections 增量 {gc_gen2_delta}"
                    f"（累计 {self.last_gc_gen2_total}））"
                )
            return

        if self._stall_started_at is None:
            # 新一轮卡顿：起点取「最后一次有效心跳」的时刻
            self._stall_started_at = heartbeat_at
            self._dumped_current_round = False
            self._sample_signatures.clear()
            self._sample_stacks.clear()
            self._last_sample_ts = 0.0
            self._begin_gc_observation()
        # 每轮固定抓 1 次单点栈：不再受 alert_interval（10 分钟）限制，
        # 持续卡顿时也至少有一份完整现场；更细的演化交给连续采样通道。
        if not self._dumped_current_round:
            self._dumped_current_round = True
            self._dump_stack(blocked)
        self._collect_sample(now)

    def _capture_loop_stack(self) -> tuple[str, str] | None:
        """取事件循环线程当前栈：返回 (最内层帧签名, 截断后的栈文本)。"""
        ident = self._loop_thread_ident
        frame = sys._current_frames().get(ident) if ident is not None else None
        if frame is None:
            return None
        frames = traceback.extract_stack(frame)
        if not frames:
            return None
        innermost = frames[-1]
        signature = (
            f'File "{innermost.filename}", line {innermost.lineno}, '
            f"in {innermost.name}"
        )
        # 只保留靠近阻塞点的尾部，避免一次刷出几百行
        parts = traceback.format_list(frames[-self.stack_max_frames :])
        return signature, "".join(parts)

    def _dump_stack(self, blocked: float) -> None:
        """抓事件循环线程的同步调用栈（谁占住了 GIL / 谁在同步阻塞）。"""
        if not self.capture_stack:
            return
        captured = self._capture_loop_stack()
        if captured is None:
            innermost = "<未取到事件循环线程的栈：线程已退出或 ident 不可用>"
            stack_text = innermost + "\n"
        else:
            innermost, stack_text = captured
        self.stack_dump_count += 1
        myfastapi_logger.warning(
            f"event=event_loop_stall_stack blocked={blocked:.2f}s "
            f"threshold={self.threshold:.2f}s dumps={self.stack_dump_count} "
            f"loop_thread={self._loop_thread_name}({self._loop_thread_ident}) "
            f"innermost=[{innermost}] "
            f"—— 事件循环线程当前同步调用栈（每次采样都在推进，栈尾部即阻塞点）：\n"
            f"{stack_text}"
        )

    def _collect_sample(self, now: float) -> None:
        """卡顿期间按 ``sample_interval`` 连续采样最内层帧（帧数少时靠重复出现定位）。"""
        if not self.capture_stack:
            return
        if len(self._sample_signatures) >= self.sample_max_samples:
            return
        if self._last_sample_ts and now - self._last_sample_ts < self.sample_interval:
            return
        captured = self._capture_loop_stack()
        if captured is None:
            return
        signature, stack_text = captured
        self._sample_signatures.append(signature)
        # 同一签名只保留一份栈文本（每轮结束后整体清空），控制内存占用
        self._sample_stacks[signature] = stack_text
        self._last_sample_ts = now
        self.stack_sample_count += 1

    def _emit_sampled_stack(self, total: float) -> None:
        """把本轮连续采样里「最内层帧重复出现」的采样聚合成一条日志。"""
        signatures = self._sample_signatures
        self._sample_signatures = []
        stacks = self._sample_stacks
        self._sample_stacks = {}
        if not signatures:
            return
        counter = Counter(signatures)
        stable = [
            (signature, count)
            for signature, count in counter.most_common(self.sample_top_frames)
            if count >= 2
        ]
        if not stable:
            # 只采到 1 帧（卡顿刚好超过阈值）时也留一条，避免现场完全丢失
            stable = counter.most_common(1)
        blocks = [
            f"[{count}/{len(signatures)}] {signature}\n{stacks.get(signature, '')}"
            for signature, count in stable
        ]
        myfastapi_logger.warning(
            f"event=event_loop_stall_stack_sampled blocked={total:.2f}s "
            f"threshold={self.threshold:.2f}s samples={len(signatures)} "
            f"interval={self.sample_interval:.2f}s distinct={len(counter)} "
            f"loop_thread={self._loop_thread_name}({self._loop_thread_ident}) "
            f"—— 卡顿期间按固定间隔连续采样，最内层帧重复出现者如下"
            f"（count/samples，栈尾即阻塞点）：\n" + "\n".join(blocks)
        )

    # ------------------------------------------------------------------ GC 观测
    def _register_gc_callback(self) -> None:
        with self._gc_lock:
            if self._gc_registered:
                return
            gc.callbacks.append(self._gc_callback)
            self._gc_registered = True

    def _unregister_gc_callback(self) -> None:
        with self._gc_lock:
            if not self._gc_registered:
                return
            self._gc_registered = False
            try:
                gc.callbacks.remove(self._gc_callback)
            except ValueError:
                pass

    def _gc_callback(self, phase: str, info: dict) -> None:
        """``gc.callbacks`` 钩子：只记录超过阈值的一次回收耗时（不改变 GC 行为）。"""
        generation = int(info.get("generation", -1))
        now = time.monotonic()
        if phase == "start":
            with self._gc_lock:
                self._gc_started[generation] = now
            return
        if phase != "stop":
            return
        with self._gc_lock:
            started = self._gc_started.pop(generation, None)
        if started is None:
            return
        elapsed = now - started
        if elapsed < self.gc_pause_threshold:
            return
        self.gc_pause_count += 1
        myfastapi_logger.warning(
            f"event=event_loop_gc_pause generation={generation} elapsed={elapsed:.3f}s "
            f"threshold={self.gc_pause_threshold:.2f}s count={self.gc_pause_count} "
            f"collected={info.get('collected', 0)} "
            f"uncollectable={info.get('uncollectable', 0)} "
            f"thread={threading.current_thread().name} "
            f"（GC 是 stop-the-world 的：即使回收发生在工作线程，事件循环也会一起暂停）"
        )

    def _begin_gc_observation(self) -> None:
        """记录本轮卡顿开始时的 GC 统计（用于恢复时计算 gen2 collections 增量）。"""
        if not self.gc_observe:
            return
        with self._gc_lock:
            self._gc_stall_start_stats = [dict(stat) for stat in gc.get_stats()]

    def _finish_gc_observation(self) -> int:
        """计算本轮卡顿期间 gen2 collections 的增量并清空起点快照。"""
        with self._gc_lock:
            started = self._gc_stall_start_stats
            self._gc_stall_start_stats = None
        if not self.gc_observe or started is None:
            return 0
        current = gc.get_stats()
        delta = 0
        if len(current) > 2:
            self.last_gc_gen2_total = int(current[2].get("collections", 0))
            if len(started) > 2:
                delta = self.last_gc_gen2_total - int(
                    started[2].get("collections", 0)
                )
        self.last_gc_gen2_delta = max(0, delta)
        return self.last_gc_gen2_delta

    # ------------------------------------------------------------------ 观测
    @property
    def is_watchdog_alive(self) -> bool:
        return self._watchdog is not None and self._watchdog.is_alive()


def start_event_loop_stall_monitor(
    *,
    threshold: float | None = None,
    interval: float | None = None,
    alert_interval: float | None = None,
    capture_stack: bool | None = None,
    faulthandler_enabled: bool | None = None,
) -> asyncio.Task:
    """在当前事件循环里启动监控协程（含取证线程），返回 Task。

    Task 由调用方负责在 shutdown 时 cancel —— ``run`` 的 ``finally`` 会顺带停掉取证线程。
    未显式传入的参数取自 ``CONFIG.loop_monitor``（环境变量 ``LOOP_MONITOR__*`` 可覆盖）。
    """

    async def _alert(message: str) -> None:
        await a_push_error(subject="事件循环卡顿", content=message)

    cfg = _load_config()
    monitor = EventLoopStallMonitor(
        threshold=cfg.threshold if threshold is None else threshold,
        interval=cfg.interval if interval is None else interval,
        alert_interval=(
            cfg.alert_interval if alert_interval is None else alert_interval
        ),
        alerter=_alert,
        capture_stack=cfg.capture_stack if capture_stack is None else capture_stack,
        stack_max_frames=cfg.stack_max_frames,
        sample_interval=cfg.sample_interval,
        sample_max_samples=cfg.sample_max_samples,
        sample_top_frames=cfg.sample_top_frames,
        gc_observe=cfg.gc_observe,
        gc_pause_threshold=cfg.gc_pause_threshold,
        faulthandler_enabled=(
            cfg.faulthandler_enabled if faulthandler_enabled is None else faulthandler_enabled
        ),
        faulthandler_timeout=cfg.faulthandler_timeout,
    )
    task = asyncio.get_running_loop().create_task(monitor.run())
    task.set_name("event-loop-stall-monitor")
    return task


def _load_config() -> LoopMonitorConfig:
    """读取 ``CONFIG.loop_monitor``（独立成函数，便于测试时替换 / 取值口径统一）。"""
    return CONFIG.loop_monitor


__all__ = ["EventLoopStallMonitor", "start_event_loop_stall_monitor"]
