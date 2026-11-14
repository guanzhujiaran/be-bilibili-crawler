from typing import Any, AsyncGenerator, Generic, List, ClassVar, TypeVar
from abc import abstractmethod
import asyncio
import random
from types import EllipsisType
from CONFIG import CONFIG
from log.base_log import myfastapi_logger
from Service.BaseCrawler.base.core import BaseCrawler
from Service.BaseCrawler.config import CrawlerConfig
from Service.BaseCrawler.model.base import WorkerModel, WorkerStatus, ParamsType
from Service.BaseCrawler.plugin.base import CrawlerPlugin
from Utils.通用.Common import ConcurrencyLimit, asyncio_gather
from Utils.推送.PushMe import a_push_error
from Utils.代理.mdoel.ProxyTier import ProxyTier
from Utils.代理.mdoel.request_context import (
    ProxyAttemptState,
    bind_attempt_state,
    bind_retry_yield,
    reset_attempt_state,
    reset_retry_yield,
    set_proxy_tier,
    reset_proxy_tier,
)
from Utils.代理.数据库操作.ProxyHealth import proxy_health_store

P = TypeVar("P", bound=CrawlerPlugin)


def _resolve(value, fallback, sentinel=None):
    """Config 里的值优先；等于哨兵值（默认 None）时回退统一配置。

    ``worker_max_timeout`` 的哨兵是 ``-1``：它的合法值里 ``None`` 表示「不限制」，
    不能跟「跟随统一配置」混用。
    """
    return fallback if value is sentinel or value == sentinel else value


class UnlimitedCrawler(BaseCrawler[ParamsType], Generic[ParamsType]):
    """
    无限爬虫基类，支持动态生成任务并执行

    特性：
    - 支持插件扩展（统计、监控、限制等）
    - 支持任务失败重入队
    - 支持并发控制（通过信号量）
    - 自动管理任务队列和 worker 线程池

    使用示例：
        from Service.BaseCrawler.config import CrawlerConfig, PluginConfig
        from Service.BaseCrawler.plugin.statusPlugin import StatsPlugin

        class MyCrawlerConfig(CrawlerConfig):
            max_sem: int = 20
            logger: Any = myfastapi_logger
            # plugin_name 即注册到 self 上的属性名（反射自动赋值，无需手写 self.xxx = ...）
            plugins: List[PluginConfig] = [PluginConfig("stats_plugin", StatsPlugin)]

        class MyCrawler(UnlimitedCrawler[MyParams]):
            Config = MyCrawlerConfig

            async def is_stop(self) -> bool:
                # 判断是否应该停止生成新任务
                return False

            async def key_params_gen(self, params=None) -> AsyncGenerator[MyParams, None]:
                # 动态生成任务参数
                for i in range(100):
                    yield MyParams(id=i)

            async def handle_fetch(self, params: MyParams) -> WorkerStatus:
                # 处理单个任务
                await self.fetch_data(params)
                return WorkerStatus.complete

    配置说明：
        max_sem / logger / plugins 及各类业务开关统一由 ``Config``（CrawlerConfig 子类）
        管理，运行时由 CONFIG.get_crawler_config 集中注入（注册表在 Service.BaseCrawler.config 中），
        实现「每个爬虫一份独立配置」。``__init__`` 不再接受任何外部传参。

        插件声明为 ``PluginConfig(plugin_name, plugin_cls)``，爬虫初始化时会通过反射
        执行 ``setattr(self, plugin_name, plugin_cls(self))``，子类直接用
        ``self.<plugin_name>``（如 ``self.stats_plugin``）即可访问，无需手动绑定。
    """

    _plugins: List[CrawlerPlugin[ParamsType]]
    # 每个爬虫通过覆盖此属性为自己的 CrawlerConfig 子类，
    # 运行时由 CONFIG 集中注入对应配置（见 CONFIG.get_crawler_config）
    Config: ClassVar[type[CrawlerConfig]] = CrawlerConfig

    def _load_config(self) -> CrawlerConfig:
        """
        加载爬虫配置。

        默认直接返回 ``self.Config`` 对应的集中配置实例。
        若需要在运行时注入动态值（如根据业务类型选择 logger），
        子类可重写本方法，返回 ``CONFIG.get_crawler_config(self.Config).model_copy(update=...)``。
        """
        return CONFIG.get_crawler_config(self.Config)

    def __init__(self):
        """
        初始化无限爬虫。

        并发来自 ``CONFIG.concurrency``（按 Config 的 concurrency_key 取），
        其余运行参数（logger / plugins / 超时 / 重试）来自 ``self.Config`` 对应的配置实例，
        不再接受任何外部传参。各插件会按 Config 中声明的 ``plugin_name`` 自动绑定到
        ``self.<plugin_name>``，子类直接用该属性即可（也可通过 ``self.get_plugin(PluginType)`` 查找）。
        """
        # 直接使用 self.Config 对应的集中配置实例（由 CONFIG 注入）
        self.config = self._load_config()

        # 运行行为：Config 里留哨兵值（None / -1）表示「跟随 CONFIG.crawler_runtime」，
        # 这样各爬虫 Config 只声明与统一值不同的项，不重复抄默认值。
        runtime = CONFIG.crawler_runtime
        self.requeue_on_fetch_fail = _resolve(
            self.config.requeue_on_fetch_fail, runtime.requeue_on_fetch_fail
        )
        self.requeue_on_timeout = _resolve(
            self.config.requeue_on_timeout, runtime.requeue_on_timeout
        )
        self.max_retries = _resolve(self.config.max_retries, runtime.max_retries)
        self.worker_max_timeout = _resolve(
            self.config.worker_max_timeout, runtime.worker_max_timeout, sentinel=-1
        )
        self.log_timeout_error = _resolve(
            self.config.log_timeout_error, runtime.log_timeout_error
        )
        self.log_error = _resolve(self.config.log_error, runtime.log_error)
        self.worker_error_delay = _resolve(
            self.config.worker_error_delay, runtime.worker_error_delay
        )
        # 失败任务的延迟重入队（避免坏代理期间「取出→失败→立刻放回」的忙循环）
        self.requeue_delay_base = _resolve(
            self.config.requeue_delay_base, runtime.requeue_delay_base
        )
        self.requeue_delay_max = _resolve(
            self.config.requeue_delay_max, runtime.requeue_delay_max
        )

        # 并发：统一从 CONFIG.concurrency 取，各爬虫只声明 concurrency_key。
        concurrency = CONFIG.concurrency
        key = self.config.concurrency_key
        if key and key not in type(concurrency).model_fields:
            myfastapi_logger.warning(
                f"[{type(self).__name__}] concurrency_key={key!r} 在 ConcurrencyConfig "
                f"里不存在，回退 crawler_workers={concurrency.crawler_workers}"
            )
            key = ""
        crawler_sem = getattr(concurrency, key or "crawler_workers")
        super().__init__(
            max_sem=crawler_sem,
            _logger=self.config.logger,
        )

        # 代理档位由各爬虫 Config 声明：只影响「走哪条车道 / 取哪一档代理」，
        # 不在这里写死任何业务判断。
        self.proxy_tier: ProxyTier = self.config.proxy_tier
        self.slow_lane_ratio: float = self.config.slow_lane_ratio

        # 实例名优先取 Config 声明的 crawler_name（支持同类多实例各自命名），
        # 留空则保持基类的类名；调度器创建时还会进一步回填为 BackgroundServiceName 的值。
        if self.config.crawler_name:
            self.crawler_name = self.config.crawler_name

        # 本轮在跑的 worker task 集合：用于「杀旧开新」时回收残留 worker
        self._worker_tasks: set[asyncio.Task] = set()
        # 延迟重入队的挂起任务：run() 收尾时要等它们把任务放回队列，否则会漏任务
        self._pending_requeues: set[asyncio.Task] = set()
        # 代理背压监控任务（只有 slow 车道需要）
        self._backpressure_task: asyncio.Task | None = None

        self._plugins = []
        for plugin_cfg in self.config.plugins:
            plugin = plugin_cfg.plugin_cls(crawler=self, **plugin_cfg.plugin_kwargs)
            # 通过反射自动把插件绑定到 self.<plugin_name>，子类无需再手动写 self.xxx = SomePlugin(self)
            setattr(self, plugin_cfg.plugin_name, plugin)
            self.__register_plugin(plugin)

    def get_plugin(self, plugin_cls: type[P]) -> P:
        """
        从已注册插件中按类型获取插件实例。

        用于按类型查找已注册插件。插件通常已在 ``__init__`` 中按 Config 声明的
        ``plugin_name`` 自动绑定到 ``self.<plugin_name>``，本方法作为补充检索手段。

        Args:
            plugin_cls: 插件类型，如 StatsPlugin、SequentialNullStopPlugin

        Returns:
            匹配到的插件实例
        """
        for plugin in self._plugins:
            if isinstance(plugin, plugin_cls):
                return plugin  # type: ignore[return-value]
        raise ValueError(
            f"插件 {plugin_cls.__name__} 未在 {self.__class__.__name__} 中注册，"
            f"请检查其 Config.plugins 是否包含对应工厂。"
        )

    @property
    def plugins(self) -> List[CrawlerPlugin[ParamsType]]:
        """
        获取已注册的插件列表

        Returns:
            List[CrawlerPlugin[ParamsType]]: 插件列表
                常见插件：StatsPlugin（统计）、SequentialNullStopPlugin（限制连续空结果）等
        """
        return self._plugins

    def __register_plugin(self, plugin: CrawlerPlugin[ParamsType]):
        """
        注册插件到爬虫实例

        Args:
            plugin: 要注册的插件对象

        功能：
        - 检查插件是否已注册（避免重复注册）
        - 添加插件到内部列表
        - 调用插件的 on_plugin_register 方法进行初始化

        注意：
        - 插件的注册顺序影响回调执行顺序
        - 如果插件已存在，不会重复注册
        """
        if plugin not in self._plugins:
            self._plugins.append(plugin)
            plugin.on_plugin_register()
            # self.log.debug(
            #     self.format_log(
            #         f"Plugin {plugin.__class__.__name__} registered to {self.__class__.__name__}."
            #     )
            # )

    @abstractmethod
    async def is_stop(self) -> bool:
        """
        判断是否应该停止生成新任务

        Returns:
            bool: True 表示应该停止，False 表示继续生成任务

        实现示例：
            async def is_stop(self) -> bool:
                # 连续失败次数超过阈值时停止
                return self._fail_count >= 10
        """
        ...

    @abstractmethod
    async def key_params_gen(
        self, params: ParamsType | Any | None
    ) -> AsyncGenerator[EllipsisType, None]:
        """
        生成任务参数的异步生成器

        Args:
            params: 初始参数，用于确定从哪里开始生成

        Yields:
            EllipsisType: 生成的任务参数，实际类型由子类定义

        实现示例：
            async def key_params_gen(self, params: MyParams) -> AsyncGenerator[MyParams, None]:
                if params is None:
                    start_id = 1
                else:
                    start_id = params.id + 1

                for i in range(start_id, 1000):
                    yield MyParams(id=i)
                    await asyncio.sleep(0.1)  # 控制生成速度
        """
        yield ...

    @abstractmethod
    async def handle_fetch(self, params: ParamsType | None) -> WorkerStatus:
        """
        处理单个任务，获取数据

        Args:
            params: 任务参数，由 key_params_gen 生成

        Returns:
            WorkerStatus | Any: 任务状态或返回值
                - WorkerStatus.complete: 任务成功完成
                - WorkerStatus.fail: 任务失败
                - 其他值: 表示任务成功完成，并返回具体数据

        实现示例：
            async def handle_fetch(self, params: MyParams) -> WorkerStatus:
                try:
                    data = await self.api.fetch(params.id)
                    await self.save_to_db(data)
                    return WorkerStatus.complete
                except Exception as e:
                    self.log.error(f"获取数据失败: {e}")
                    return WorkerStatus.fail
        """
        ...

    @abstractmethod
    async def main(self, *args, **kwargs):
        """
        爬虫的主入口方法，包含完整的业务逻辑流程

        可以包含以下内容：
        - 数据预处理
        - 调用 self.run() 执行爬取
        - 数据后处理
        - 结果统计和上报

        实现示例：
            async def main(self, *args, **kwargs):
                # 准备工作
                await self.init_database()

                # 执行爬取
                await self.run(init_params=MyParams(id=0))

                # 清理工作
                await self.close_connections()

                # 上报结果
                await self.send_report()
        """

    async def on_task_requeue(self, worker_model: WorkerModel):
        """
        任务重新入队前的回调，允许子类修改任务参数

        Args:
            worker_model: 包含任务参数、序列号、状态等信息的模型对象

        用途：
        - 修改任务参数（如更新重试时的参数）
        - 记录重试信息
        - 重置任务状态相关数据

        注意：
        - 此回调在任务重新入队前调用
        - 此时任务状态已设置为 pending，retry_count 已在 _handle_requeue 中 +1
        - 修改 worker_model.params 会影响重试时的任务参数
        - retry_count 的自增不放在这里：子类覆盖时若忘记调用 super 会导致重试计数失效
        """
        await asyncio_gather(
            *[x.on_task_requeue(worker_model) for x in self._plugins], log=self.log
        )

    async def on_worker_start(self, worker_model: WorkerModel):
        """
        Worker 开始处理任务时的回调，触发所有插件的 on_worker_start 方法

        Args:
            worker_model: 包含任务参数、序列号、状态等信息的模型对象

        用途：
        - 记录任务开始时间
        - 更新任务状态统计
        - 初始化任务上下文
        """
        await asyncio_gather(
            *[x.on_worker_start(worker_model) for x in self._plugins], log=self.log
        )

    async def on_worker_end(self, worker_model: WorkerModel):
        """
        Worker 完成任务处理时的回调，触发所有插件的 on_worker_end 方法

        Args:
            worker_model: 包含任务参数、序列号、状态等信息的模型对象

        用途：
        - 更新任务统计信息（成功/失败计数）
        - 记录任务执行时长
        - 更新进度信息
        """
        # self.log.debug(
        #     self.format_log(
        #         f"开始执行 on_worker_end。任务参数：{worker_model.params}，状态：{worker_model.fetchStatus}"
        #     )
        # )
        # self.log.debug(
        #     self.format_log(
        #         f"即将调用插件回调，插件数量：{len(self._plugins)}"
        #     )
        # )
        result = await asyncio_gather(
            *[x.on_worker_end(worker_model) for x in self._plugins], log=self.log
        )
        # self.log.debug(
        #     self.format_log(
        #         f"插件回调完成，返回结果：{result}"
        #     )
        # )
        # self.log.debug(
        #     self.format_log(
        #         f"完成执行 on_worker_end。任务参数：{worker_model.params}，状态：{worker_model.fetchStatus}"
        #     )
        # )

    async def on_run_end(self, end_param: WorkerModel):
        """
        爬虫运行结束时的回调，触发所有插件的 on_run_end 方法

        Args:
            end_param: 最后一个任务参数（可能为 None）

        用途：
        - 生成统计报告
        - 清理资源
        - 保存最终状态
        - 发送通知
        """
        await asyncio_gather(
            *[x.on_run_end(end_param) for x in self._plugins], log=self.log
        )

    async def worker(
        self,
        queue: asyncio.Queue[WorkerModel | None] | None = None,
        gate: ConcurrencyLimit | None = None,
    ):
        """
        Worker 协程，固定池模式，持续从队列中获取任务并处理

        Args:
            queue: 本轮车道的任务队列。必须由 run() 显式传入并全程使用该实例，
                不能用 self.task_queue —— 爬虫实例会被「杀旧开新」复用，
                旧 worker 若回头读 self.task_queue 会拿到新一轮的队列对象，
                导致 task_done() 打到新队列上（ValueError: task_done() called
                too many times）并抢走本轮任务。默认为 self.task_queue 仅为兼容。
            gate: 本轮车道的并发闸（fast / slow 各一套）。默认为 fast 车道。

        工作流程：
        1. 从任务队列获取一个任务（如果收到 None 则退出）
        2. 获取并发额度（控制并发数）
        3. 绑定本次任务的上下文（代理档位 / 失败代理名单 / 重试让出钩子）
        4. 触发 on_worker_start 回调
        5. 调用 handle_fetch 处理任务
        6. 处理异常情况、更新任务状态
        7. 触发 on_worker_end 回调
        8. 释放并发额度
        9. 如果任务失败且需要重试，在**并发额度之外**延迟重新入队（避免死锁与忙循环）
        10. 循环回到步骤 1

        重要：重新入队的操作必须在并发额度之外执行，否则当 max_sem=1 时可能导致死锁。
        """
        if queue is None:
            queue = self.task_queue
        if gate is None:
            gate = self.fast_gate
        # 重试等待期间「让出并发额度」的钩子：注入到上下文后，request_wrapper
        # 的所有 sleep 都会先释放额度、睡完再取回，坏代理不会长期占满 worker。
        retry_yield = self._make_retry_yield(gate)
        while True:
            worker_model: WorkerModel | None = await queue.get()
            if worker_model is None:
                self.log.debug(self.format_log("收到退出信号，退出任务处理。"))
                queue.task_done()
                return

            should_requeue = False
            tier_token = set_proxy_tier(self.proxy_tier)
            attempt_token = bind_attempt_state(ProxyAttemptState())
            yield_token = bind_retry_yield(retry_yield)

            try:
                async with gate:
                    try:
                        self.log.debug(
                            self.format_log(f"开始处理任务: {worker_model.params}")
                        )
                        await self.on_worker_start(worker_model)
                        worker_model.fetchStatus = await self._execute_fetch(
                            worker_model
                        )
                        self.log.debug(
                            self.format_log(f"完成任务状态: {worker_model.fetchStatus}")
                        )
                        should_requeue = self._should_requeue(worker_model)
                    finally:
                        queue.task_done()

                if should_requeue:
                    self.log.debug(
                        self.format_log(f"任务需要重试: {worker_model.params}")
                    )
                    await self._handle_requeue(worker_model, queue)
            except asyncio.CancelledError:
                # 取消（如调度器"杀旧开新"）必须向上传播，此时不再触发统计回调，
                # 否则 await 会立即被二次取消；task_done 已在内层 finally 中保证执行。
                self.log.debug(self.format_log("worker 被取消，退出任务处理。"))
                raise
            except Exception as e:
                # 兜底：任何未捕获异常都不能让 worker 协程静默退出（否则并发度永久 -1、
                # 该任务也完全不进入统计）。标记失败后继续走统计回调。
                self.log.exception(
                    self.format_log(
                        f"worker 处理任务异常: {worker_model.params}, {e!r}"
                    )
                )
                if worker_model.fetchStatus == WorkerStatus.pending:
                    worker_model.fetchStatus = WorkerStatus.fail
            finally:
                reset_retry_yield(yield_token)
                reset_attempt_state(attempt_token)
                reset_proxy_tier(tier_token)

            await self.on_worker_end(worker_model)

    def _make_retry_yield(self, gate: ConcurrencyLimit):
        """构造「让出并发额度」钩子：release → sleep → acquire。

        取消时不重新取回额度：本轮 run 即将结束，gate 随本轮丢弃，
        不会把偏差带进下一轮（这也是本类不用裸 Semaphore 的原因）。
        """

        async def _yield(delay: float) -> None:
            await gate.release()
            try:
                if delay > 0:
                    await asyncio.sleep(delay)
            except asyncio.CancelledError:
                raise
            await gate.acquire()

        return _yield

    async def _execute_fetch(self, worker_model: WorkerModel) -> WorkerStatus:
        """执行 fetch 操作，处理超时和异常，返回任务状态。"""
        try:
            async with asyncio.timeout(self.worker_max_timeout):
                fetch_result = await self.handle_fetch(worker_model.params)
        except asyncio.TimeoutError:
            if self.log_timeout_error:
                self.log.exception(
                    self.format_log(f"爬取超时：{self.worker_max_timeout}s")
                )
            return WorkerStatus.timeoutError
        except Exception as e:
            if self.log_error:
                self.log.exception(self.format_log(f"爬取异常：{e}"))
                await a_push_error(
                    subject="运行异常",
                    content=f"爬取任务[{self.crawler_name}]异常\n{worker_model}\n{e}",
                )
            await asyncio.sleep(self.worker_error_delay)
            return WorkerStatus.fail

        if fetch_result is None:
            # handle_fetch 未显式返回状态：按约定视为成功，但记录告警提醒补全返回值，
            # 否则该任务永远不可能被判定为 nullData / fail，统计与停止条件都会失真。
            self.log.warning(
                self.format_log(
                    f"handle_fetch 返回 None，已按成功处理，建议显式返回 WorkerStatus："
                    f"{worker_model.params}"
                )
            )
            return WorkerStatus.complete
        if not isinstance(fetch_result, WorkerStatus):
            return WorkerStatus.complete
        return fetch_result

    def _should_requeue(self, worker_model: WorkerModel) -> bool:
        """判断任务是否需要重试，检查状态和重试次数限制。"""
        status = worker_model.fetchStatus
        if status == WorkerStatus.fail and not self.requeue_on_fetch_fail:
            return False
        if status == WorkerStatus.timeoutError and not self.requeue_on_timeout:
            return False
        if status not in (WorkerStatus.fail, WorkerStatus.timeoutError):
            return False

        # 注意：retry_count 只在 _handle_requeue 中自增一次，
        # 此前在 _should_requeue 与 on_task_requeue 中各 +1，导致一轮重试被计成 2 次，
        # 实际重试次数只有 max_retries 的一半。
        if self.max_retries < 0 or worker_model.retry_count < self.max_retries:
            return True

        self.log.warning(
            self.format_log(
                f"任务已达到最大重试次数({self.max_retries})，不再重试：{worker_model.params}"
            )
        )
        return False

    def _requeue_delay(self, retry_count: int) -> float:
        """失败任务的延迟重入队时长：``base × 2^(retry-1)``，封顶 ``requeue_delay_max``。

        没有这个延迟时，坏代理会让队列形成「取出→失败→立刻放回」的忙循环：
        任务重入次数暴涨、有效数据为 0，看起来像 worker 卡死。
        """
        base = max(0.0, float(self.requeue_delay_base or 0))
        if base <= 0:
            return 0.0
        cap = max(base, float(self.requeue_delay_max or 0))
        exponent = min(max(retry_count - 1, 0), 16)
        delay = min(base * (2**exponent), cap)
        # 抖动：避免同一批失败任务在同一时刻一起回到队列；抖动后仍要夹在 cap 内
        return min(cap, delay * random.uniform(0.85, 1.15))

    async def _handle_requeue(
        self,
        worker_model: WorkerModel,
        queue: asyncio.Queue[WorkerModel | None] | None = None,
    ):
        """将任务延迟重新入队（在并发额度之外执行，避免死锁）。

        queue 必须与 worker 处理该任务时使用的是同一个队列实例，
        否则「杀旧开新」后重排的任务会落进新一轮的队列。
        延迟通过独立 task 实现：不占用 worker 协程，也不占并发额度。
        """
        if queue is None:
            queue = self.task_queue
        worker_model.fetchStatus = WorkerStatus.pending
        worker_model.retry_count += 1
        await self.on_task_requeue(worker_model)
        delay = self._requeue_delay(worker_model.retry_count)
        if delay <= 0:
            await queue.put(worker_model)
            return
        task = asyncio.create_task(
            self._delayed_requeue(queue, worker_model, delay)
        )
        self._pending_requeues.add(task)
        task.add_done_callback(self._pending_requeues.discard)

    async def _delayed_requeue(
        self,
        queue: asyncio.Queue[WorkerModel | None],
        worker_model: WorkerModel,
        delay: float,
    ):
        await asyncio.sleep(delay)
        await queue.put(worker_model)

    async def _reap_leftover_workers(self):
        """回收上一轮残留的 worker 协程与「延迟重入队」任务。

        「杀旧开新」只 cancel 了 run()/main() 所在的协程，worker 是独立 task，
        不会随之结束：它们会阻塞在旧队列的 get() 上，并继续占用并发额度。
        新一轮 run() 开始时统一 cancel 掉，避免协程与额度泄漏。
        """
        leftover = [t for t in self._worker_tasks if not t.done()]
        pending = [t for t in self._pending_requeues if not t.done()]
        # 上一轮被 cancel 时不会走到 run() 的收尾逻辑，背压监控要在这里一并回收
        if self._backpressure_task is not None and not self._backpressure_task.done():
            self._backpressure_task.cancel()
            await asyncio.gather(self._backpressure_task, return_exceptions=True)
        self._backpressure_task = None
        if not leftover and not pending:
            return
        if leftover:
            self.log.warning(
                self.format_log(f"发现上一轮残留 worker {len(leftover)} 个，已回收。")
            )
        if pending:
            # 延迟重入队任务属于上一轮：直接取消，任务本身随旧队列一起丢弃
            self.log.warning(
                self.format_log(
                    f"发现上一轮残留的延迟重入队任务 {len(pending)} 个，已取消。"
                )
            )
        for task in leftover + pending:
            task.cancel()
        await asyncio.gather(*leftover, *pending, return_exceptions=True)
        self._worker_tasks.clear()
        self._pending_requeues.clear()

    def _slow_lane_base_limit(self) -> int:
        """slow 车道基准并发 = ``max_sem × slow_lane_ratio``（至少 1）。"""
        ratio = self.slow_lane_ratio if self.slow_lane_ratio > 0 else 1.0
        return max(1, int(round(self.max_sem * ratio)))

    async def _proxy_backpressure_loop(
        self, gate: ConcurrencyLimit, base_limit: int
    ) -> None:
        """代理背压：已验证可用池低于阈值时按比例收缩 slow 车道并发。

        背景：坏代理占满 worker 会让整个 slow 车道「有并发、无产出」。
        这里用健康状态机的分档计数（Redis，O(1)）判断「已验证可用池」是否够用，
        不够就按比例收缩 slow 并发，把有限的槽位留给更有希望的探测；
        **fast 车道用自己的闸，完全不受影响**。

        口径为什么是 ``hot + warm`` 而不是只看 hot：cold 只代表「还没验证」，
        不代表不可用；只看 hot 会让「池子里全是冷代理」的早期阶段把 slow 并发
        压到最低（本项目的 dyn / other_space 正是跑在 slow 车道），反而拖慢抓取。

        收缩不会打断在途任务，只限制新的进入；池子恢复后自动回到基准值。
        """
        cfg = CONFIG.proxy_health
        interval = max(5, cfg.backpressure_interval)
        watermark = max(1, cfg.hot_low_watermark)
        min_ratio = min(1.0, max(0.0, cfg.slow_shrink_min_ratio))
        last_limit = base_limit
        cycle = 0
        while True:
            await asyncio.sleep(interval)
            cycle += 1
            try:
                counts = await proxy_health_store.get_counts()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 背压失败不能拖垮爬虫
                self.log.warning(
                    self.format_log(f"读取代理分档失败，本轮不调整并发：{e!r}")
                )
                continue
            verified = counts.hot + counts.warm
            if verified >= watermark:
                target = base_limit
            else:
                ratio = max(min_ratio, verified / watermark)
                target = max(1, int(round(base_limit * ratio)))
            if await gate.set_limit(target):
                self.log.warning(
                    self.format_log(
                        f"代理背压：slow 并发 {last_limit} → {target}"
                        f"（已验证池 hot+warm={verified} < 阈值 {watermark}，"
                        f"fast 车道不受影响）；分档 {counts.to_log_str()}"
                    )
                )
                last_limit = target
            elif cycle % 20 == 0:
                # 每 20 个周期（约 5 分钟）留一条分档快照，便于观察池子变化
                self.log.warning(
                    self.format_log(
                        f"代理分档快照：slow 并发 {gate.limit}/{base_limit}"
                        f"（活跃 {gate.active}）；{counts.to_log_str()}"
                    )
                )

    async def run(self, init_params: ParamsType | None = None):
        """
        爬虫的主运行方法，负责任务的生成和调度

        工作流程：
        1. 创建固定数量的 worker（等于 max_sem）
        2. 通过 key_params_gen 动态生成任务参数并放入队列
        3. Worker 持续从队列获取任务并处理
        4. 检查停止条件（is_stop 和插件的 should_stop_check）
        5. 支持暂停功能
        6. 发送哨兵值通知 worker 退出
        7. 等待所有 worker 完成任务
        8. 清理队列并触发 on_run_end

        Args:
            init_params: 初始任务参数，用于确定从哪里开始生成后续任务

        关键机制：
        - 任务队列：异步队列，用于在任务生成器和 worker 之间传递任务
        - 固定worker池：创建 max_sem 个 worker 持续处理任务，避免协程数量爆炸
        - 哨兵模式：使用 None 作为退出信号，优雅地关闭 worker
        - 回调机制：task.add_done_callback(task_set.discard) 自动清理已完成的 worker

        注意：
        - 使用固定worker池替代动态创建worker，从根本上解决协程数量问题
        - seqId 用于标识任务的顺序，从 0 开始递增
        - 支持暂停：通过 self._is_pause 标志控制，暂停时每 10 秒检查一次
        """
        self.log.info(self.format_log(f"starting with init_params: {init_params}"))

        seqId = 0
        worker_model = WorkerModel(params=init_params, seqId=seqId)

        # 先回收上一轮残留的 worker：调度器"杀旧开新"只 cancel 了 run() 所在协程，
        # worker 是独立 task，会继续阻塞在旧队列上。
        await self._reap_leftover_workers()

        # 重建任务队列：爬虫实例是复用的（调度器"杀旧开新"会 cancel 上一轮），
        # 上一轮被中断时队列中可能残留未执行任务、且 unfinished_tasks 计数可能不为 0
        # （cancel 打断在 get() 与 task_done() 之间时），残留会导致本轮统计被污染、
        # 甚至下一轮 join() 永久等待。
        leftover = self.task_queue.qsize()
        if leftover:
            self.log.warning(
                self.format_log(f"发现上一轮残留任务 {leftover} 条，已丢弃并重建队列。")
            )

        # 本轮两条车道都重建（并发闸同理）：取消后计数偏差随本轮一起丢弃。
        fast_queue: asyncio.Queue[WorkerModel | None] = asyncio.Queue()
        slow_queue: asyncio.Queue[WorkerModel | None] = asyncio.Queue()
        fast_gate = ConcurrencyLimit(self.max_sem)
        slow_base_limit = self._slow_lane_base_limit()
        slow_gate = ConcurrencyLimit(slow_base_limit)
        self.fast_queue = fast_queue
        self.slow_queue = slow_queue
        self.fast_gate = fast_gate
        self.slow_gate = slow_gate
        # 本轮生效车道：由爬虫 Config 声明，队列与并发闸都取这一条
        queue = fast_queue if self.proxy_tier is ProxyTier.FAST else slow_queue
        gate = fast_gate if self.proxy_tier is ProxyTier.FAST else slow_gate
        self.task_queue = queue

        await asyncio_gather(
            *[x.on_run_start(worker_model) for x in self._plugins], log=self.log
        )

        # 创建固定数量的 worker 池
        # 确保 worker 数量不超过信号量大小，也不超过一个合理上限
        worker_count = self.max_sem

        task_set = set()

        # 启动固定数量的 worker（只挂在生效车道上；另一条车道保留结构但不占协程）
        for _ in range(worker_count):
            task = asyncio.create_task(self.worker(queue, gate))
            task_set.add(task)
            self._worker_tasks.add(task)
            task.add_done_callback(task_set.discard)
            task.add_done_callback(self._worker_tasks.discard)

        # slow 车道起一个背压监控：hot 池不足时按比例收缩 slow 并发，fast 车道不受影响
        if self.proxy_tier is ProxyTier.SLOW:
            self._backpressure_task = asyncio.create_task(
                self._proxy_backpressure_loop(slow_gate, slow_base_limit)
            )

        # 处理初始参数（仅当 init_params 不为 None 时才入队执行）
        if init_params is not None:
            seqId += 1
            await queue.put(worker_model)

        # 开始循环生成任务
        try:
            async for param in self.key_params_gen(init_params):
                worker_model = WorkerModel(params=param, seqId=seqId)
                seqId += 1
                self.log.debug(self.format_log(f"生成任务: {param}"))
                if await self.is_stop():
                    self.log.info(self.format_log("触发终止条件，停止生成新任务。"))
                    break

                # 修复：正确处理协程列表
                should_stop_results = await asyncio_gather(
                    *[x.should_stop_check() for x in self._plugins], log=self.log
                )
                if True in should_stop_results:
                    self.log.info(self.format_log("触发终止条件，停止生成新任务。"))
                    break

                if self._is_pause:
                    while self._is_pause:
                        await asyncio.sleep(10)

                # 智能背压控制：根据队列大小动态调整生成速度
                queue_size = queue.qsize()
                if queue_size >= worker_count:
                    # 队列达到max_sem限制，暂停生成直到有空间
                    while queue.qsize() >= worker_count:
                        await asyncio.sleep(0.1)

                await queue.put(worker_model)

        except Exception as e:
            self.log.exception(self.format_log(f"任务生成器异常: {e}"))

        self.log.info(
            self.format_log(
                f"任务生成完成。正在等待剩余任务完成，当前活跃任务数：{len(task_set)}，队列大小：{self.task_queue.qsize()}"
            )
        )

        # 等待队列中的所有任务被处理完毕。
        # 两个竞态要一起处理：
        # 1. join() 与失败任务重排入队之间：join 的等待 future 已被唤醒、但协程尚未恢复时，
        #    worker 仍可能 put 一个重排任务，此时 join 会直接返回而漏掉该任务；
        # 2. 延迟重入队：任务还在 sleep，尚未 put 回队列，join() 看不到它。
        # 因此 join 返回后要同时确认「队列为空」且「没有挂起的延迟重入队任务」。
        while True:
            await queue.join()
            if queue.empty() and not self._pending_requeues:
                break
            if not queue.empty():
                self.log.debug(
                    self.format_log(
                        f"join 后仍有 {queue.qsize()} 个任务，继续等待（重排竞态）。"
                    )
                )
            else:
                self.log.debug(
                    self.format_log(
                        f"还有 {len(self._pending_requeues)} 个任务在延迟重入队，继续等待。"
                    )
                )
                await asyncio.sleep(0.1)

        # 收尾：停掉背压监控
        if self._backpressure_task is not None:
            self._backpressure_task.cancel()
            await asyncio.gather(self._backpressure_task, return_exceptions=True)
            self._backpressure_task = None

        # 发送哨兵值通知所有 worker 退出
        # 发送 worker_count 个哨兵值，确保所有 worker 都能收到退出信号
        for _ in range(worker_count):
            await queue.put(None)

        # 等待所有活跃的 worker 任务完成
        if task_set:
            await asyncio.gather(*task_set, return_exceptions=True)

        self.log.info(self.format_log("所有任务已完成。"))

        await self.on_run_end(worker_model)

        self.log.info(self.format_log("run finished."))

        # 清理队列中可能残留的项目（理论上应该为空，因为所有worker已完成）
        while not queue.empty():
            await queue.get()

        # 收尾回收：正常路径 worker 已收到哨兵退出，这里兜底清理被 cancel 的个别 worker。
        await self._reap_leftover_workers()
