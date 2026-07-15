from typing import Any, AsyncGenerator, Generic, List, ClassVar, TypeVar
from abc import abstractmethod
import asyncio
from types import EllipsisType
from CONFIG import CONFIG
from Service.BaseCrawler.base.core import BaseCrawler
from Service.BaseCrawler.config import CrawlerConfig
from Service.BaseCrawler.model.base import WorkerModel, WorkerStatus, ParamsType
from Service.BaseCrawler.plugin.base import CrawlerPlugin
from Utils.通用.Common import asyncio_gather
from Utils.推送.PushMe import a_push_error

P = TypeVar("P", bound=CrawlerPlugin)


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
        若需要在运行时注入动态值（如根据业务类型选择 logger / max_sem），
        子类可重写本方法，返回 ``CONFIG.get_crawler_config(self.Config).model_copy(update=...)``。
        """
        return CONFIG.get_crawler_config(self.Config)

    def __init__(self):
        """
        初始化无限爬虫。

        所有运行参数（max_sem、logger、plugins 等）均来自 ``self.Config`` 对应的配置实例，
        不再接受任何外部传参。各插件会按 Config 中声明的 ``plugin_name`` 自动绑定到
        ``self.<plugin_name>``，子类直接用该属性即可（也可通过 ``self.get_plugin(PluginType)`` 查找）。
        """
        # 直接使用 self.Config 对应的集中配置实例（由 CONFIG 注入）
        self.config = self._load_config()

        self.requeue_on_fetch_fail = self.config.requeue_on_fetch_fail
        self.requeue_on_timeout = self.config.requeue_on_timeout
        self.max_retries = self.config.max_retries
        self.worker_max_timeout = self.config.worker_max_timeout
        self.log_timeout_error = self.config.log_timeout_error
        self.log_error = self.config.log_error
        self.worker_error_delay = self.config.worker_error_delay

        super().__init__(max_sem=self.config.max_sem, _logger=self.config.logger)

        self._plugins = []
        for plugin_cfg in self.config.plugins:
            plugin = plugin_cfg.plugin_cls(crawler=self)
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
    async def handle_fetch(self, params: ParamsType | None) -> WorkerStatus | Any:
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
        - 此时任务状态已设置为 pending
        - 修改 worker_model.params 会影响重试时的任务参数
        """
        worker_model.retry_count += 1

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

    async def worker(self):
        """
        Worker 协程，固定池模式，持续从队列中获取任务并处理

        工作流程：
        1. 从任务队列获取一个任务（如果收到 None 则退出）
        2. 获取信号量（控制并发数）
        3. 触发 on_worker_start 回调
        4. 调用 handle_fetch 处理任务
        5. 处理异常情况
        6. 更新任务状态
        7. 触发 on_worker_end 回调
        8. 释放信号量
        9. 如果任务失败且需要重试，在信号量作用域外重新入队（避免死锁）
        10. 循环回到步骤 1

        重要：重新入队的操作必须在 async with self.sem 作用域外执行，
        否则当 max_sem=1 时可能导致死锁。
        """
        while True:
            worker_model: WorkerModel | None = await self.task_queue.get()
            if worker_model is None:
                self.log.debug(self.format_log("收到退出信号，退出任务处理。"))
                self.task_queue.task_done()
                return

            should_requeue = False

            async with self.sem:
                try:
                    self.log.debug(self.format_log(f"开始处理任务: {worker_model.params}"))
                    await self.on_worker_start(worker_model)
                    worker_model.fetchStatus = await self._execute_fetch(worker_model)
                    self.log.debug(self.format_log(f"完成任务状态: {worker_model.fetchStatus}"))
                    should_requeue = self._should_requeue(worker_model)
                finally:
                    self.task_queue.task_done()

            if should_requeue:
                self.log.debug(self.format_log(f"任务需要重试: {worker_model.params}"))
                await self._handle_requeue(worker_model)

            await self.on_worker_end(worker_model)

    async def _execute_fetch(self, worker_model: WorkerModel) -> WorkerStatus:
        """执行 fetch 操作，处理超时和异常，返回任务状态。"""
        try:
            async with asyncio.timeout(self.worker_max_timeout):
                fetch_result = await self.handle_fetch(worker_model.params)
        except asyncio.TimeoutError:
            if self.log_timeout_error:
                self.log.exception(self.format_log(f"爬取超时：{self.worker_max_timeout}s"))
            return WorkerStatus.timeoutError
        except Exception as e:
            if self.log_error:
                self.log.exception(self.format_log(f"爬取异常：{e}"))
                await a_push_error(
                    subject="运行异常",
                    content=f"爬取任务[{self.__class__.__name__}]异常\n{worker_model}\n{e}",
                )
            await asyncio.sleep(self.worker_error_delay)
            return WorkerStatus.fail

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

        if self.max_retries < 0 or worker_model.retry_count < self.max_retries:
            worker_model.retry_count += 1
            return True

        self.log.warning(
            self.format_log(
                f"任务已达到最大重试次数({self.max_retries})，不再重试：{worker_model.params}"
            )
        )
        return False

    async def _handle_requeue(self, worker_model: WorkerModel):
        """将任务重新入队（在信号量作用域外执行，避免死锁）。"""
        worker_model.fetchStatus = WorkerStatus.pending
        await self.on_task_requeue(worker_model)
        await self.task_queue.put(worker_model)

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
        self.log.info(self.format_log(
            f"starting with init_params: {init_params}"))

        seqId = 0
        worker_model = WorkerModel(params=init_params, seqId=seqId)
        await asyncio_gather(
            *[x.on_run_start(worker_model) for x in self._plugins], log=self.log
        )
        
        # 创建固定数量的 worker 池
        # 确保 worker 数量不超过信号量大小，也不超过一个合理上限
        worker_count = self.max_sem
        
        task_set = set()
        
        # 启动固定数量的 worker
        for _ in range(worker_count):
            task = asyncio.create_task(self.worker())
            task_set.add(task)
            task.add_done_callback(task_set.discard)
        
        # 处理初始参数（仅当 init_params 不为 None 时才入队执行）
        if init_params is not None:
            seqId += 1
            await self.task_queue.put(worker_model)
            
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
                queue_size = self.task_queue.qsize()
                if queue_size >= worker_count:
                    # 队列达到max_sem限制，暂停生成直到有空间
                    while self.task_queue.qsize() >= worker_count:
                        await asyncio.sleep(0.1)
                
                await self.task_queue.put(worker_model)
                
        except Exception as e:
            self.log.exception(self.format_log(f"任务生成器异常: {e}"))
        
        self.log.info(self.format_log(
            f"任务生成完成。正在等待剩余任务完成，当前活跃任务数：{len(task_set)}，队列大小：{self.task_queue.qsize()}"
        ))
        
        # 等待队列中的所有任务被处理完毕
        await self.task_queue.join()
        
        # 发送哨兵值通知所有 worker 退出
        # 发送 worker_count 个哨兵值，确保所有 worker 都能收到退出信号
        for _ in range(worker_count):
            await self.task_queue.put(None)
        
        # 等待所有活跃的 worker 任务完成
        if task_set:
            await asyncio.gather(*task_set, return_exceptions=True)
        
        self.log.info(self.format_log("所有任务已完成。"))

        await self.on_run_end(worker_model)

        self.log.info(self.format_log("run finished."))
        
        # 清理队列中可能残留的项目（理论上应该为空，因为所有worker已完成）
        while not self.task_queue.empty():
            await self.task_queue.get()
