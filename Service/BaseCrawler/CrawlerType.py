from typing import Any, AsyncGenerator, Generic, List, ClassVar, TypeVar
from abc import abstractmethod
from datetime import datetime
import asyncio
import random
import time
from types import EllipsisType
from CONFIG import CONFIG
from log.base_log import myfastapi_logger
from Service.BaseCrawler.base.core import BaseCrawler
from Service.BaseCrawler.config import CrawlerConfig
from Service.BaseCrawler.model.base import WorkerModel, WorkerStatus, ParamsType
from Service.BaseCrawler.plugin.base import CrawlerPlugin
from Service.BaseCrawler.resilience import (
    AlertDecision,
    FailureAlertThrottle,
    FinalFailTally,
    ProxyCircuitBreaker,
    is_pool_level_error,
)
from Service.BaseCrawler.retry_queue import (
    RETRY_BATCH_LIMIT,
    CrawlFailedTaskDao,
    crawl_failed_task_dao,
)
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
    # 需要「至少一次」语义的爬虫声明自己的重试任务类型（见 Service/BaseCrawler/retry_queue.py）。
    # 空串 = 不参与：任务最终失败即丢弃（老行为），不做任何持久化。
    retry_task_type: ClassVar[str] = ""

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
        # 重入队上限：重试预算耗尽后「预算之外」的处置上限，
        # 没有它时代理池整体不可用会让同一批任务无限打回队列（26 条/分钟 ERROR）。
        self.max_requeue_per_task = _resolve(
            self.config.max_requeue_per_task, runtime.max_requeue_per_task
        )
        self.requeue_max_task_seconds = _resolve(
            self.config.requeue_max_task_seconds, runtime.requeue_max_task_seconds
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
        # 挂起任务 → 它携带的 WorkerModel：「杀旧开新」取消它们时要写回重试队列，
        # 否则这些「已取出、还没执行」的任务会随旧队列一起消失（丢数据）
        self._pending_requeue_models: dict[asyncio.Task, WorkerModel] = {}

        # ===== 故障收敛：代理池熔断 + 失败告警聚合 =====
        # 实例级（不是进程级）：每个爬虫各自熔断，互不牵连；
        # 跨轮次保留（调度器「杀旧开新」不复位），否则几秒一次的重启会让熔断形同虚设。
        self._breaker = ProxyCircuitBreaker(
            name=lambda: self.crawler_name,
            config=CONFIG.crawler_breaker,
            log=self.log,
        )
        self._alert_throttle = FailureAlertThrottle(config=CONFIG.crawler_alert)
        # 本轮「达到重入队上限」的最终失败计数（run 开始时重置）
        self._final_fail_tally = FinalFailTally()

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

    # ===== 「至少一次」重试队列（机制，不含任何业务判断） =====
    # 参与条件：子类声明 retry_task_type 并实现 retry_business_id / retry_params /
    # on_task_final_fail。未声明（空串）的爬虫完全走老行为，不产生任何额外 IO。

    @property
    def _retry_dao(self) -> CrawlFailedTaskDao:
        return crawl_failed_task_dao

    def retry_business_id(self, params: ParamsType | Any | None) -> int | None:
        """从任务参数里取「业务 id」（重试队列的主键之一）。默认不参与。"""
        return None

    def retry_params(self, business_id: int) -> ParamsType | None:
        """把重试队列里的业务 id 还原成本爬虫的任务参数。默认不参与。"""
        return None

    async def on_task_final_fail(self, worker_model: WorkerModel):
        """任务**最终失败**（判定为不重新入队）时的附加钩子。

        默认空实现。注意：**落库已在基类 ``_settle_retry_task`` 统一完成**
        （每次失败即 upsert pending），所以这里不需要再写库；
        需要「最终失败时额外做点什么」（比如单独告警）的爬虫才覆盖它。
        """
        return None

    async def _upsert_retry_task(self, worker_model: WorkerModel) -> bool:
        """把任务写回重试队列（status=pending，立即到期）。返回是否真的写入了。"""
        if not self.retry_task_type:
            return False
        business_id = self.retry_business_id(worker_model.params)
        if business_id is None:
            return False
        await self._retry_dao.mark_pending(
            self.retry_task_type,
            business_id,
            last_error=(
                f"{self.crawler_name} 失败：status={worker_model.fetchStatus.name} "
                f"retry_count={worker_model.retry_count}"
            ),
        )
        return True

    async def _settle_retry_task(
        self, worker_model: WorkerModel, should_requeue: bool
    ) -> None:
        """任务结束后同步重试队列状态（成功 done / **失败立即落 pending**）。

        口径（用户明确要求）：**重试发生在本轮**，不是等下一轮。
        - 成功（complete / nullData）→ ``done``：清掉历史 pending，游标才能推进；
        - 失败（fail / timeout）→ **先落 pending 再做本轮重入队**：
          写库只是「进程重启 / 杀旧开新」的持久化兜底，不是重试通道；
          本轮由 ``_handle_requeue`` 立刻延迟重入队继续试，
          直到重入队上限（``max_requeue_per_task``）或累计时长上限才真正留给下一轮。
        - 达上限后库里保持 pending，下一轮 ``load_retry_params`` 接着跑（永不丢弃）。

        先落库再重入队：即使进程在重入队前被杀，库里也有记录。
        任何 DB 异常只记日志，不影响 worker 本轮重试。
        """
        if not self.retry_task_type:
            return
        business_id = self.retry_business_id(worker_model.params)
        if business_id is None:
            return
        status = worker_model.fetchStatus
        try:
            if status in (WorkerStatus.complete, WorkerStatus.nullData):
                # 成功写入：清掉历史 pending（补数 / 上一轮遗留），游标才能推进
                if await self._retry_dao.mark_done(self.retry_task_type, business_id):
                    self.log.debug(
                        self.format_log(f"重试任务已完成并从队列摘除：{business_id}")
                    )
                return
            if status in (WorkerStatus.fail, WorkerStatus.timeoutError):
                # 失败即落库：本轮会继续重试，库里的记录保证「杀旧开新 / 重启」不丢
                await self._upsert_retry_task(worker_model)
                if not should_requeue:
                    self.log.warning(
                        self.format_log(
                            f"任务本轮重试用尽（{business_id}），已留在重试队列 pending，"
                            "下一轮开头继续补跑"
                        )
                    )
                    await self.on_task_final_fail(worker_model)
        except Exception as e:  # noqa: BLE001 记账失败不能拖垮爬虫主循环
            self.log.exception(
                self.format_log(f"同步重试队列失败（不影响本任务）：{e!r}")
            )

    async def load_retry_params(
        self, limit: int = RETRY_BATCH_LIMIT
    ) -> List[ParamsType]:
        """取**本轮开头**要补跑的任务参数（pending 且到了重试时间，按 task_type 过滤）。

        由各爬虫 ``main()`` 在 ``run()`` 之前调用，并通过 ``run(pre_params=...)``
        入队执行。语义说明：**本轮内的重试不靠它**（失败后 ``_settle_retry_task``
        先落库、``_handle_requeue`` 立刻重入队）；这里只负责接上
        「上一轮用尽 / 进程重启 / 杀旧开新」遗留的 pending。
        """
        if not self.retry_task_type:
            return []
        try:
            tasks = await self._retry_dao.fetch_pending(self.retry_task_type, limit=limit)
            stats = await self._retry_dao.get_stats(self.retry_task_type)
        except Exception as e:  # noqa: BLE001 读不到队列时按「没有待补任务」处理
            self.log.exception(
                self.format_log(f"读取重试队列失败，本轮跳过补跑：{e!r}")
            )
            return []
        params: List[ParamsType] = []
        for task in tasks:
            param = self.retry_params(task.business_id)
            if param is not None:
                params.append(param)
        if tasks:
            self.log.warning(
                self.format_log(
                    f"重试队列[{self.retry_task_type}]：本轮补跑 {len(params)} 条"
                    f"（上限 {limit}）；{stats.to_log_str()}"
                )
            )
            if stats.pending > len(params):
                self.log.warning(
                    self.format_log(
                        f"重试队列仍有 {stats.pending - len(params)} 条待补任务未进本轮，"
                        "下一轮继续（不会丢弃）"
                    )
                )
        return params

    async def pending_min_business_id(self) -> int | None:
        """该爬虫最小的待重试业务 id；``None`` = 没有 pending（或未参与 / 读库失败）。

        游标推进必须用它做闸门：只有「最小 pending 也大于当前 id」时才能推进水位，
        否则失败 id 会被后续成功越过，形成永久缺口（这就是 2026-11-16 丢 8 条 rid
        的原因：断点只记「最后成功的 id」）。
        """
        if not self.retry_task_type:
            return None
        try:
            return await self._retry_dao.min_pending_business_id(self.retry_task_type)
        except Exception as e:  # noqa: BLE001 读不到时按「无 pending」处理（保守但不阻塞抓取）
            self.log.exception(
                self.format_log(f"读取重试队列最小 pending 失败：{e!r}")
            )
            return None

    async def can_advance_cursor(self, business_id: int) -> bool:
        """游标能否推进到 ``business_id``：该 id 及之前没有 pending 才允许。"""
        pending_min = await self.pending_min_business_id()
        if pending_min is not None and pending_min <= business_id:
            self.log.debug(
                self.format_log(
                    f"游标暂不推进到 {business_id}：重试队列还有更早的 "
                    f"pending {pending_min} 未写入成功"
                )
            )
            return False
        return True

    async def _persist_leftover_worker(self, worker_model: WorkerModel | None) -> bool:
        """把「已从队列取出但没执行完」的任务写回重试队列（杀旧开新 / 进程重启兜底）。"""
        if worker_model is None or worker_model.params is None:
            return False
        try:
            return await self._upsert_retry_task(worker_model)
        except Exception as e:  # noqa: BLE001 兜底失败只记日志
            self.log.exception(
                self.format_log(f"残留任务写回重试队列失败：{e!r}")
            )
            return False

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

                # 至少一次语义：**先落库，再决定本轮重入队**。
                # 失败 → 立刻 upsert pending（进程被杀也不丢），然后本轮继续重试；
                # 成功 → done。都放在并发额度之外做 IO，不占 worker 槽位。
                await self._settle_retry_task(worker_model, should_requeue)

                if should_requeue:
                    self.log.info(
                        self.format_log(
                            f"任务本轮重试（第 {worker_model.retry_count + 1} 次）："
                            f"{worker_model.params}"
                        )
                    )
                    await self._handle_requeue(worker_model, queue)
            except asyncio.CancelledError:
                # 杀旧开新：当前任务已取出但没执行完，写回重试队列（不能静默丢弃）。
                # shield 保证取消风暴中落库协程仍能跑完；失败只记日志，不阻断取消传播。
                await asyncio.shield(self._persist_leftover_worker(worker_model))
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
                await self._settle_retry_task(worker_model, should_requeue=False)
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
        """执行 fetch 操作，处理超时和异常，返回任务状态。

        代理池整体不可用时（熔断开启）先 ``await`` 退避到可用为止，**继续取数**
        只会制造失败风暴；退避结束后再走正常流程。
        """
        # 熔断退避：必须 await（禁止 time.sleep），期间不产生任何上游请求
        await self._breaker.wait_until_ready()
        try:
            async with asyncio.timeout(self.worker_max_timeout):
                fetch_result = await self.handle_fetch(worker_model.params)
        except asyncio.TimeoutError:
            self._breaker.record_failure("爬取超时", pool_level=False)
            if self.log_timeout_error:
                self.log.exception(
                    self.format_log(f"爬取超时：{self.worker_max_timeout}s")
                )
            return WorkerStatus.timeoutError
        except Exception as e:
            pool_level = is_pool_level_error(e)
            self._breaker.record_failure(f"{type(e).__name__}: {e}", pool_level=pool_level)
            if self.log_error:
                self.log.exception(self.format_log(f"爬取异常：{e}"))
                await self._push_failure_alert(worker_model, e)
            # 熔断已开启时不再叠加 worker_error_delay：
            # 退避等待本身就是冷却，双重等待会让任务时长无谓翻倍。
            if not self._breaker.is_open:
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
            self._breaker.record_success()
            return WorkerStatus.complete
        if not isinstance(fetch_result, WorkerStatus):
            self._breaker.record_success()
            return WorkerStatus.complete
        if fetch_result in (WorkerStatus.complete, WorkerStatus.nullData):
            self._breaker.record_success()
        elif fetch_result in (WorkerStatus.fail, WorkerStatus.timeoutError):
            # handle_fetch 内部吞掉异常、直接返回失败状态：同样计入熔断口径
            self._breaker.record_failure(
                f"fetch_status={fetch_result.name}", pool_level=False
            )
        return fetch_result

    async def _push_failure_alert(
        self, worker_model: WorkerModel, err: BaseException
    ) -> None:
        """失败告警：按「爬虫 + 时间窗口」聚合，避免失败风暴打爆推送通道。

        content 里只放**稳定可去重**的信息（爬虫名 / 计数 / 异常类型），
        具体 params 与堆栈已写本地日志 —— message-service 才能聚合成一条摘要。
        """
        decision: AlertDecision = self._alert_throttle.hit()
        if not decision.push:
            self.log.debug(
                self.format_log(
                    f"失败告警已聚合（窗口 {decision.window_seconds:.0f}s 内 "
                    f"{decision.count} 次，暂不推送）：{type(err).__name__}"
                )
            )
            return
        aggregated = (
            f"，其中 {decision.suppressed} 次已聚合省略" if decision.suppressed else ""
        )
        tag = "窗口汇总" if decision.aggregated else "首次告警"
        await a_push_error(
            subject="运行异常",
            content=(
                f"爬虫[{self.crawler_name}] 失败{tag}：窗口 {decision.window_seconds:.0f}s 内 "
                f"{decision.count} 次失败{aggregated}；最近一次异常类型 "
                f"{type(err).__name__}；熔断状态 {self._breaker.state.value}"
                f"（明细见本地日志）"
            ),
        )

    def _requeue_limit_reason(
        self, worker_model: WorkerModel
    ) -> tuple[str, str] | None:
        """重入队上限判定：返回 ``(原因代码, 人话说明)``，未到上限返回 ``None``。

        三道上限取**最先到的一条**：

        - ``max_retries``：历史语义，-1 表示不限；
        - ``max_requeue_per_task``：预算耗尽后「预算之外」的处置上限（新增）；
        - ``requeue_max_task_seconds``：单个任务的累计时长上限（新增）。

        注意：``retry_count`` 只在 ``_handle_requeue`` 中自增一次，这里读到的是
        「已经重入队过多少次」，因此 ``>=`` 判定刚好允许 cap 次重入队。
        """
        retries = max(0, int(worker_model.retry_count))
        if self.max_retries >= 0 and retries >= self.max_retries:
            return "max_retries", f"已达到最大重试次数({self.max_retries})"
        requeue_cap = int(self.max_requeue_per_task)
        if requeue_cap >= 0 and retries >= requeue_cap:
            return "requeue_cap", f"重入队次数已达上限({requeue_cap})"
        duration_cap = float(self.requeue_max_task_seconds or 0)
        if duration_cap > 0:
            elapsed = max(
                0.0, (datetime.now() - worker_model.created_at).total_seconds()
            )
            if elapsed >= duration_cap:
                return (
                    "duration_cap",
                    f"任务累计时长 {elapsed:.0f}s 达上限({duration_cap:.0f}s)",
                )
        return None

    def _should_requeue(self, worker_model: WorkerModel) -> bool:
        """判断任务是否需要重试，检查状态和重入队上限。"""
        status = worker_model.fetchStatus
        if status == WorkerStatus.fail and not self.requeue_on_fetch_fail:
            return False
        if status == WorkerStatus.timeoutError and not self.requeue_on_timeout:
            return False
        if status not in (WorkerStatus.fail, WorkerStatus.timeoutError):
            return False

        limit = self._requeue_limit_reason(worker_model)
        if limit is None:
            return True

        code, text = limit
        total = self._final_fail_tally.add(code)
        # 每个达标任务只落一条汇总 WARNING（不再无限打回队列 → 不再无限刷屏）。
        # 注意：这里的「用尽」只针对**本轮**——任务已先落 crawl_failed_task pending，
        # 下一轮开头 load_retry_params 会继续补跑，不会丢。
        self.log.warning(
            self.format_log(
                f"任务本轮重试用尽（{text}），已留在重试队列待下一轮补跑："
                f"{worker_model.params}；本轮累计 {self._final_fail_tally.to_log_str()}"
            )
        )
        if total % 20 == 0:
            self.log.warning(
                self.format_log(
                    f"本轮重试用尽的任务持续增长：{self._final_fail_tally.to_log_str()}"
                )
            )
        return False

    async def _flush_failure_alert(self) -> None:
        """运行结束时把失败告警窗口里积压的数量补一条汇总推送。"""
        decision = self._alert_throttle.flush()
        if decision is None:
            return
        await a_push_error(
            subject="运行异常",
            content=(
                f"爬虫[{self.crawler_name}] 失败窗口汇总：窗口 "
                f"{decision.window_seconds:.0f}s 内共 {decision.count} 次失败"
                f"（{decision.suppressed} 次已聚合省略）；本轮最终失败 "
                f"{self._final_fail_tally.total} 个；熔断状态 "
                f"{self._breaker.state.value}（明细见本地日志）"
            ),
        )

    def _log_final_fail_summary(self) -> None:
        """运行结束时落一条汇总（有最终失败才打，正常跑完不产生噪音）。"""
        if self._final_fail_tally.total <= 0:
            return
        self.log.warning(
            self.format_log(
                f"本轮最终失败汇总：{self._final_fail_tally.to_log_str()}；"
                f"熔断快照 {self._breaker.snapshot().model_dump(mode='json')}"
            )
        )

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
        """任务失败后重新入队（在并发额度之外执行，避免死锁）。

        queue 必须与 worker 处理该任务时使用的是同一个队列实例，
        否则「杀旧开新」后重排的任务会落进新一轮的队列。

        **「至少一次」爬虫（声明了 ``retry_task_type``）：无条件立即插到队首**，
        不经过任何退避计算 —— 业务数据有连贯性（前面的 id 缺失会影响后面的结束
        判断），而且队列可能积压几千条（生产者不限速），放队尾等于「拖到最后」。
        非「至少一次」的爬虫保留老的「指数退避 + 独立 task」逻辑（防止忙循环）。
        """
        if queue is None:
            queue = self.task_queue
        worker_model.fetchStatus = WorkerStatus.pending
        worker_model.retry_count += 1
        await self.on_task_requeue(worker_model)
        if self.retry_task_type:
            # 马上重试：插队首，下一个空闲 worker 立刻取到它
            self._put_front(queue, worker_model)
            return
        delay = self._requeue_delay(worker_model.retry_count)
        if delay <= 0:
            self._put_front(queue, worker_model)
            return
        task = asyncio.create_task(
            self._delayed_requeue(queue, worker_model, delay)
        )
        self._pending_requeues.add(task)
        # 记下它携带的任务：「杀旧开新」取消这些挂起任务时要把 WorkerModel 写回重试队列
        self._pending_requeue_models[task] = worker_model
        task.add_done_callback(self._pending_requeues.discard)
        task.add_done_callback(self._pending_requeue_models.pop)

    @staticmethod
    def _put_front(
        queue: "asyncio.Queue[WorkerModel | None]", worker_model: WorkerModel
    ) -> None:
        """把任务插到队首（``asyncio.Queue`` 没有公开 API，这里直接维护内部状态）。

        与 ``queue.put()`` 等价的部分：入队元素、``_unfinished_tasks += 1``、
        清掉 ``join()`` 的完成标记、唤醒一个正在 ``get()`` 的 worker。
        之所以不用「自己实现 deque 队列」：``join()`` / ``task_done()`` /
        ``_unfinished_tasks`` 的语义在 ``run()`` 收尾逻辑里已经被依赖（见那里的竞态注释），
        换队列实现的风险远大于用这三个私有字段。
        """
        queue._queue.appendleft(worker_model)  # noqa: SLF001 asyncio.Queue 无 put_front
        queue._unfinished_tasks += 1  # noqa: SLF001
        queue._finished.clear()  # noqa: SLF001
        queue._wakeup_next(queue._getters)  # noqa: SLF001

    async def _delayed_requeue(
        self,
        queue: asyncio.Queue[WorkerModel | None],
        worker_model: WorkerModel,
        delay: float,
    ):
        await asyncio.sleep(delay)
        # 延迟重试同样优先于「还没执行的新任务」
        self._put_front(queue, worker_model)

    async def _reap_leftover_workers(self):
        """回收上一轮残留的 worker 协程与「延迟重入队」任务。

        「杀旧开新」只 cancel 了 run()/main() 所在的协程，worker 是独立 task，
        不会随之结束：它们会阻塞在旧队列的 get() 上，并继续占用并发额度。
        新一轮 run() 开始时统一 cancel 掉，避免协程与额度泄漏。
        """
        leftover = [t for t in self._worker_tasks if not t.done()]
        pending = [t for t in self._pending_requeues if not t.done()]
        if not leftover and not pending:
            return
        if leftover:
            self.log.warning(
                self.format_log(f"发现上一轮残留 worker {len(leftover)} 个，已回收。")
            )
        if pending:
            self.log.warning(
                self.format_log(
                    f"发现上一轮残留的延迟重入队任务 {len(pending)} 个，"
                    "已取消并写回重试队列。"
                )
            )
        # 取消前先取走它们携带的任务：「延迟重入队」里 sleep 的任务还没进队列，
        # 直接 cancel 就等于丢任务（代理故障期间这里每天都在丢）。
        leftover_models = [
            self._pending_requeue_models.get(task) for task in pending
        ]
        for task in leftover + pending:
            task.cancel()
        await asyncio.gather(*leftover, *pending, return_exceptions=True)
        for model in leftover_models:
            await self._persist_leftover_worker(model)
        self._worker_tasks.clear()
        self._pending_requeues.clear()
        self._pending_requeue_models.clear()

    def _slow_lane_base_limit(self) -> int:
        """slow 车道基准并发 = ``max_sem × slow_lane_ratio``（至少 1）。"""
        ratio = self.slow_lane_ratio if self.slow_lane_ratio > 0 else 1.0
        return max(1, int(round(self.max_sem * ratio)))

    async def _persist_leftover_queue_tasks(self) -> int:
        """把上一轮队列里**没执行完**的任务写回重试队列，而不是静默丢弃。

        背景：调度器「杀旧开新」直接 cancel 上一轮 run()，队列里的任务随旧队列
        一起消失（2026-11-16 丢数据的直接原因之一）。这里把它们按业务 id
        upsert 成 pending，下一轮 ``load_retry_params`` 会重新取到并重跑。
        未声明 ``retry_task_type`` 的爬虫保持老行为（丢弃 + WARNING）。
        """
        old_queue = self.task_queue
        models: list[WorkerModel] = []
        while not old_queue.empty():
            item = old_queue.get_nowait()
            old_queue.task_done()
            if item is None:
                continue
            models.append(item)
        if not models:
            return 0
        if not self.retry_task_type:
            self.log.warning(
                self.format_log(
                    f"发现上一轮残留任务 {len(models)} 条，已丢弃并重建队列"
                    f"（未声明 retry_task_type，无持久化兜底）。"
                )
            )
            return 0
        persisted = 0
        for model in models:
            if await self._persist_leftover_worker(model):
                persisted += 1
        self.log.warning(
            self.format_log(
                f"发现上一轮残留任务 {len(models)} 条，已写回重试队列 "
                f"{persisted} 条并重建队列。"
            )
        )
        return persisted

    async def run(
        self,
        init_params: ParamsType | None = None,
        pre_params: List[ParamsType] | None = None,
    ):
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
            pre_params: 本轮要**优先补跑**的任务参数（来自 ``crawl_failed_task`` 的
                pending 任务，见 ``load_retry_params``）。它们在任务生成器之前入队，
                失败会继续留在重试队列里，不依赖游标。

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

        # 本轮统计复位：最终失败计数 / 告警聚合窗口。
        # 熔断状态**不**复位 —— 「杀旧开新」几秒一次，复位会让熔断形同虚设。
        self._final_fail_tally = FinalFailTally()
        self._alert_throttle.reset()

        seqId = 0
        worker_model = WorkerModel(params=init_params, seqId=seqId)

        # 先回收上一轮残留的 worker：调度器"杀旧开新"只 cancel 了 run() 所在协程，
        # worker 是独立 task，会继续阻塞在旧队列上。
        await self._reap_leftover_workers()

        # 重建任务队列：爬虫实例是复用的（调度器"杀旧开新"会 cancel 上一轮），
        # 上一轮被中断时队列中可能残留未执行任务、且 unfinished_tasks 计数可能不为 0
        # （cancel 打断在 get() 与 task_done() 之间时），残留会导致本轮统计被污染、
        # 甚至下一轮 join() 永久等待。
        # 残留任务**不能静默丢弃**：先按业务 id 写回重试队列，下一轮会重新取到。
        await self._persist_leftover_queue_tasks()

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

        # 代理背压（按 hot/warm 分档计数收缩 slow 并发）已停用：
        # 取免费池回退到「随机一条 status 可用」的老语义后，hot/warm 不再代表
        # 「可用量」，按它收缩并发只会误伤正常抓取（老版本也没有这个机制）。

        # 处理初始参数（仅当 init_params 不为 None 时才入队执行）
        if init_params is not None:
            seqId += 1
            await queue.put(worker_model)

        # 先跑「上一轮没写成功」的补跑任务：它们不依赖游标，必须优先执行，
        # 否则游标会一直停在最小 pending 之前（见各爬虫 retry_task_type 声明）。
        for param in pre_params or []:
            self.log.info(self.format_log(f"补跑重试任务: {param}"))
            await queue.put(WorkerModel(params=param, seqId=seqId))
            seqId += 1

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
        #
        # 关键修复（2026-11 线上「启动后 HTTP 无响应 / CPU 100%」）：
        # ``queue.join()`` 在 ``_unfinished_tasks == 0`` 时**立即返回且不 await**。
        # 旧写法在「队列非空但 unfinished=0」（重排竞态窗口）那一条分支里只有 log.debug，
        # 整个 while 体一次 await 都没有 → 忙等把事件循环（含 uvicorn）彻底饿死 4.5 分钟。
        # 现在保证**每一轮循环都至少 await 一次**，并对日志做节流。
        last_wait_log_ts = 0.0
        while True:
            if queue.empty() and not self._pending_requeues:
                break
            unfinished = getattr(queue, "_unfinished_tasks", 0)
            if unfinished > 0:
                # 真正有未完成任务：join 会挂起等待，不消耗 CPU
                await queue.join()
            else:
                # 队列非空但 unfinished=0（重排竞态）：必须让出事件循环
                await asyncio.sleep(0.05)
            if time.monotonic() - last_wait_log_ts >= 5:
                last_wait_log_ts = time.monotonic()
                self.log.debug(
                    self.format_log(
                        f"收尾等待中：队列 {queue.qsize()} 个，"
                        f"延迟重入队 {len(self._pending_requeues)} 个，"
                        f"未完成计数 {unfinished}。"
                    )
                )

        # 发送哨兵值通知所有 worker 退出
        # 发送 worker_count 个哨兵值，确保所有 worker 都能收到退出信号
        for _ in range(worker_count):
            await queue.put(None)

        # 等待所有活跃的 worker 任务完成
        if task_set:
            await asyncio.gather(*task_set, return_exceptions=True)

        self.log.info(self.format_log("所有任务已完成。"))

        await self.on_run_end(worker_model)

        # 收尾：最终失败汇总 + 告警窗口积压补推（有积压才推）
        self._log_final_fail_summary()
        await self._flush_failure_alert()

        self.log.info(self.format_log("run finished."))

        # 清理队列中可能残留的项目（理论上应该为空，因为所有worker已完成）
        while not queue.empty():
            await queue.get()

        # 收尾回收：正常路径 worker 已收到哨兵退出，这里兜底清理被 cancel 的个别 worker。
        await self._reap_leftover_workers()
