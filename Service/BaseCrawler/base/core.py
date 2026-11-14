import asyncio
from abc import ABC, abstractmethod
from typing import Generic

from log.base_log import myfastapi_logger
from Service.BaseCrawler.model.base import WorkerModel, ParamsType
from Utils.通用.Common import ConcurrencyLimit, sem_gen
from Utils.代理.mdoel.ProxyTier import ProxyTier


class BaseCrawler(ABC, Generic[ParamsType]):
    def __init__(self, max_sem: int = 10, _logger=myfastapi_logger):
        self.log = _logger
        self.max_sem: int = max_sem
        # 爬虫实例名称：日志前缀 / 告警推送统一使用它，而不是类名。
        # 一个爬虫类可能有多个实例（如 LotteryApiRobot 的 business_type=2 与 10），
        # 用类名会让两者的日志与推送完全无法区分。
        # 默认取类名，UnlimitedCrawler 会用 Config.crawler_name 覆盖，
        # 调度器创建时再回填为 BackgroundServiceName 的值。
        self.crawler_name: str = type(self).__name__

        # ===== fast / slow 双车道 =====
        # 两条队列 + 两套并发闸：
        # - fast 车道只跑「高价值、可快速失败」的任务，并发永不收缩；
        # - slow 车道跑「可容忍等待、允许用 cold 代理探测」的任务，
        #   代理 hot 池低于阈值时按比例收缩（见 UnlimitedCrawler 的背压循环）。
        # 爬虫用哪条车道由 CrawlerConfig.proxy_tier 声明；两条车道都建好，
        # 但只有声明的那条会真正挂 worker（另一条留空，不浪费协程）。
        self.proxy_tier: ProxyTier = ProxyTier.FAST
        self.fast_queue: asyncio.Queue[WorkerModel | None] = asyncio.Queue()
        self.slow_queue: asyncio.Queue[WorkerModel | None] = asyncio.Queue()
        # task_queue 保留为「当前生效车道的队列」别名，兼容既有引用
        self.task_queue: asyncio.Queue[WorkerModel | None] = self.fast_queue
        # sem 保留给历史引用（真正的并发控制走 fast_gate / slow_gate）
        self.sem = sem_gen(max_sem)
        # 并发闸实例在每轮 run() 里重建，避免上一轮取消后计数偏差污染新轮次；
        # 这两个属性是「最近一轮」的引用，仅供观测。
        self.fast_gate: ConcurrencyLimit = ConcurrencyLimit(max_sem)
        self.slow_gate: ConcurrencyLimit = ConcurrencyLimit(max_sem)
        self._is_pause: bool = False

    def format_log(self, msg: str) -> str:
        return f"[{self.crawler_name}] {msg}"

    @abstractmethod
    async def worker(self) -> WorkerModel:
        """
        里面丢一个自己实现的获取数据然后存进去的函数
        :return:
        """
        ...

    @abstractmethod
    async def run(self, *args, **kwargs): ...

    async def start(self):
        self.log.info("UnlimitedCrawler start method called.")
        self._is_pause = False

    async def pause(self):
        self.log.info("UnlimitedCrawler pause method called.")
        self._is_pause = True
