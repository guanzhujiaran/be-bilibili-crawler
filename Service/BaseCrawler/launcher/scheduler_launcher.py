# -*- coding: utf-8 -*-
import time
from datetime import datetime
from typing import Literal, Optional, AsyncGenerator, Any
from apscheduler.triggers.cron import CronTrigger
from loguru import logger as default_logger
from Models.base.custom_pydantic import CustomBaseModelHashable, CustomBaseModel
from Service.BaseCrawler.CrawlerType import UnlimitedCrawler
from Service.BaseCrawler.config import CrawlerConfig
from Service.BaseCrawler.model.base import ParamsType, WorkerStatus
from Utils.通用.Common import GLOBAL_SCHEDULER
from Utils.通用.Tool import format_duration
from Utils.推送 import push_switch
from Utils.推送.PushMe import (
    PushSubject,
    a_push_error,
    a_pushme,
    async_pushme_try_catch_decorator,
)
import asyncio
from typing import Callable
from dao.commStorageRedisObj import comm_storage_redis_obj


class CrawlerExecutionInfoModel(CustomBaseModel):
    crawler_name: str
    default_interval_seconds: int
    last_exec_time: datetime | None = None


class CrawlerExecutionInfo:
    info: CrawlerExecutionInfoModel

    def __init__(
        self,
        crawler_name: str,
        default_interval_seconds: int = 2 * 3600,
        logger=default_logger,
    ):
        self.logger = logger
        self.info = CrawlerExecutionInfoModel(
            crawler_name=crawler_name,
            default_interval_seconds=default_interval_seconds,
        )
        self._exec_info_redis_key = f"{self.info.crawler_name}_last_exec_time.txt"

    async def load_last_exec_time(self):
        if self.info.last_exec_time is None:
            ts_str = await comm_storage_redis_obj.get_val(self._exec_info_redis_key)
            if ts_str:
                self.info.last_exec_time = datetime.fromtimestamp(float(ts_str))
                self.logger.info(
                    f"[{self.info.crawler_name}] 加载上次执行时间：{self.info.last_exec_time}"
                )
            else:
                self.logger.info(
                    f"[{self.info.crawler_name}] 未找到上次执行时间，使用默认值。"
                )
                self.info.last_exec_time = datetime.fromtimestamp(
                    86400
                )  # 默认时间点：1970-01-02 00:00:00

    async def save_last_exec_time(self):
        now = datetime.now()
        self.info.last_exec_time = now
        await comm_storage_redis_obj.set_val(
            self._exec_info_redis_key, str(now.timestamp())
        )

    async def is_need_to_execute(self) -> bool:
        """判断是否需要执行爬虫"""
        await self.load_last_exec_time()
        if self.info.last_exec_time is None:
            self.logger.info(
                f"[{self.info.crawler_name}] 上次执行时间为空，将执行一次。"
            )
            return True
        now = datetime.now()
        delta = (now - self.info.last_exec_time).total_seconds()
        if delta >= self.info.default_interval_seconds:
            self.logger.info(f"[{self.info.crawler_name}] 满足执行条件，delta={delta}s")
            return True
        else:
            self.logger.info(
                f"[{self.info.crawler_name}] 不满足执行条件，delta={delta}s"
            )
            return False


class BaseScheduler:
    """
    定时任务调度器
    """

    def __init__(
        self,
        func: Callable,
        cron_expr: str,
        default_interval_seconds: int = 2 * 3600,
        crawler_name: str = "",
        logger=default_logger,
    ):
        self.func = func
        self.crawler_asyncio_task = None
        self.cron_expr = cron_expr
        self.job_id = f"crawler_job_{crawler_name}"
        self.logger = logger
        # 初始化执行信息管理器
        self.exec_info = CrawlerExecutionInfo(
            crawler_name=crawler_name,
            default_interval_seconds=default_interval_seconds,
            logger=self.logger,
        )

        # 构建cron trigger
        minute, hour, day, month, day_of_week = cron_expr.split()
        self.trigger = CronTrigger(
            minute=minute, hour=hour, day=day, month=month, day_of_week=day_of_week
        )
        # 添加或更新任务
        self._add_or_update_job()

    def _add_or_update_job(self):
        job = GLOBAL_SCHEDULER.get_job(self.job_id)
        if job:
            GLOBAL_SCHEDULER.reschedule_job(
                job_id=self.job_id,
                trigger=self.trigger,
            )
        else:
            # 强制首次任务立即执行
            GLOBAL_SCHEDULER.add_job(
                self.run,
                name=self.job_id,
                trigger=self.trigger,
                id=self.job_id,
                next_run_time=datetime.now(),  # 立即执行第一次
                coalesce=True,  # 错过的任务合并为一次
                max_instances=1,  # 同时最多允许一个实例
                misfire_grace_time=24 * 3600,  # 允许延迟最多 1 天
            )
            self.logger.debug(
                f"[{self.exec_info.info.crawler_name}] 已添加新任务，首次运行时间已设为现在，将立即尝试执行"
            )

    @async_pushme_try_catch_decorator
    async def run(self):
        # 定时任务每次触发都会走到这里，属于正常调度流程，用 debug 记录即可，
        # 避免污染 CRITICAL 级别（只应保留真正的严重故障）。
        self.logger.debug(
            f"[{self.exec_info.info.crawler_name}] 定时任务被触发，正在检查是否需要执行..."
        )

        # 杀旧：若上一次任务仍在运行（如卡死/超长），先强制终止，避免
        # APScheduler 因 max_instances=1 而 skip 本次触发，导致任务永久停摆
        if self.crawler_asyncio_task and not self.crawler_asyncio_task.done():
            self.logger.warning(
                f"[{self.exec_info.info.crawler_name}] 检测到上一次任务仍在运行，先终止旧任务再重新执行"
            )
            self.crawler_asyncio_task.cancel()
            try:
                await self.crawler_asyncio_task
            except asyncio.CancelledError:
                pass
            self.crawler_asyncio_task = None

        if await self.exec_info.is_need_to_execute():
            try:
                self.logger.debug(
                    f"[{self.exec_info.info.crawler_name}] 开始执行爬虫任务..."
                )
                # 调用异步 main 函数；不 await，立即返回，
                # 保证下次 cron 触发时能再次进入 run() 执行杀旧开新
                self.crawler_asyncio_task = asyncio.create_task(
                    self._run_and_save_exec_time()
                )
            except Exception as e:
                self.logger.exception(
                    f"[{self.exec_info.info.crawler_name}] 创建爬虫任务失败：{e}"
                )
                await a_push_error(
                    subject="运行异常",
                    content=f"{self.exec_info.info.crawler_name} 创建任务失败\n错误详情：{str(e)}",
                )
        else:
            self.logger.info(
                f"[{self.exec_info.info.crawler_name}] 当前不满足执行条件，跳过本次任务。"
            )

    async def _run_and_save_exec_time(self):
        """在后台执行爬虫任务，完成后保存执行时间。

        开始 / 结束推送的开关见 ``Utils.推送.push_switch``：
        本地文件（接口改，热生效）优先，其次环境变量 CRAWLER_PUSH_START / CRAWLER_PUSH_END；
        默认全关，``*`` / ``all`` 匹配全部爬虫。
        """
        crawler_name = self.exec_info.info.crawler_name
        start_ts = time.time()
        await self._push_stage(
            crawler_name, "start", f"开始时间：{datetime.now():%Y-%m-%d %H:%M:%S}"
        )
        try:
            await self.func()
            await self.exec_info.save_last_exec_time()
            await self._push_stage(
                crawler_name,
                "end",
                "状态：执行完成\n"
                f"开始时间：{datetime.fromtimestamp(start_ts):%Y-%m-%d %H:%M:%S}\n"
                f"结束时间：{datetime.now():%Y-%m-%d %H:%M:%S}\n"
                f"耗时：{format_duration(time.time() - start_ts)}",
            )
        except asyncio.CancelledError:
            self.logger.warning(
                f"[{self.exec_info.info.crawler_name}] 爬虫任务被终止（杀旧开新）。"
            )
            await self._push_stage(
                crawler_name,
                "end",
                "状态：执行被终止（检测到上一次任务仍在运行，已杀旧开新）\n"
                f"开始时间：{datetime.fromtimestamp(start_ts):%Y-%m-%d %H:%M:%S}\n"
                f"耗时：{format_duration(time.time() - start_ts)}",
            )
            raise
        except Exception as e:
            self.logger.exception(
                f"[{self.exec_info.info.crawler_name}] 爬虫执行出错：{e}"
            )
            pushed = await self._push_stage(
                crawler_name,
                "end",
                "状态：执行失败\n"
                f"开始时间：{datetime.fromtimestamp(start_ts):%Y-%m-%d %H:%M:%S}\n"
                f"耗时：{format_duration(time.time() - start_ts)}\n"
                f"错误详情：{e}",
            )
            # 结束开关已推送「执行失败」时不再重复发一条「运行异常」；
            # 开关关闭（默认）时保持原有的失败告警行为不变。
            if not pushed:
                await a_push_error(
                    subject="运行异常",
                    content=f"{self.exec_info.info.crawler_name} 执行异常\n错误详情：{str(e)}",
                )

    async def _push_stage(
        self, crawler_name: str, stage: Literal["start", "end"], content: str
    ) -> bool:
        """按开关推送「开始 / 结束」通知；返回是否真的推送了。

        推送失败只记日志，不影响爬虫任务。
        """
        try:
            # 最终开关 = 本地文件配置（接口改，热生效）优先，其次环境变量
            enabled = (
                push_switch.is_start_enabled(crawler_name)
                if stage == "start"
                else push_switch.is_end_enabled(crawler_name)
            )
            if not enabled:
                return False
            if stage == "start":
                await a_pushme(
                    f"{crawler_name} 爬虫开始执行",
                    content,
                    subject=PushSubject.INFO,
                )
            elif content.startswith("状态：执行完成"):
                await a_pushme(
                    f"{crawler_name} 爬虫执行完成",
                    content,
                    subject=PushSubject.SUCCESS,
                )
            else:
                await a_push_error(
                    content=content,
                    subject=f"{crawler_name} 爬虫{content.splitlines()[0].removeprefix('状态：')}",
                    topic=PushSubject.FAILURE,
                )
            return True
        except Exception as e:
            self.logger.warning(
                f"[{crawler_name}] 推送 {stage} 通知失败：{e!r}"
            )
            return False

    def terminate(self):
        """
        终止当前正在执行的任务
        """
        if self.crawler_asyncio_task:
            self.crawler_asyncio_task.cancel()
            return True
        return False

    def start(self):
        if not GLOBAL_SCHEDULER.running:
            self.logger.critical("调度器未运行，请确保已启动 GLOBAL_SCHEDULER。")
        GLOBAL_SCHEDULER.resume_job(self.job_id)

    def pause(self):
        """
        暂停任务计划，对当前正在执行任务无影响
        """
        GLOBAL_SCHEDULER.pause_job(self.job_id)

    def remove(self):
        """
        移除将来的任务计划，对当前正在执行的任务无影响
        """
        GLOBAL_SCHEDULER.remove_job(self.job_id)

    def add_job(self):
        """
        添加任务到调度器
        """
        job = GLOBAL_SCHEDULER.get_job(self.job_id)
        if job is None:
            GLOBAL_SCHEDULER.add_job(
                self.run,
                name=self.job_id,
                trigger=self.trigger,
                id=self.job_id,
                next_run_time=datetime.now(),
                coalesce=True,
                max_instances=1,
                misfire_grace_time=3600,
            )
            self.logger.info(
                f"[{self.exec_info.info.crawler_name}] 任务已重新添加到调度器"
            )


class GenericCrawlerScheduler(BaseScheduler):
    """
    通用爬虫调度器，用于调度任意爬虫类
    """

    def __init__(
        self,
        crawler: Any,
        cron_expr: str,
        default_interval_seconds: int = 2 * 3600,
        crawler_name: Optional[str] = None,
    ):
        self.crawler = crawler
        # 统一「调度器名 == 爬虫实例名 == 卡住监控名」：
        # 未显式指定时依次回退「爬虫自身声明的名字 → 类名」，
        # 并把最终名字回填到爬虫实例，使其日志前缀与告警推送也能区分同类多实例
        # （例如 LotteryApiRobot 的 DYN / RESERVE 两个实例）。
        resolved_name = (
            crawler_name
            or getattr(crawler, "crawler_name", "")
            or crawler.__class__.__name__
        )
        crawler.crawler_name = resolved_name
        super().__init__(
            func=self.crawler.main,
            cron_expr=cron_expr,
            default_interval_seconds=default_interval_seconds,
            crawler_name=resolved_name,
            logger=self.crawler.log,
        )


if __name__ == "__main__":

    class MockParams(CustomBaseModelHashable):
        a: int

        def __hash__(self):
            return hash(self.a)

    class MockCrawlerConfig(CrawlerConfig):
        pass

    class MockCrawler(UnlimitedCrawler[MockParams]):
        """模拟的爬虫类，仅用于测试"""

        Config = MockCrawlerConfig

        async def handle_fetch(self, params: ParamsType) -> WorkerStatus | Any:
            self.log.info(f"[MockCrawler] 模拟爬虫正在执行 handle_fetch...{params}")
            await asyncio.sleep(1)

        async def key_params_gen(
            self, params: ParamsType
        ) -> AsyncGenerator[MockParams, None]:
            for i in range(10):
                yield MockParams(a=i)

        async def is_stop(self) -> bool: ...

        async def main(self):
            self.log.info("[MockCrawler] 开始执行 main 方法...")
            await self.run()
            self.log.info("[MockCrawler] main 方法执行完成")

    async def _test_scheduler():
        # 启动全局调度器（如果尚未启动）
        if not GLOBAL_SCHEDULER.running:
            GLOBAL_SCHEDULER.start()
        # 创建爬虫和调度器
        crawler = MockCrawler()
        scheduler = GenericCrawlerScheduler(
            crawler=crawler,
            cron_expr="*/1 * * * *",  # 每分钟执行一次
            default_interval_seconds=60,  # 至少间隔 60 秒才能再次执行
        )

        default_logger.info("调度器已启动，等待任务触发...")

        try:
            # 让主函数持续运行一段时间以便观察定时任务
            await asyncio.sleep(300)  # 5分钟
        except KeyboardInterrupt:
            default_logger.info("收到退出信号，正在关闭调度器...")
        finally:
            scheduler.remove()
            GLOBAL_SCHEDULER.shutdown()

    asyncio.run(_test_scheduler())
