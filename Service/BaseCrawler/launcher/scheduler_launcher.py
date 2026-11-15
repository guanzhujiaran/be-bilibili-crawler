# -*- coding: utf-8 -*-
import random
import time
from datetime import datetime, timedelta
from typing import Final, Literal, Optional, AsyncGenerator, Any
from apscheduler.triggers.cron import CronTrigger
from loguru import logger as default_logger
from CONFIG import CONFIG
from log.base_log import myfastapi_logger
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


#: 首跑策略标签（写进 WARNING 日志，便于一眼区分三类任务）
POLICY_STARTUP_CATCHUP: Final[str] = "startup_catchup"
POLICY_HIGH_FREQ_JITTER: Final[str] = "high_freq_jitter"
POLICY_CRON_ONLY: Final[str] = "cron_only"


def _should_run_on_startup_by_cron(cron_expr: str) -> bool:
    """**默认**是否允许「启动抖动窗内补跑一次」（只按 cron 频率自动判定）。

    判据：minute 或 hour 字段带 ``*``（如 ``*/10 * * * *``、``0 */5 * * *``），
    即一天触发多次的高频任务 → 默认补跑；像 ``0 0 * * *``、``30 4 1 * *``
    这种「某天某点」的日级/月级任务 → **默认等 cron 下次触发**。

    ⚠️ 自动判定只是「默认安全值」，不代表业务语义：
    冷数据归档 / 表空间维护这类重活必须保持默认（或显式 ``run_on_startup=False``）；
    日级**数据爬取**任务（GET_DYN / GET_TOPIC / SAMSCCLUB ...）重启后需要补跑一次，
    必须在 ``CrawlerScheduler`` 构造时**显式**传 ``run_on_startup=True``。
    以后新增日级任务时，若忘了这个显式标记，它会默认等 cron 而不补跑 ——
    这是刻意的安全侧默认（宁可晚跑，也不要把重活压到启动瞬间）。

    补跑不等于「立即抓取」：任务仍走 ``BaseScheduler.run()`` →
    ``CrawlerExecutionInfo.is_need_to_execute()``，按 Redis 的 last_exec_time +
    default_interval_seconds 节流，刚抓过就会被跳过。
    """
    minute, hour, _day, _month, _day_of_week = cron_expr.split()
    return "*" in minute or "*" in hour


def _first_run_time(trigger: CronTrigger, run_on_startup: bool) -> datetime:
    """首次运行时间策略（只决定「什么时候触发」，不决定「是否真的抓取」）。

    - ``run_on_startup=True``：现在 + 0~``startup_jitter_seconds`` 随机偏移，
      把启动风暴摊平到 1 分钟内。用于高频任务与**显式声明要补跑的日级爬虫**；
      触发后 ``run()`` 仍会按 ``is_need_to_execute()`` 节流，不绕过判断。
    - ``run_on_startup=False``：直接用 cron 的**下一次触发时间**，不允许启动即跑。
      2026-11-15 的事故就是每日 0 点的冷数据归档被抖动提前触发，同步 gzip 读回
      占住事件循环 238.97s，导致 uvicorn 端口已 listen 但 HTTP 全部超时。
    """
    if run_on_startup:
        jitter = max(0.0, CONFIG.crawler_runtime.startup_jitter_seconds)
        return datetime.now() + timedelta(seconds=random.uniform(0, jitter))
    now = datetime.now(trigger.timezone)
    next_fire = trigger.get_next_fire_time(None, now)
    # 理论上 cron 总能算出下一次；兜底避免 next_run_time=None 让任务永远不会跑
    return next_fire or now + timedelta(days=1)


class BaseScheduler:
    """
    定时任务调度器

    ``startup_policy`` 三态（见 ``_should_run_on_startup_by_cron``）：
    ``high_freq_jitter`` 高频任务启动抖动 / ``startup_catchup`` 日级爬虫显式声明的
    启动补跑（仍受 ``is_need_to_execute`` 节流）/ ``cron_only`` 重活只等 cron。
    """

    startup_policy: str

    def __init__(
        self,
        func: Callable,
        cron_expr: str,
        default_interval_seconds: int = 2 * 3600,
        crawler_name: str = "",
        logger=default_logger,
        run_on_startup: bool | None = None,
    ):
        self.func = func
        self.crawler_asyncio_task = None
        # 首次触发（启动抖动窗内的补跑 / cron 首跑）单独留痕，便于在 WARNING 日志里
        # 证明「补跑被 is_need_to_execute 节流跳过」而不是没跑起来
        self._first_fire = True
        self.cron_expr = cron_expr
        self.job_id = f"crawler_job_{crawler_name}"
        self.logger = logger
        # 自动判定的结果单独留一份：日志要区分「高频抖动」与「日级显式补跑」
        self._cron_default_run_on_startup = _should_run_on_startup_by_cron(cron_expr)
        # None = 按 cron 频率自动判定（默认只有高频补跑、日级默认等 cron）；
        # 显式传值可覆盖：日级爬虫要补跑就传 True，归档/维护重活传 False
        self.run_on_startup = (
            self._cron_default_run_on_startup
            if run_on_startup is None
            else run_on_startup
        )
        if not self.run_on_startup:
            self.startup_policy = POLICY_CRON_ONLY
        elif self._cron_default_run_on_startup:
            self.startup_policy = POLICY_HIGH_FREQ_JITTER
        else:
            self.startup_policy = POLICY_STARTUP_CATCHUP
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
            # 三态首跑策略：高频抖动 / 日级显式补跑 / 重活等 cron
            first_run = _first_run_time(self.trigger, self.run_on_startup)
            GLOBAL_SCHEDULER.add_job(
                self.run,
                name=self.job_id,
                trigger=self.trigger,
                id=self.job_id,
                next_run_time=first_run,
                coalesce=True,  # 错过的任务合并为一次
                max_instances=1,  # 同时最多允许一个实例
                misfire_grace_time=24 * 3600,  # 允许延迟最多 1 天
            )
            jitter = max(0.0, CONFIG.crawler_runtime.startup_jitter_seconds)
            policy_detail = {
                POLICY_STARTUP_CATCHUP: (
                    "日级爬虫启动补跑：仍在抖动窗内只触发一次，"
                    "run() 继续走 is_need_to_execute 节流（不是立即重复抓取）"
                ),
                POLICY_HIGH_FREQ_JITTER: (
                    "高频任务启动抖动：一天多次，摊平启动风暴"
                ),
                POLICY_CRON_ONLY: (
                    "重活等 cron 下次触发：不在启动抖动窗内执行"
                ),
            }[self.startup_policy]
            # 三类策略统一 WARNING 且落 error_fastapi_log.log（LOG_LEVEL=WARNING 可见）：
            # 只有看到「首次运行时间 = cron 下次触发」+ policy=cron_only，才能确认
            # 重启没有把归档拉起来；看到 policy=startup_catchup 才能确认日级爬虫会补跑。
            myfastapi_logger.warning(
                f"event=crawler_job_added "
                f"job={self.exec_info.info.crawler_name} "
                f"policy={self.startup_policy} cron={self.cron_expr} "
                f"首次运行时间={first_run:%Y-%m-%d %H:%M:%S} "
                f"jitter={jitter:.0f}s —— {policy_detail}"
            )

    @async_pushme_try_catch_decorator
    async def run(self):
        # 定时任务每次触发都会走到这里，属于正常调度流程，用 debug 记录即可，
        # 避免污染 CRITICAL 级别（只应保留真正的严重故障）。
        is_first_fire = self._first_fire
        self._first_fire = False
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
            # INFO 在 LOG_LEVEL=WARNING 下不落盘，启动补跑是否真的被节流就无从确认。
            # 只在「首次触发 + 日级补跑策略」补一条 WARNING 到 error_fastapi_log.log，
            # 证明补跑走的是 run() → is_need_to_execute()，而不是没被调度起来，
            # 也不改动任何节流判断本身。
            if is_first_fire and self.startup_policy == POLICY_STARTUP_CATCHUP:
                myfastapi_logger.warning(
                    f"event=crawler_startup_catchup_skipped "
                    f"job={self.exec_info.info.crawler_name} "
                    f"interval={self.exec_info.info.default_interval_seconds}s "
                    "启动补跑被 is_need_to_execute 节流跳过（距上次执行不足间隔，不是漏跑）"
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
                # 与 _add_or_update_job 同一口径：重新挂载时也不允许长周期任务「立刻开跑」
                next_run_time=_first_run_time(self.trigger, self.run_on_startup),
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
        run_on_startup: bool | None = None,
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
            run_on_startup=run_on_startup,
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
