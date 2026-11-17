from log.base_log import reserve_lot_logger, official_lot_logger
from scripts.database.clean_backup_outdated_dynamic import cleaner
from Service.DataArchive.tablespace_maintenance import run_monthly_maintenance
from Service.BaseCrawler.launcher.scheduler_launcher import (
    GenericCrawlerScheduler,
    BaseScheduler,
)
from Service.GrpcModule.GrpcSrc.获取取关对象.GetRmFollowingListV2 import gmflv2
from Service.opus新版官方抽奖.bili_lottery_api.scrapyLotteryDataFromBapi import (
    LotteryApiRobot,
)
from Service.samsclub.main import sams_club_crawler, sams_club_SPU_detail_crawler
from Utils.代理.redisProxyRequest.GetProxyFromNet import get_proxy_methods
from Utils.代理.数据库操作.async_proxy_op_alchemy_mysql_ver import SQLHelper
from Service.opus新版官方抽奖.活动抽奖.话题抽奖.robot import topic_robot
from Service.opus新版官方抽奖.预约抽奖.etc.scrapyReserveJsonData import reserve_robot
from Service.GrpcModule.GrpcSrc.getDynDetail import dyn_detail_scrapy
from Service.GetOthersLotDyn.core.robot import get_others_lot_dyn_robot
from Service.opus新版官方抽奖.bili_lottery_api.refresh_bili_lot_database import (
    refresh_bili_lot_database_crawler,
)
from Service.BackgroundService.DailyReportCrawler import crawler_stuck_checker
from Models.v1.background_service.background_service_model import BackgroundServiceName


class BackgroundService:
    STUCK_CHECK_SCHEDULER: BaseScheduler | None = None
    DYN_DETAIL_DATABASE_CLEANER: BaseScheduler | None = None
    TABLESPACE_MAINTENANCE: BaseScheduler | None = None
    PROXY_POOL_CLEANER: BaseScheduler | None = None
    GET_PROXY_METHODS_SCHEDULER: GenericCrawlerScheduler | None = None
    SAMSCCLUB_SCHEDULER: GenericCrawlerScheduler | None = None
    SAMSCCLUB_SPU_DETAIL_SCHEDULER: GenericCrawlerScheduler | None = None
    GET_RESERVE_INFO: GenericCrawlerScheduler | None = None
    GET_DYN: GenericCrawlerScheduler | None = None
    GET_TOPIC: GenericCrawlerScheduler | None = None
    GET_OTHERS_LOT_DYN: GenericCrawlerScheduler | None = None
    REFRESH_BILI_LOTDATA_DATABASE: GenericCrawlerScheduler | None = None
    LOTTERY_API_ROBOT_DYN_SCHEDULER: GenericCrawlerScheduler | None = None
    LOTTERY_API_ROBOT_RESERVE_SCHEDULER: GenericCrawlerScheduler | None = None
    GMFLV2_SCHEDULER: GenericCrawlerScheduler | None = None

    def __init__(self):
        # ===== 首跑策略（见 scheduler_launcher._should_run_on_startup_by_cron）=====
        # 自动判定只作「安全侧默认值」：高频（minute/hour 带 *）默认启动抖动，
        # 「某天某点」的日级/月级任务默认等 cron 下次触发。
        # - 重活（冷数据归档 / 表空间维护）：显式 False，绝不启动即跑；
        # - 日级数据爬取：必须显式 True，重启后仍在抖动窗内补跑一次，避免数据
        #   新鲜度最坏延迟近 24h。补跑仍走 run() → is_need_to_execute()，按 Redis
        #   last_exec_time + default_interval_seconds 节流，刚抓过会被跳过；
        # - 高频任务：不传，保持默认 high_freq_jitter。
        # 以后新增日级爬虫请照这里显式声明，否则会默认等 cron 而不补跑。
        self.STUCK_CHECK_SCHEDULER = BaseScheduler(
            func=crawler_stuck_checker.check_and_report_stuck,
            cron_expr="*/10 * * * *",  # 每10分钟检查一次爬虫是否卡住
            default_interval_seconds=600,  # 10分钟间隔
            crawler_name=BackgroundServiceName.STUCK_CHECK_SCHEDULER.value,
        )
        self.DYN_DETAIL_DATABASE_CLEANER = BaseScheduler(
            # 每日：先导出抽奖信息 csv.gz，再归档冷数据 csv.gz 并删库。
            # 不许「启动即跑」：2026-11-15 每次重启都被抖动提前触发，归档读回
            # 大 gzip 包同步占住事件循环 238.97s，HTTP 全部超时。
            func=cleaner.run_daily_backup,
            cron_expr="0 0 * * *",
            crawler_name=BackgroundServiceName.DYN_DETAIL_DATABASE_CLEANER.value,
            default_interval_seconds=2 * 3600,
            run_on_startup=False,
        )
        self.TABLESPACE_MAINTENANCE = BaseScheduler(
            # 每月 1 号 04:30：把上月归档删库后残留的 InnoDB 空洞还给操作系统。
            # 放在 0 点归档任务之后、白天业务前，整表重建（ALTER TABLE ... FORCE）
            # 需要接近表大小的临时磁盘，默认只有空洞够大才真的重建。
            # 同样是月级重活，只在 cron 的下一次触发时间跑，不跟启动抢资源。
            func=run_monthly_maintenance,
            cron_expr="30 4 1 * *",
            crawler_name=BackgroundServiceName.TABLESPACE_MAINTENANCE.value,
            default_interval_seconds=25 * 86400,
            run_on_startup=False,
        )
        self.PROXY_POOL_CLEANER = BaseScheduler(
            # 每天 05:00（业务低峰）按「失败原因 + 时间窗口」分批清理僵尸代理。
            # 只在清理任务里判定，不影响取用；risk_control 永不删除。
            # 单轮上限 / 批大小 / 批间 sleep 见 ProxyHealthConfig.clean_*。
            func=SQLHelper.clear_unusable_proxy,
            cron_expr="0 5 * * *",
            crawler_name=BackgroundServiceName.PROXY_POOL_CLEANER.value,
            default_interval_seconds=24 * 3600,
            run_on_startup=False,
        )
        self.GET_PROXY_METHODS_SCHEDULER = GenericCrawlerScheduler(
            crawler=get_proxy_methods,
            cron_expr="0 */5 * * *",
            default_interval_seconds=12 * 3600,
            crawler_name=BackgroundServiceName.GET_PROXY_METHODS_SCHEDULER.value,
        )
        # ===== 日级数据爬取：显式 run_on_startup=True（重启后补跑，受节流）=====
        self.SAMSCCLUB_SCHEDULER = GenericCrawlerScheduler(
            crawler=sams_club_crawler,
            cron_expr="0 3 * * *",
            default_interval_seconds=15 * 3600,
            crawler_name=BackgroundServiceName.SAMSCCLUB_SCHEDULER.value,
            run_on_startup=True,
        )
        self.SAMSCCLUB_SPU_DETAIL_SCHEDULER = GenericCrawlerScheduler(
            crawler=sams_club_SPU_detail_crawler,
            cron_expr="0 4 * * *",
            default_interval_seconds=15 * 3600,
            crawler_name=BackgroundServiceName.SAMSCCLUB_SPU_DETAIL_SCHEDULER.value,
            run_on_startup=True,
        )
        self.GET_RESERVE_INFO = GenericCrawlerScheduler(
            crawler=reserve_robot,
            cron_expr="0 1 * * *",
            default_interval_seconds=15 * 3600,
            crawler_name=BackgroundServiceName.GET_RESERVE_INFO.value,
            run_on_startup=True,
        )
        self.GET_DYN = GenericCrawlerScheduler(
            crawler=dyn_detail_scrapy,
            cron_expr="0 2 * * *",
            default_interval_seconds=15 * 3600,
            crawler_name=BackgroundServiceName.GET_DYN.value,
            run_on_startup=True,
        )
        self.GET_TOPIC = GenericCrawlerScheduler(
            crawler=topic_robot,
            cron_expr="0 3 * * *",
            default_interval_seconds=15 * 3600,
            crawler_name=BackgroundServiceName.GET_TOPIC.value,
            run_on_startup=True,
        )
        self.GET_OTHERS_LOT_DYN = GenericCrawlerScheduler(
            crawler=get_others_lot_dyn_robot,
            cron_expr="0 8 * * *",
            default_interval_seconds=15 * 3600,
            crawler_name=BackgroundServiceName.GET_OTHERS_LOT_DYN.value,
            run_on_startup=True,
        )
        self.REFRESH_BILI_LOTDATA_DATABASE = GenericCrawlerScheduler(
            crawler=refresh_bili_lot_database_crawler,
            cron_expr="0 4 * * *",
            default_interval_seconds=15 * 3600,
            crawler_name=BackgroundServiceName.REFRESH_BILI_LOTDATA_DATABASE.value,
            run_on_startup=True,
        )
        self.LOTTERY_API_ROBOT_DYN_SCHEDULER = GenericCrawlerScheduler(
            crawler=LotteryApiRobot(log=official_lot_logger, business_type=2),
            cron_expr="0 5 * * *",
            default_interval_seconds=15 * 3600,
            crawler_name=BackgroundServiceName.LOTTERY_API_ROBOT_DYN_SCHEDULER.value,
            run_on_startup=True,
        )
        self.LOTTERY_API_ROBOT_RESERVE_SCHEDULER = GenericCrawlerScheduler(
            crawler=LotteryApiRobot(log=reserve_lot_logger, business_type=10),
            cron_expr="0 6 * * *",
            default_interval_seconds=15 * 3600,
            crawler_name=BackgroundServiceName.LOTTERY_API_ROBOT_RESERVE_SCHEDULER.value,
            run_on_startup=True,
        )
        self.GMFLV2_SCHEDULER = GenericCrawlerScheduler(
            crawler=gmflv2,
            cron_expr="0 7 * * *",
            default_interval_seconds=1,
            crawler_name=BackgroundServiceName.GMFLV2_SCHEDULER.value,
            run_on_startup=True,
        )


background_service = BackgroundService()

__all__ = ["background_service", "BackgroundService"]
