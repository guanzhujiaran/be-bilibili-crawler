import os
from dataclasses import dataclass, field
from typing import Any, List
from pydantic import BaseModel, ConfigDict, Field

from log.base_log import (
    myfastapi_logger,
    sql_log,
    official_lot_logger,
    get_rm_following_list_logger,
    reserve_lot_logger,
    topic_lot_logger,
    sams_club_logger,
    background_task_logger,
    live_monitor_logger,
    get_others_lot_logger,
)
from Service.BaseCrawler.plugin.base import CrawlerPlugin
from Service.BaseCrawler.plugin.statusPlugin import StatsPlugin, SequentialNullStopPlugin

_current_dir = os.path.dirname(os.path.abspath(__file__))


@dataclass
class PluginConfig:
    """插件声明（dataclass 写法）。

    - ``plugin_name``：注册到爬虫实例上的属性名。
      爬虫初始化时通过反射自动执行 ``setattr(self, plugin_name, plugin_cls(self))``，
      子类即可直接用 ``self.<plugin_name>`` 访问插件，无需手动写 ``self.xxx = StatsPlugin(self)``。
    - ``plugin_cls``：插件类（必须是 ``CrawlerPlugin`` 的子类），仅需可经 ``plugin_cls(self)`` 构造。

    例如：
        plugins=[PluginConfig("stats_plugin", StatsPlugin)]
    """

    plugin_name: str
    plugin_cls: type[CrawlerPlugin]


class CrawlerConfig(BaseModel):
    """无限爬虫通用配置（pydantic BaseModel）。

    每个具体爬虫都定义自己的 ``CrawlerConfig`` 子类，并设置独立的默认值；
    配置自管理在本模块（``Service/BaseCrawler/config.py``），**不写入全局
    ``Settings``**——``Settings`` 只承载 DB / Redis / MQ 等部署相关配置。

    字段默认值与 ``UnlimitedCrawler`` 原有的硬编码默认值保持一致。

    ``max_sem`` / ``logger`` / ``plugins`` 等运行参数统一在此管理，
    爬虫初始化时直接读取 ``self.Config`` 对应的配置实例，不再接受外部传参。
    """

    model_config = ConfigDict(extra="ignore")

    # 最大并发数（同时运行的 worker 数量）
    max_sem: int = Field(default=10, description="最大并发数")
    # 任务失败时是否重新入队
    requeue_on_fetch_fail: bool = Field(default=False)
    # 任务超时时是否重新入队（独立于 requeue_on_fetch_fail）
    requeue_on_timeout: bool = Field(default=True)
    # 失败任务最大重试次数，-1 表示无限重试
    max_retries: int = Field(default=-1)
    # 单个任务最大超时时间（秒），None 表示不限制
    worker_max_timeout: int | None = Field(default=None)
    # 是否打印超时错误日志
    log_timeout_error: bool = Field(default=True)
    # 是否打印抓取异常日志
    log_error: bool = Field(default=True)
    # 任务失败后的延迟时间（秒）
    worker_error_delay: int = Field(default=300)

    # 日志对象（由 Config 集中管理；运行时可用对应环境变量覆盖其子字段前的 logger 选择）
    logger: Any = myfastapi_logger
    # 插件声明列表：每个元素为 PluginConfig(plugin_name, plugin_cls)，
    # 爬虫初始化时自动 setattr 到 self.<plugin_name>，无需子类手动绑定。
    plugins: List[PluginConfig] = []


# ===== 各爬虫配置子类（集中管理） =====
# 仅覆盖与基类默认值不同的字段；运行时通过模块级注册表注入，不再经过全局 Settings。
# logger / plugins 同样在此声明，实现「每个爬虫一份独立配置」。
# 注意：pydantic v2 要求子类覆盖字段必须带类型注解。


class GetProxyMethodsConfig(CrawlerConfig):
    """代理获取爬虫配置（env: PROXY__*）"""

    max_sem: int = 10
    worker_max_timeout: int | None = 300
    requeue_on_fetch_fail: bool = False
    requeue_on_timeout: bool = True
    logger: Any = sql_log
    plugins: List[PluginConfig] = [PluginConfig("stats_plugin", StatsPlugin)]


class DynDetailScrapyConfig(CrawlerConfig):
    """动态详情爬虫配置（env: DYN_DETAIL__*）"""

    max_sem: int = 20
    worker_max_timeout: int | None = 300
    requeue_on_fetch_fail: bool = True
    requeue_on_timeout: bool = True
    log_timeout_error: bool = False
    logger: Any = official_lot_logger
    plugins: List[PluginConfig] = [PluginConfig("status_plugin", StatsPlugin)]


class GetRmFollowingListV2Config(CrawlerConfig):
    """取关对象爬虫配置（env: RM_FOLLOWING__*）"""

    max_sem: int = 100
    logger: Any = get_rm_following_list_logger
    plugins: List[PluginConfig] = [PluginConfig("status", StatsPlugin)]


class ReserveScrapyRobotConfig(CrawlerConfig):
    """预约抽奖爬虫配置（env: RESERVE__*）"""

    max_sem: int = 1
    worker_max_timeout: int | None = 300
    requeue_on_fetch_fail: bool = False
    requeue_on_timeout: bool = True
    logger: Any = reserve_lot_logger
    plugins: List[PluginConfig] = [
        PluginConfig("stats_plugin", StatsPlugin),
        PluginConfig("null_stop_plugin", SequentialNullStopPlugin),
    ]


class TopicRobotConfig(CrawlerConfig):
    """话题抽奖爬虫配置（env: TOPIC__*）"""

    max_sem: int = 1
    worker_max_timeout: int | None = 300
    requeue_on_fetch_fail: bool = False
    requeue_on_timeout: bool = True
    logger: Any = topic_lot_logger
    plugins: List[PluginConfig] = [
        PluginConfig("stats_plugin", StatsPlugin),
        PluginConfig("null_stop_plugin", SequentialNullStopPlugin),
    ]


class LotteryApiRobotConfig(CrawlerConfig):
    """抽奖 API 爬虫配置（env: LOTTERY_API__*）

    max_sem 与 logger 由运行时调用方注入（见 LotteryApiRobot._load_config），
    故此处保持默认；plugins 固定为 StatsPlugin。
    """

    logger: Any = myfastapi_logger
    plugins: List[PluginConfig] = [PluginConfig("stats_plugin", StatsPlugin)]


class RefreshBiliLotDatabaseConfig(CrawlerConfig):
    """刷新抽奖数据库爬虫配置（env: REFRESH_LOT__*）"""

    max_sem: int = 1
    logger: Any = background_task_logger
    plugins: List[PluginConfig] = [PluginConfig("stats_plugin", StatsPlugin)]


class SamsClubCrawlerConfig(CrawlerConfig):
    """山姆分类爬虫配置（env: SAMS_CLUB__*）"""

    max_sem: int = 1
    requeue_on_fetch_fail: bool = False
    logger: Any = sams_club_logger
    plugins: List[PluginConfig] = [PluginConfig("stats_plugin", StatsPlugin)]


class SamsClubSPUDetailCrawlerConfig(CrawlerConfig):
    """山姆 SPU 详情爬虫配置（env: SAMS_CLUB_SPU__*）"""

    max_sem: int = 1
    requeue_on_fetch_fail: bool = False
    logger: Any = sams_club_logger
    plugins: List[PluginConfig] = [PluginConfig("stats_plugin", StatsPlugin)]


class BiliLiveCrawlerConfig(CrawlerConfig):
    """直播监控爬虫配置（env: BILI_LIVE__*）"""

    logger: Any = live_monitor_logger
    plugins: List[PluginConfig] = []


class GetOthersLotDynRobotConfig(CrawlerConfig):
    """获取他人抽奖动态爬虫配置（直接构造，不依赖全局 Settings 注入）"""

    max_sem: int = 1
    # 空间动态 / 抽奖判定可能持续很久（逐页抓取），不做整体超时
    worker_max_timeout: int | None = None
    # 单个用户/动态抓取失败时不再重入队（与原始逻辑一致：跳过并继续）
    requeue_on_fetch_fail: bool = False
    requeue_on_timeout: bool = False
    max_retries: int = 0
    log_timeout_error: bool = False
    # 避免异常时长时间休眠，异常已在各业务方法内部处理
    worker_error_delay: int = 0
    logger: Any = get_others_lot_logger
    plugins: List[PluginConfig] = []


# ===== 爬虫配置注册表（集中管理，不依赖全局 Settings） =====
# config 类 -> 默认配置实例。新增爬虫配置时在此登记即可，无需改动 CONFIG.Settings。
CRAWLER_CONFIG_REGISTRY: dict[type[CrawlerConfig], CrawlerConfig] = {
    GetProxyMethodsConfig: GetProxyMethodsConfig(),
    DynDetailScrapyConfig: DynDetailScrapyConfig(),
    GetRmFollowingListV2Config: GetRmFollowingListV2Config(),
    ReserveScrapyRobotConfig: ReserveScrapyRobotConfig(),
    TopicRobotConfig: TopicRobotConfig(),
    LotteryApiRobotConfig: LotteryApiRobotConfig(),
    RefreshBiliLotDatabaseConfig: RefreshBiliLotDatabaseConfig(),
    SamsClubCrawlerConfig: SamsClubCrawlerConfig(),
    SamsClubSPUDetailCrawlerConfig: SamsClubSPUDetailCrawlerConfig(),
    BiliLiveCrawlerConfig: BiliLiveCrawlerConfig(),
    GetOthersLotDynRobotConfig: GetOthersLotDynRobotConfig(),
}


def get_crawler_config(config_cls: type[CrawlerConfig]) -> CrawlerConfig:
    """按爬虫的 ``Config`` 类返回集中管理的配置实例。

    未知类（如测试用的临时子类）回退为默认实例。
    """
    return CRAWLER_CONFIG_REGISTRY.get(config_cls, config_cls())
