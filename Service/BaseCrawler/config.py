import os
from dataclasses import dataclass, field
from typing import Any, List, Literal
from pydantic import BaseModel, ConfigDict, Field

from log.base_log import (
    myfastapi_logger,
    official_lot_logger,
    get_rm_following_list_logger,
    reserve_lot_logger,
    topic_lot_logger,
    sams_club_logger,
    background_task_logger,
    get_others_lot_logger,
)
from Service.BaseCrawler.plugin.base import CrawlerPlugin
from Service.BaseCrawler.plugin.statusPlugin import (
    StatsPlugin,
    SequentialNullStopPlugin,
)
from Utils.代理.mdoel.ProxyTier import ProxyTier

_current_dir = os.path.dirname(os.path.abspath(__file__))


@dataclass
class PluginConfig:
    """插件声明（dataclass 写法）。

    - ``plugin_name``：注册到爬虫实例上的属性名。
      爬虫初始化时通过反射自动执行 ``setattr(self, plugin_name, plugin_cls(self))``，
      子类即可直接用 ``self.<plugin_name>`` 访问插件，无需手动写 ``self.xxx = StatsPlugin(self)``。
    - ``plugin_cls``：插件类（必须是 ``CrawlerPlugin`` 的子类），仅需可经 ``plugin_cls(self)`` 构造。
    - ``plugin_kwargs``：插件设置项，实例化时透传给插件构造函数
      （插件是 pydantic 模型，设置项需在插件类中声明为字段）。

    例如：
        plugins=[PluginConfig("stats_plugin", StatsPlugin)]
        plugins=[PluginConfig("stats_plugin", StatsPlugin, {"push_result": True})]
    """

    plugin_name: str
    plugin_cls: type[CrawlerPlugin]
    plugin_kwargs: dict[str, Any] = field(default_factory=dict)


class CrawlerConfig(BaseModel):
    """无限爬虫通用配置（pydantic BaseModel）。

    每个具体爬虫都定义自己的 ``CrawlerConfig`` 子类，并设置独立的默认值；
    配置自管理在本模块（``Service/BaseCrawler/config.py``），**不写入全局
    ``Settings``**——``Settings`` 只承载 DB / Redis / MQ 等部署相关配置。

    字段默认值与 ``UnlimitedCrawler`` 原有的硬编码默认值保持一致。

    ``logger`` / ``plugins`` 等在配置类里声明；**并发数不在这里**，
    统一用 ``concurrency_key`` 指向 ``CONFIG.concurrency``。
    爬虫初始化时直接读取 ``self.Config`` 对应的配置实例，不再接受外部传参。
    """

    model_config = ConfigDict(extra="ignore")

    # 爬虫名称（用于日志前缀 / 异常推送 / 统计推送的标识）。
    # 一个爬虫类可能有多个实例（如 LotteryApiRobot 的 business_type=2 与 10），
    # 此时必须由实例声明各自的名字，不能退化成类名，否则日志与告警无法区分。
    # 留空表示回退为类名；实际由调度器创建时统一回填为 BackgroundServiceName 的值。
    crawler_name: str = Field(
        default="", description="爬虫实例名称（留空则回退为类名）"
    )

    # ===== 并发：不在各爬虫里写数字，只声明去 CONFIG.concurrency 的哪一项取 =====
    # 例如 ``concurrency_key = "reserve"`` → 读 CONFIG.concurrency.reserve。
    # 留空则用 CONFIG.concurrency.crawler_workers 作为兜底。
    concurrency_key: str = Field(
        default="", description="并发数取自 CONFIG.concurrency 的字段名"
    )
    # ===== 运行行为：全部跟随统一配置，子类只在「不一样」时才覆盖 =====
    # 统一来源见 CONFIG.crawler_runtime（超时/重试/退避/日志），
    # 由 UnlimitedCrawler.__init__ 解析；这里的 None / -1 都是「跟随」的哨兵值。
    # 任务失败时是否重新入队：None = 跟随
    requeue_on_fetch_fail: bool | None = Field(default=None)
    # 任务超时时是否重新入队（独立于 requeue_on_fetch_fail）：None = 跟随
    requeue_on_timeout: bool | None = Field(default=None)
    # 失败任务最大重试次数，-1 = 无限重试：None = 跟随
    max_retries: int | None = Field(default=None)
    # 单个任务最大超时时间（秒），None = 不限制：-1 = 跟随
    worker_max_timeout: int | None = Field(default=-1)
    # 是否打印超时 / 抓取异常日志：None = 跟随
    log_timeout_error: bool | None = Field(default=None)
    log_error: bool | None = Field(default=None)
    # 任务失败后的延迟时间（秒）：None = 跟随
    worker_error_delay: int | None = Field(default=None)

    # ===== 代理档位（只表达排序偏好与重试预算，见 RequestRetryPolicy / ProxyHealth）=====
    # slow：口径接近老版本 select_proxy("rand") —— 随机取一条 status 可用的就用，
    #       EWMA / 冷却 / 在途只记账与排序（开关：ProxyHealthConfig.slow_skip_*），
    #       重试预算大（指数退避封顶 60s）；
    # fast：严格准入（EWMA + 冷却 + 在途 + 段级闸门），重试预算小（6 次即 -5）。
    # 默认 slow —— 爬虫全部退出 fast 车道：2026-11-16 代理池故障期间，fast 的
    # 「ewma>=0.9 + 在途<2 + 段未冷却」多重闸门叠加后 8 小时 3877 次
    # ProxyRetryBudgetExhausted(-5)，而池子里明明有 1.5 万条 status 可用的行。
    # 这是**每个爬虫自己声明**的语义：本文件不写死任何业务名称。
    proxy_tier: ProxyTier = Field(
        default=ProxyTier.SLOW, description="该爬虫使用的代理档位"
    )
    # slow 车道的基准并发相对 max_sem 的比例（不再按 hot/warm 背压收缩）
    slow_lane_ratio: float = Field(
        default=1.0, description="slow 车道基准并发 = max_sem × 本比例"
    )
    # 失败任务的延迟重入队（秒）：None = 跟随 CONFIG.crawler_runtime
    requeue_delay_base: int | None = Field(default=None)
    requeue_delay_max: int | None = Field(default=None)
    # 重入队上限（重试预算耗尽后的处置上限）：None = 跟随 CONFIG.crawler_runtime
    max_requeue_per_task: int | None = Field(default=None)
    requeue_max_task_seconds: float | None = Field(default=None)
    # 本轮失败后的重入队策略（按爬虫声明，基类不硬编码任何业务名）：
    # - immediate_front：立即插队首（历史行为，默认，避免其他爬虫回归）；
    # - delayed：按指数退避延迟后再插队首；
    # - next_round：本轮不重排、不计 max_retries，留在 crawl_failed_task pending，
    #   由下一轮 main 的 load_retry_params() 补跑（需要 retry_task_type 才生效）。
    requeue_strategy: Literal["immediate_front", "delayed", "next_round"] = Field(
        default="immediate_front", description="本轮失败任务的重入队策略"
    )
    # ===== 代理池整体不可用（池级失败）时的重入队语义 =====
    # 池级失败（``RequestProxyUnavailableError`` / ``ProxyRetryBudgetExhausted``，
    # 判定统一走 ``resilience.is_pool_level_error``）说明「换哪个代理都取不到数」，
    # 任务本身没有问题，不能按业务失败直接丢弃 —— 必须留在本轮重试队列，
    # 等代理恢复后继续抓（至少一次）。开启后该分支跳过
    # requeue_on_fetch_fail / requeue_on_timeout / max_retries，
    # 但仍受 max_requeue_per_task / requeue_max_task_seconds 两道兜底上限约束。
    requeue_on_pool_unavailable: bool = Field(
        default=True,
        description="池级失败是否强制本轮重入队，不受 requeue_on_fetch_fail / "
        "requeue_on_timeout / max_retries 限制（仍受重入队次数与累计时长上限约束）",
    )
    # 轮次级等待上限（防止代理长期不可用时无限期挂住）：
    # 单个任务一轮内最多因熔断等待 ``pool_unavailable_max_waits`` 次，
    # 每次最长 ``pool_unavailable_wait_seconds`` 秒；用尽后不再等待，
    # 直接按池级失败交回队列（由上面的重入队上限决定何时收尾）。
    pool_unavailable_max_waits: int = Field(
        default=10,
        description="单个任务一轮内因代理池不可用最多等待次数；0 = 不等待，-1 = 不限",
    )
    pool_unavailable_wait_seconds: float = Field(
        default=900.0,
        description="单次等待代理池恢复的最长秒数（应不小于熔断退避上限，"
        "避免一次退避被切碎成多轮等待）；<=0 表示不限制单次等待时长",
    )
    # 轮末判定「被代理挡住」的池级失败占比阈值：
    # 达到时保持本轮未完成，下一轮复用该轮次继续抓（业务侧用自己更精确的口径时可覆盖）。
    proxy_blocked_ratio_threshold: float = Field(
        default=0.5,
        description="池级失败占比达到该阈值即视为本轮被代理池挡住（0~1）",
    )
    # 失败推送里「业务 id」的取值范围（按顺序取第一个命中的字段）：
    # 留空则自动取 params 里第一个以 ``_id`` 结尾的字段。业务差异只在各爬虫 Config 声明。
    business_id_fields: List[str] = Field(
        default_factory=list, description="从任务参数提取 business_id 的候选字段名"
    )
    # 失败推送里附带的「最近 N 个失败 id 摘要」条数（0 = 不附带）
    failure_push_id_limit: int = Field(
        default=5, description="失败汇总推送里附带最近多少个失败业务 id"
    )

    # 日志对象（由 Config 集中管理；运行时可用对应环境变量覆盖其子字段前的 logger 选择）
    logger: Any = myfastapi_logger
    # 插件声明列表：每个元素为 PluginConfig(plugin_name, plugin_cls)，
    # 爬虫初始化时自动 setattr 到 self.<plugin_name>，无需子类手动绑定。
    plugins: List[PluginConfig] = []


# ===== 各爬虫配置子类（集中管理） =====
# 仅覆盖与基类默认值不同的字段；运行时通过模块级注册表注入，不再经过全局 Settings。
# logger / plugins 同样在此声明，实现「每个爬虫一份独立配置」。
# 注意：pydantic v2 要求子类覆盖字段必须带类型注解。


class DynDetailScrapyConfig(CrawlerConfig):
    """动态详情爬虫配置（env: DYN_DETAIL__*）"""

    concurrency_key: str = "dyn_detail"
    # 差异：失败也重入队；单条详情最长 300s
    requeue_on_fetch_fail: bool = True
    # 失败后**无条件立即插回队首**重试（不看退避、不进队尾：队列积压时放队尾等于
    # 拖到最后，而前面的 rid 缺失会影响后面的结束判断）；
    # 次数上限 20，实际由 requeue_max_task_seconds=1800s 兜底；
    # 用尽后任务留在 crawl_failed_task pending，下一轮开头继续 —— 不会丢
    max_requeue_per_task: int | None = 20
    worker_max_timeout: int | None = 300
    # 该爬虫的任务量巨大且可容忍等待：走 slow 车道（= 老版本免费池口径，
    # 随机取一条 status 可用的就用；健康度只记账不挡人）。
    proxy_tier: ProxyTier = ProxyTier.SLOW
    # 失败推送里的业务 id：动态详情用 rid（其余字段名只在各爬虫 Config 声明）
    business_id_fields: List[str] = ["rid", "dynamic_id"]
    logger: Any = official_lot_logger
    plugins: List[PluginConfig] = [
        PluginConfig("status_plugin", StatsPlugin, {"push_result": True}),
    ]


class GetRmFollowingListV2Config(CrawlerConfig):
    """取关对象爬虫配置（env: RM_FOLLOWING__*）"""

    concurrency_key: str = "rm_following"
    logger: Any = get_rm_following_list_logger
    plugins: List[PluginConfig] = [
        PluginConfig("status", StatsPlugin),
    ]


class ReserveScrapyRobotConfig(CrawlerConfig):
    """预约抽奖爬虫配置（env: RESERVE__*）"""

    concurrency_key: str = "reserve"
    worker_max_timeout: int | None = 300
    # 默认 slow 车道（随机取一条 status 可用的就用，退避等池子恢复）。
    # 不再为了「时效敏感」直接上 fast：fast 预算耗尽后也会自动降档 slow 兜底，
    # 但直接从 slow 起步可以少浪费 6 次快速失败和一轮代理轮换。
    proxy_tier: ProxyTier = ProxyTier.SLOW
    # 至少一次（马上重试）：失败既落 crawl_failed_task，也**无条件立即插回队首**重试
    # （不等退避、不进队尾 —— 前面的 id 缺失会影响后面的结束判断）
    requeue_on_fetch_fail: bool = True
    max_requeue_per_task: int | None = 20
    logger: Any = reserve_lot_logger
    plugins: List[PluginConfig] = [
        PluginConfig("stats_plugin", StatsPlugin, {"push_result": True}),
        PluginConfig("null_stop_plugin", SequentialNullStopPlugin),
    ]


class TopicRobotConfig(CrawlerConfig):
    """话题抽奖爬虫配置（env: TOPIC__*）"""

    concurrency_key: str = "topic"
    worker_max_timeout: int | None = 300
    # 默认 slow 车道：话题时间窗口内允许退避等代理池恢复；
    # 确需快速失败的场景才显式改回 fast（依赖 request_wrapper 的 fast→slow 兜底）。
    proxy_tier: ProxyTier = ProxyTier.SLOW
    # 至少一次（马上重试）：同上
    requeue_on_fetch_fail: bool = True
    max_requeue_per_task: int | None = 20
    # 失败推送里的业务 id：话题抽奖用 topic_id
    business_id_fields: List[str] = ["topic_id"]
    logger: Any = topic_lot_logger
    plugins: List[PluginConfig] = [
        PluginConfig("stats_plugin", StatsPlugin, {"push_result": True}),
        PluginConfig("null_stop_plugin", SequentialNullStopPlugin),
    ]


class LotteryApiRobotConfig(CrawlerConfig):
    """抽奖 API 爬虫配置（env: LOTTERY_API__*）

    并发取自 CONFIG.concurrency.lottery_api，logger 由运行时调用方注入（LotteryApiRobot._load_config），
    故此处保持默认；plugins 固定为 StatsPlugin。
    """

    concurrency_key: str = "lottery_api"
    # 默认 slow 车道：官方抽奖动态 / 预约 notice 失败后允许退避等池子恢复，
    # 避免一次代理池抖动就把整批请求打成 ProxyRetryBudgetExhausted。
    proxy_tier: ProxyTier = ProxyTier.SLOW
    # 至少一次（马上重试）：官方抽奖动态 / 预约 notice 失败后立刻插回队首重试
    requeue_on_fetch_fail: bool = True
    max_requeue_per_task: int | None = 20
    logger: Any = myfastapi_logger
    plugins: List[PluginConfig] = [
        PluginConfig("stats_plugin", StatsPlugin, {"push_result": True}),
    ]


class RefreshBiliLotDatabaseConfig(CrawlerConfig):
    """刷新抽奖数据库爬虫配置（env: REFRESH_LOT__*）"""

    concurrency_key: str = "refresh_lot_db"
    logger: Any = background_task_logger
    plugins: List[PluginConfig] = [PluginConfig("stats_plugin", StatsPlugin)]


class SamsClubCrawlerConfig(CrawlerConfig):
    """山姆分类爬虫配置（env: SAMS_CLUB__*）"""

    concurrency_key: str = "sams_club"
    logger: Any = sams_club_logger
    plugins: List[PluginConfig] = [PluginConfig("stats_plugin", StatsPlugin)]


class SamsClubSPUDetailCrawlerConfig(CrawlerConfig):
    """山姆 SPU 详情爬虫配置（env: SAMS_CLUB_SPU__*）"""

    concurrency_key: str = "sams_club_spu"
    logger: Any = sams_club_logger
    plugins: List[PluginConfig] = [PluginConfig("stats_plugin", StatsPlugin)]


class GetOthersLotDynRobotConfig(CrawlerConfig):
    """获取他人抽奖动态爬虫配置（直接构造，不依赖全局 Settings 注入）"""

    concurrency_key: str = "get_others_lot_dyn"
    # 空间动态 / 抽奖判定可能持续很久（逐页抓取），不做整体超时
    worker_max_timeout: int | None = None
    # 单个用户/动态抓取失败时跳过并继续：不重入队、不重试、不退避
    requeue_on_timeout: bool = False
    max_retries: int = 0
    worker_error_delay: int = 0
    # 但「代理池整体不可用」不是业务失败（见 requeue_on_pool_unavailable）：
    # 任务必须留在本轮重试队列，等代理恢复后继续抓，不能把整轮用户当失败丢弃。
    # 下面两道上限是池级失败专用兜底：单任务累计 6 小时内最多重入队 100 次，
    # 超过才交给下一轮（轮次保持未完成，下一轮复用该轮次继续抓）。
    max_requeue_per_task: int | None = 100
    requeue_max_task_seconds: float | None = 6 * 3600
    # 默认 slow 车道：他人动态是长期批量任务，可容忍退避等池子恢复；
    # 历史上 fast 在池抖时反复 ProxyRetryBudgetExhausted(-5)，任务被交回队列空转。
    # 注：内部空间动态 / 动态详情 / 评论的 RequestConf 都从本配置取档位，
    # 否则历史参数 is_use_available_proxy=True 会把档位重新解析成 slow。
    # 少数显式 tier=FAST 的调用点由 request_wrapper 的 fast→slow 兜底。
    proxy_tier: ProxyTier = ProxyTier.SLOW
    logger: Any = get_others_lot_logger
    plugins: List[PluginConfig] = [
        PluginConfig("stats_plugin", StatsPlugin, {"push_result": True}),
    ]


# ===== 爬虫配置注册表（集中管理，不依赖全局 Settings） =====
# config 类 -> 默认配置实例。新增爬虫配置时在此登记即可，无需改动 CONFIG.Settings。
CRAWLER_CONFIG_REGISTRY: dict[type[CrawlerConfig], CrawlerConfig] = {
    DynDetailScrapyConfig: DynDetailScrapyConfig(),
    GetRmFollowingListV2Config: GetRmFollowingListV2Config(),
    ReserveScrapyRobotConfig: ReserveScrapyRobotConfig(),
    TopicRobotConfig: TopicRobotConfig(),
    LotteryApiRobotConfig: LotteryApiRobotConfig(),
    RefreshBiliLotDatabaseConfig: RefreshBiliLotDatabaseConfig(),
    SamsClubCrawlerConfig: SamsClubCrawlerConfig(),
    SamsClubSPUDetailCrawlerConfig: SamsClubSPUDetailCrawlerConfig(),
    GetOthersLotDynRobotConfig: GetOthersLotDynRobotConfig(),
}


def get_crawler_config(config_cls: type[CrawlerConfig]) -> CrawlerConfig:
    """按爬虫的 ``Config`` 类返回集中管理的配置实例。

    未知类（如测试用的临时子类）回退为默认实例。
    """
    return CRAWLER_CONFIG_REGISTRY.get(config_cls, config_cls())
