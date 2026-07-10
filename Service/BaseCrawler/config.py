import os
from pydantic import BaseModel, ConfigDict, Field

_current_dir = os.path.dirname(os.path.abspath(__file__))


class CrawlerConfig(BaseModel):
    """无限爬虫通用配置（pydantic BaseModel）。

    每个具体爬虫都定义自己的 ``CrawlerConfig`` 子类，并设置独立的默认值；
    全部配置集中在全局 ``Settings``（CONFIG.py）中作为嵌套子模型管理，
    部署时可通过环境变量 / .env 文件用双下划线覆盖，例如：

        DYN_DETAIL__MAX_SEM=30
        DYN_DETAIL__WORKER_MAX_TIMEOUT=600

    字段默认值与 ``UnlimitedCrawler`` 原有的硬编码默认值保持一致。
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


# ===== 各爬虫配置子类（集中管理） =====
# 仅覆盖与基类默认值不同的字段；运行时通过全局 Settings（CONFIG）注入，
# 可用对应环境变量（CRAWLER 字段名 + 双下划线 + 子字段名）覆盖。


class GetProxyMethodsConfig(CrawlerConfig):
    """代理获取爬虫配置（env: PROXY__*）"""

    max_sem: int = 10
    worker_max_timeout: int | None = 300
    requeue_on_fetch_fail: bool = False
    requeue_on_timeout: bool = True


class DynDetailScrapyConfig(CrawlerConfig):
    """动态详情爬虫配置（env: DYN_DETAIL__*）"""

    max_sem: int = 20
    worker_max_timeout: int | None = 300
    requeue_on_fetch_fail: bool = True
    requeue_on_timeout: bool = True
    log_timeout_error: bool = False


class GetRmFollowingListV2Config(CrawlerConfig):
    """取关对象爬虫配置（env: RM_FOLLOWING__*）"""

    max_sem: int = 100


class ReserveScrapyRobotConfig(CrawlerConfig):
    """预约抽奖爬虫配置（env: RESERVE__*）"""

    max_sem: int = 1
    worker_max_timeout: int | None = 300
    requeue_on_fetch_fail: bool = False
    requeue_on_timeout: bool = True


class TopicRobotConfig(CrawlerConfig):
    """话题抽奖爬虫配置（env: TOPIC__*）"""

    max_sem: int = 1
    worker_max_timeout: int | None = 300
    requeue_on_fetch_fail: bool = False
    requeue_on_timeout: bool = True


class LotteryApiRobotConfig(CrawlerConfig):
    """抽奖 API 爬虫配置（env: LOTTERY_API__*）

    max_sem 由运行时 ``sem_num`` 决定，故此处保持默认 1，
    调用方仍可通过 ``super().__init__(max_sem=...)`` 覆盖。
    """


class RefreshBiliLotDatabaseConfig(CrawlerConfig):
    """刷新抽奖数据库爬虫配置（env: REFRESH_LOT__*）"""

    max_sem: int = 1


class SamsClubCrawlerConfig(CrawlerConfig):
    """山姆分类爬虫配置（env: SAMS_CLUB__*）"""

    max_sem: int = 1
    requeue_on_fetch_fail: bool = False


class SamsClubSPUDetailCrawlerConfig(CrawlerConfig):
    """山姆 SPU 详情爬虫配置（env: SAMS_CLUB_SPU__*）"""

    max_sem: int = 1
    requeue_on_fetch_fail: bool = False


class BiliLiveCrawlerConfig(CrawlerConfig):
    """直播监控爬虫配置（env: BILI_LIVE__*）"""
