"""爬虫推送开关的请求 / 响应模型（本地文件持久化，不入库）"""

from pydantic import BaseModel, Field


class CrawlerPushSwitchItem(BaseModel):
    """单个爬虫的推送开关"""

    crawler_name: str = Field(description="爬虫名，取值同 BackgroundServiceName")
    start: bool = Field(default=False, description="开始执行时是否推送")
    end: bool = Field(default=False, description="结束时是否推送")


class CrawlerPushState(BaseModel):
    """推送开关的持久化内容（整体覆盖式保存）"""

    items: list[CrawlerPushSwitchItem] = Field(
        default_factory=list, description="按爬虫配置的开关列表，未列出的爬虫回退到环境变量"
    )
    updated_at: int | None = Field(default=None, description="最近保存时间（秒级时间戳）")


class CrawlerPushEffectiveItem(BaseModel):
    """合并「文件配置 + 环境变量」之后的生效状态"""

    crawler_name: str
    start: bool = False
    end: bool = False
    source: str = Field(description="配置来源：file / env")


class CrawlerPushStateResp(BaseModel):
    """查询推送开关的响应"""

    state: CrawlerPushState = Field(description="文件里持久化的内容")
    env_start: str = Field(default="", description="环境变量 CRAWLER_PUSH_START 原值")
    env_end: str = Field(default="", description="环境变量 CRAWLER_PUSH_END 原值")
    state_file: str = Field(default="", description="开关文件落盘路径（排查用）")
    effective: list[CrawlerPushEffectiveItem] = Field(
        default_factory=list, description="每个爬虫的最终生效状态"
    )
