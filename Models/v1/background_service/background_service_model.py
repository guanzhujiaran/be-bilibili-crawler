from bili_common.models import StrEnumAutoDoc
from Service.BaseCrawler.plugin.statusPlugin import StatsPlugin
import time
from typing import Any

from pydantic import computed_field, Field

from Models.base.custom_pydantic import CustomBaseModel


class BackgroundServiceName(StrEnumAutoDoc):
    """后台服务名称枚举"""

    DYN_DETAIL_DATABASE_CLEANER = "DYN_DETAIL_DATABASE_CLEANER"
    TABLESPACE_MAINTENANCE = "TABLESPACE_MAINTENANCE"
    GET_PROXY_METHODS_SCHEDULER = "GET_PROXY_METHODS_SCHEDULER"
    SAMSCCLUB_SCHEDULER = "SAMSCCLUB_SCHEDULER"
    SAMSCCLUB_SPU_DETAIL_SCHEDULER = "SAMSCCLUB_SPU_DETAIL_SCHEDULER"
    GET_RESERVE_INFO = "GET_RESERVE_INFO"
    GET_DYN = "GET_DYN"
    GET_TOPIC = "GET_TOPIC"
    REFRESH_BILI_LOTDATA_DATABASE = "REFRESH_BILI_LOTDATA_DATABASE"
    LOTTERY_API_ROBOT_DYN_SCHEDULER = "LOTTERY_API_ROBOT_DYN_SCHEDULER"
    LOTTERY_API_ROBOT_RESERVE_SCHEDULER = "LOTTERY_API_ROBOT_RESERVE_SCHEDULER"
    GMFLV2_SCHEDULER = "GMFLV2_SCHEDULER"
    GET_OTHERS_LOT_DYN = "GET_OTHERS_LOT_DYN"
    STUCK_CHECK_SCHEDULER = "STUCK_CHECK_SCHEDULER"


class ScrapyTypeEnum(StrEnumAutoDoc):
    """可查询的爬虫类型枚举，对应 get_scrapy_status 的合法入参"""

    DYN = "dyn"
    TOPIC = "topic"
    RESERVE = "reserve"
    OTHER_SPACE = "other_space"
    OTHER_DYN = "other_dyn"
    REFRESH_BILI_OFFICIAL = "refresh_bili_official"
    REFRESH_BILI_RESERVE = "refresh_bili_reserve"


class ProgressStatusResp(CustomBaseModel):
    succ_count: int = 0
    start_ts: int = 0
    total_num: int = 0
    progress: float | int = Field(default=0, description="当前进度")
    is_running: bool = False
    update_ts: int = 0  # 最后更新时间
    running_params: set = Field(default_factory=set, description="运行中的参数")

    @computed_field
    def update_time(self) -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.update_ts))

    @computed_field
    def start_time(self) -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.start_ts))


class BaseScrapyStatusResp(CustomBaseModel):
    succ_count: int = 0
    cur_stop_num: int = 0
    start_ts: int = 0
    freq: int | float = Field(default=0, description="爬取成功的频率，单位为（条/秒）")
    is_running: bool = False
    update_ts: int = 0  # 最后更新时间


class ProxyStatusResp(CustomBaseModel):
    """代理池概览（口径全部来自 MySQL ``proxy_tab``；Redis 代理池已整体废弃）。

    ``proxy_unknown_count`` 是「从未被真实业务请求用过」的代理数
    （``success_times IS NULL OR success_times = 0``），
    与 ``proxy_usable_count``（``status=0`` 且未被判死）**不是互斥口径**：
    可用池里同时包含「从未使用」和「用过且成功过」两部分。
    因此 ``total != black + usable + unknown``，不要按旧口径（Redis zset 成员数）理解。
    """

    proxy_total_count: int = 0
    proxy_black_count: int = 0
    proxy_unknown_count: int = Field(
        default=0,
        description="从未被业务请求使用过的代理数（success_times IS NULL OR success_times = 0），"
        "与 proxy_usable_count 非互斥：可用池里既有从未使用的，也有用过且成功过的",
    )
    proxy_usable_count: int = 0
    free_proxy_fetch_ts: int = Field(
        default=0, description="最近一次入库代理的 add_ts（代理源最新产出时间，unix 秒）"
    )


TypeScrapyStatus = StatsPlugin | ProgressStatusResp | None


class AllLotScrapyStatusResp(CustomBaseModel):
    official_scrapy_status: TypeScrapyStatus
    reserve_scrapy_status: TypeScrapyStatus
    other_space_scrapy_status: TypeScrapyStatus
    dyn_scrapy_status: TypeScrapyStatus
    topic_scrapy_status: TypeScrapyStatus
