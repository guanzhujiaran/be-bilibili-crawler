from Service.BaseCrawler.plugin.statusPlugin import StatsPlugin
import time
from enum import Enum
from typing import Any

from pydantic import computed_field, Field

from Models.base.custom_pydantic import CustomBaseModel

class BackgroundServiceName(str, Enum):
    """后台服务名称枚举"""
    DYN_DETAIL_DATABASE_CLEANER = "DYN_DETAIL_DATABASE_CLEANER"
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


class ScrapyTypeEnum(str, Enum):
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
    progress: float | int = Field(default=0, description='当前进度')
    is_running: bool = False
    update_ts: int = 0  # 最后更新时间
    running_params: set = Field(default_factory=set, description='运行中的参数')

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
    freq: int | float = Field(default=0, description='爬取成功的频率，单位为（条/秒）')
    is_running: bool = False
    update_ts: int = 0  # 最后更新时间


class ProxyStatusResp(CustomBaseModel):
    proxy_total_count: int = 0
    proxy_black_count: int = 0
    proxy_unknown_count: int = 0
    proxy_usable_count: int = 0
    mysql_sync_redis_ts: int = 0
    free_proxy_fetch_ts: int = 0
    sync_ts: int = 0  # 同步到redis的时间

    def is_need_sync(self) -> bool:
        return not (bool(self.sync_ts) and self.sync_ts > int(time.time()) - 600)

TypeScrapyStatus = StatsPlugin | ProgressStatusResp | None


class AllLotScrapyStatusResp(CustomBaseModel):
    official_scrapy_status: TypeScrapyStatus
    reserve_scrapy_status: TypeScrapyStatus
    other_space_scrapy_status: TypeScrapyStatus
    dyn_scrapy_status: TypeScrapyStatus
    topic_scrapy_status: TypeScrapyStatus
