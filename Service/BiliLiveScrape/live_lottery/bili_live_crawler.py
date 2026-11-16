import time
from typing import Any, AsyncGenerator

from Service.BaseCrawler.CrawlerType import UnlimitedCrawler
from Service.BaseCrawler.config import BiliLiveCrawlerConfig
from Service.BaseCrawler.model.base import ParamsType, WorkerStatus
from Service.GrpcModule.Grpc.Bapi.BiliApi import (
    xlive_web_interface_v1_index_getWebAreaList,
)
from Utils.代理.mdoel.RequestConf import RequestConf


class BiliLiveCrawler(UnlimitedCrawler[int]):
    Config = BiliLiveCrawlerConfig

    async def is_stop(self) -> bool:
        return False

    async def key_params_gen(
        self, params: ParamsType
    ) -> AsyncGenerator[ParamsType, None]:
        # TODO 这里维护一个列表，根据时间返回房间号，
        sync_live_area_ts = 0
        while 1:
            if int(time.time()) - sync_live_area_ts > 24 * 60 * 60:
                # IPv6 专线优先；专线冷却 / 本次失败被排除后，按 FAST 档回落 hot/warm。
                # 之前这里传的是 use_custom_proxy=True（函数没有该参数，会 TypeError），
                # 实际一直是按权重抽池；现在改为显式通道语义。
                live_area_list_resp = await xlive_web_interface_v1_index_getWebAreaList(
                    request_conf=RequestConf(is_use_custom_proxy=True)
                )
                self.live_area_list = live_area_list_resp.get("data", {}).get(
                    "data", []
                )
                sync_live_area_ts = int(time.time())

    async def handle_fetch(self, params: ParamsType) -> WorkerStatus | Any:
        pass

    def __init__(self):
        # 配置（logger / 超时 / 重试 / 插件等）统一由 BiliLiveCrawlerConfig 控制
        super().__init__()
        self.live_area_list = []

    async def main(self): ...
