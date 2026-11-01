"""爬虫实时状态查询（控制面接口）。

刻意从 ``LotteryData.py`` 拆出来：
这两个接口读的是爬虫进程内的单例状态（``background_service``），
和「抽奖数据查询」不是一回事。留在 LotteryData 里会迫使
纯查询服务（``query_main.py``）import ``Service.BackgroundServiceStatus``，
从而把爬虫调度 / 抓取依赖链整条拉进查询进程。

对外路径保持不变（同前缀 ``/api/v1/lottery_database/bili``），
只有爬虫角色（``main.py``）挂载本 router。
"""

from fastapi import Query

from ApiRoutes import RouterPaths, RouterNames
from Models.v1.background_service.background_service_model import (
    AllLotScrapyStatusResp,
    ScrapyTypeEnum,
    TypeScrapyStatus,
)
from Service.BackgroundServiceStatus.GetScrapyStaus import get_scrapy_status
from bili_common.models.response import StandardResponse as CommonResponseModel

from .base import NoCacheTs, new_router

router = new_router()


@router.get(
    RouterPaths.GET_SINGLE_SCRAPY_STATUS,
    name=RouterNames.GET_SINGLE_SCRAPY_STATUS,
    description="根据爬虫类型查询单个爬虫的状态（GET，前端附带 t 时间戳防浏览器缓存）",
    response_model=CommonResponseModel[TypeScrapyStatus],
    response_model_exclude_none=True,
)
def get_single_scrapy_status(
    scrapy_name: ScrapyTypeEnum = Query(..., description="爬虫类型"),
    t: NoCacheTs = None,
):
    """
    根据传入的爬虫类型查询对应的单个爬虫实时状态

    :param scrapy_name: 爬虫类型，必须是 ScrapyTypeEnum 中的合法值
        （dyn / topic / reserve / other_space / other_dyn /
         refresh_bili_official / refresh_bili_reserve）
    :return: 对应爬虫的状态信息
    """
    return CommonResponseModel(data=get_scrapy_status(scrapy_name.value))


@router.get(
    RouterPaths.GET_ALL_LOT_SCRAPY_STATUS,
    name=RouterNames.GET_ALL_LOT_SCRAPY_STATUS,
    description="获取所有爬虫状态",
    response_model=CommonResponseModel[AllLotScrapyStatusResp],
    response_model_exclude_none=True,
)
def get_all_scrapy_status():
    return CommonResponseModel(
        data=AllLotScrapyStatusResp(
            official_scrapy_status=get_scrapy_status("refresh_bili_official"),
            reserve_scrapy_status=get_scrapy_status("reserve"),
            other_space_scrapy_status=get_scrapy_status("other_space"),
            dyn_scrapy_status=get_scrapy_status("dyn"),
            topic_scrapy_status=get_scrapy_status("topic"),
        )
    )
