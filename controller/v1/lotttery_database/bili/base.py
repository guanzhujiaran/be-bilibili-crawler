import hashlib
from typing import Annotated, Optional

from fastapi import APIRouter, Query

from ApiRoutes import RouterPrefix, RouterTags


def new_router(dependencies=None):
    router = APIRouter()
    router.tags = [RouterTags.V1_BILI]
    router.prefix = RouterPrefix.BILI_LOTTERY
    if dependencies:
        router.dependencies = dependencies
    return router


# 防浏览器/CDN 缓存的时间戳查询参数：抽奖查询接口统一改为 GET，前端每次请求附带
# ``t = Date.now()`` 毫秒时间戳让 URL 每次不同；后端不使用该值（仅靠响应头 no-store
# 已可禁用缓存），显式声明是为了在 OpenAPI 契约中固定该参数并忽略其取值。
# 放在 base.py：LotteryData（查询）与 ScrapyStatus（控制面）两处都要用，
# 且控制面模块不能反过来 import 查询模块，否则查询服务会被迫拉起爬虫依赖链。
NoCacheTs = Annotated[
    Optional[int],
    Query(
        description="防缓存时间戳（前端传当前毫秒时间戳 Date.now()）；后端不使用该值，"
        "仅用于让每次 URL 不同以绕过浏览器/CDN 缓存"
    ),
]


def lottery_query_cache_key(
    func,
    namespace: str = "",
    *,
    request=None,
    response=None,
    args=(),
    kwargs=None,
) -> str:
    """抽奖查询接口的缓存 key：剔除防缓存时间戳 ``t``。

    fastapi_cache 默认 key 由函数入参生成，而前端每次请求都带不同的 ``t``
    （防浏览器缓存），照默认实现缓存永远不会命中。这里把 ``t`` 从 key 中剔除，
    其余入参（分页 / 筛选）照常参与，保证不同查询各自缓存。
    """
    filtered = {key: value for key, value in (kwargs or {}).items() if key != "t"}
    raw = f"{func.__module__}:{func.__qualname__}:{args}:{sorted(filtered.items())}"
    return f"{namespace}:{hashlib.md5(raw.encode()).hexdigest()}"

