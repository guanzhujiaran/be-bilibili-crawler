# -*- coding: utf-8 -*-
"""be-bilibili-crawler 的「纯查询」入口（SERVICE_ROLE=query）。

与 ``main.py`` 共用同一份源码、同一个镜像，只是 Dockerfile 里换了 target
（``crawler`` / ``crawler-query``）与启动命令，因此可以两个容器并行跑：

    crawler        23333  爬虫 + 写入 + 控制面（main.py，行为与拆分前一致）
    crawler-query  23336  只读查询（本文件）

本入口刻意不挂载的东西（挂上就等于把爬虫依赖重新绑回来）：

- ``MqController``（bili_data 的入库消费者：抽奖入库 / Milvus / 奖品抽取等）；
- ``BackgroundServiceController`` / ``zhuanlanController``：控制面与抓取链路；
- 写入路由 ``LotteryData.router``（Add* 系列）：查询服务连只读账号时不该有写能力。

**但只读取数 RPC（``FastapiApp.rpc.*`` 的 READ 类）由本入口提供**（2.70.0 读写分离）：
RPC handler 与 ``query_router`` 是同一批只读函数，放这里才能做到「爬虫挂了 RPA 也能取数」，
且天然不可能写库。写 RPC（如果有）只挂在 crawler 角色，两个角色不会注册同一个队列。

启动只做「读库连通性探测」，不做 alembic 迁移、不做 schema 校验、不初始化 Milvus。
"""

import asyncio
import os
import sys
import time

import uvloop

current_dir = os.path.dirname(__file__)
grpc_dir = os.path.join(current_dir, "Service/GrpcModule/Grpc/GrpcProto")
sys.path.append(grpc_dir)
if not sys.platform.startswith("win"):
    uvloop.install()
elif sys.platform.startswith("windows"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from loguru import logger
from starlette.requests import Request
import fastapi_cdn_host
import traceback

from CONFIG import settings
from log.base_log import myfastapi_logger
from Utils.FastAPI.QueryFastapiLifespan import query_life_span
from Utils.推送.PushMe import a_push_error
from bili_common.exceptions import register_http_exception_handlers
from bili_common.middlewares import add_error_status_middleware
from bili_common.models.response import StandardResponse as CommonResponseModel
from controller.v1.ip_info import get_ip_info
from controller.v1.lotttery_database.bili import LotteryData
from controller.v1.lotttery_database.bili.lottery_statistic import LotteryStatistic

# 导入只读 RPC handler 模块（副作用：@rpc_subscriber 把 8 个 READ 方法注册到 router 上），
# 随后 include 该 router 才会真正建队列并消费 FastapiApp.rpc.*
import controller.v1.mq.rpc_read_lottery_data
from controller.v1.mq.rpc_server import (
    registered_rpc_methods,
    router as rpc_router,
)


logger.info(
    "查询服务启动 | SERVICE_ROLE={} | MYSQL={}:{}",
    settings.SERVICE_ROLE.value,
    settings.MYSQL_HOST,
    settings.MYSQL_PORT,
)
if not settings.is_query_only:
    logger.warning(
        "当前 SERVICE_ROLE={}，但入口是 query_main：路由仍只有只读集合，"
        "仅建议本地调试这样跑",
        settings.SERVICE_ROLE.value,
    )

if not settings.SHOW_LOG:
    # 与 main.py 一致：只移除 loguru 默认 stderr sink，保留 log/base_log 注册的文件 sink
    logger.remove(0)
    logger.add(
        sink=sys.stdout,
        level=settings.LOG_LEVEL,
        colorize=True,
        backtrace=False,
        diagnose=False,
    )
    print(f"日志输出：stdout sink 级别={settings.LOG_LEVEL}（本地文件 sink 保持不变）")


app = FastAPI(lifespan=query_life_span)
fastapi_cdn_host.patch_docs(app)

# 只读查询路由：抽奖库查询 + 抽奖统计 + IP 归属地
app.include_router(LotteryData.query_router)
app.include_router(LotteryStatistic.router)
app.include_router(get_ip_info.router)
# 只读取数 RPC 服务端（FastapiApp.rpc.*）：FastAPI 会自动 start / stop 该 broker
app.include_router(rpc_router)
myfastapi_logger.info(
    "只读取数 RPC 服务端已注册：{}（写库走 bili_data 普通 MQ 消息，由 crawler 容器消费）".format(
        registered_rpc_methods() or "（无）"
    )
)

# LotteryData 里十余个 GET 带 @cache(expire=...)，未 init 会直接 500
FastAPICache.init(InMemoryBackend(), prefix="fastapi-cache-query")

register_http_exception_handlers(app)
add_error_status_middleware(app)


@app.middleware("http")
async def log_query_requests(request: Request, call_next):
    request_ip = request.client.host if request.client else "unknown"
    start_ts = time.time()
    response = await call_next(request)
    myfastapi_logger.info(
        f"查询请求耗时: {time.time() - start_ts:.4f}秒 | IP: {request_ip} | "
        f"{request.method} {request.url.path} -> {response.status_code}"
    )
    return response


# 抽奖查询一律不允许被浏览器 / CDN 缓存（与 main.py 的策略保持一致）
_LOTTERY_QUERY_PREFIX = "/api/v1/lottery_database/"


@app.middleware("http")
async def no_cache_lottery_queries(request: Request, call_next):
    response = await call_next(request)
    if request.method == "GET" and request.url.path.startswith(_LOTTERY_QUERY_PREFIX):
        response.headers["Cache-Control"] = (
            "no-cache, no-store, must-revalidate, max-age=0"
        )
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


@app.exception_handler(exc_class_or_status_code=Exception)
async def base_error_handler(request: Request, exc: Exception):
    request_ip = request.client.host if request.client else "unknown"
    error_detail = {
        "error": str(exc),
        "request_url": str(request.url),
        "request_method": request.method,
        "client_host": request_ip,
    }
    myfastapi_logger.exception(f"查询服务请求异常: {error_detail}")
    err_title = str(object=exc).replace("\n", "")
    await a_push_error(
        subject="查询服务请求异常",
        content=f"URL: {request.url}\n错误详情: {err_title}\n{traceback.format_exc()}",
    )
    return JSONResponse(
        status_code=500,
        content=CommonResponseModel(code=-500, msg=str(exc)).model_dump(),
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=23336)
