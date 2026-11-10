# -*- coding: utf-8 -*-
import uvloop
import asyncio
import sys
import os

current_dir = os.path.dirname(__file__)
grpc_dir = os.path.join(current_dir, "Service/GrpcModule/Grpc/GrpcProto")
sys.path.append(grpc_dir)
if not sys.platform.startswith("win"):
    uvloop.install()
elif sys.platform.startswith("windows"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
from fastapi.responses import JSONResponse
from starlette.responses import Response
from starlette.requests import Request
from loguru import logger
from fastapi_cache.backends.inmemory import InMemoryBackend
from fastapi_cache import FastAPICache
from fastapi import FastAPI, HTTPException
import fastapi_cdn_host
import traceback
import time
from CONFIG import settings
from log.base_log import myfastapi_logger
from Utils.推送.PushMe import a_push_error
from Utils.通用.Common import mask_settings_for_log
from Utils.FastAPI.FastapiLifespan import life_span
from controller.v1.lotttery_database.bili import LotteryData, ScrapyStatus
from controller.v1.lotttery_database.bili.lottery_statistic import LotteryStatistic
from controller.v1.ip_info import get_ip_info
from controller.v1.background_service import BackgroundServiceController
from controller.common import CommonRouter, health
from controller.v1.mq import mq_controller as MQController
from controller.v1.mq.rpc_info_controller import router as RpcInfoRouter
from controller.v1.samsClub import samsClubController
from controller.v1.captcha import captchaController
from bili_common.exceptions import register_http_exception_handlers
from bili_common.middlewares import add_error_status_middleware
from bili_common.models.response import StandardResponse as CommonResponseModel
from controller.v1.lotttery_database.bili.zhuanlan import zhuanlanController


logger.info("运行 settings:{}", mask_settings_for_log(settings))
if settings.is_api_only:
    # 本地开发用：只把接口挂出去，不迁移 / 不校验 schema / 不启调度器 / 不消费 MQ
    logger.warning(
        "当前 SERVICE_ROLE=api：只提供接口，不迁移 / 不调度 / 不消费 MQ / 不自动爬取"
    )
if not settings.SHOW_LOG:
    # 只移除 loguru 默认的 stderr sink（id=0，loguru 在 import 时注册），
    # 保留 log.base_log 注册的业务文件 sink。
    # 原先的 logger.remove() 会把文件 sink 一并删掉，等于把 WARNING 以上的业务日志静默丢弃。
    logger.remove(0)
    # backtrace/diagnose 关闭：堆栈只保留「调用链 + 异常本身」，
    # 不再打印每一帧的局部变量值（loguru 默认开启会产生大量冗余信息）。
    logger.add(
        sink=sys.stdout,
        level=settings.LOG_LEVEL,
        colorize=True,
        backtrace=False,
        diagnose=False,
    )
    # 用 print 而非 logger：该自述信息需要在任何 LOG_LEVEL 下都可见（与 dev_env_main.py 一致）
    print(f"日志输出：stdout sink 级别={settings.LOG_LEVEL}（本地文件 sink 保持不变）")


app = FastAPI(lifespan=life_span)
fastapi_cdn_host.patch_docs(app)
# 写入 / 控制面路由（Add*、爬虫状态）与只读查询路由分属两个 APIRouter：
# 爬虫角色两个都挂（对外路径与拆分前一致），查询角色（query_main.py）只挂 query_router
app.include_router(LotteryData.router)
app.include_router(LotteryData.query_router)
# 爬虫实时状态（控制面）：读进程内 background_service 单例，只有爬虫角色需要
app.include_router(ScrapyStatus.router)
app.include_router(LotteryStatistic.router)
app.include_router(get_ip_info.router)
app.include_router(BackgroundServiceController.router)
app.include_router(CommonRouter.router)
# 轻量探活（GET /health）：网关上游探活打这里，不再打根路径产生 404 噪音
app.include_router(health.router)
# MQ 消费者与 RPC 信息路由只有爬虫角色挂：
# faststream 的 RabbitRouter 一旦被 include，FastAPI 就会在启动时连 broker 并按
# @router.subscriber 开始**消费队列**（入库 / Milvus / 抽奖结果抽取等真实动作）。
# SERVICE_ROLE=api（只提供接口、本地开发用）不应该有消费者，其余路由照常挂载。
if not settings.is_api_only:
    # 本角色只挂 bili_data 的写库消费者（MQ 消息），不注册任何 RPC：
    # RPC（FastapiApp.rpc.*）一律是只读取数，由 query 角色（query_main.py）提供。
    app.include_router(MQController.router)
    app.include_router(RpcInfoRouter)
app.include_router(captchaController.router)
app.include_router(samsClubController.router)
app.include_router(zhuanlanController.router)
FastAPICache.init(InMemoryBackend(), prefix="fastapi-cache")

# 统一错误响应：HTTP 异常（404/405…）与参数校验失败统一为 {code, msg, data} + 非 200 状态码
# （Exception 兜底仍在下方 base_error_handler，保留告警推送）。
register_http_exception_handlers(app)
# 兜底：业务代码里「HTTP 200 + 非 0 业务码」的响应统一改写成非 200 状态码。
add_error_status_middleware(app)


@app.middleware("http")
async def global_handler(request: Request, call_next):
    request_ip = request.client.host if request.client else "unknown"
    start_ts = time.time()
    response = await call_next(request)
    # 可选：仅在调试模式下返回错误堆栈
    if hasattr(app, "debug"):
        end_ts = time.time()
        response_body = b""
        async for chunk in response.body_iterator:
            response_body += chunk
        myfastapi_logger.info(
            f"请求耗时: {end_ts - start_ts:.4f}秒\n"
            f"请求IP: {request_ip}\n"
            f"请求方法: {request.method}\n"
            f"请求路径: {request.url.path}\n"
            f"响应状态码: {response.status_code}\n"
            f"响应体: {response_body.decode(errors='replace')}"
        )
        # 构造新的响应对象，使用原始的响应体迭代器
        return Response(
            content=response_body,
            status_code=response.status_code,
            headers=dict(response.headers),
            media_type=response.media_type,
        )
    else:
        # 在非调试模式下，直接返回原始响应
        return response


# 抽奖查询接口已统一由 POST 改为 GET。fastapi-cache 的 @cache 在命中/回源时会给响应回写
# ``Cache-Control: max-age=<ttl>``，GET 响应可能被浏览器/CDN 据此缓存。这里在出站阶段对
# 抽奖数据库下所有 GET 查询响应强制覆盖为「完全不缓存（缓存策略=0）」，与前端每次请求
# 附带的 ``t=当前时间戳`` 双保险，确保前端永远拿到最新数据、不读浏览器旧缓存。
_LOTTERY_QUERY_PREFIX = "/api/v1/lottery_database/"


@app.middleware("http")
async def no_cache_lottery_queries(request: Request, call_next):
    response = await call_next(request)
    if request.method == "GET" and request.url.path.startswith(_LOTTERY_QUERY_PREFIX):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate, max-age=0"
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
    myfastapi_logger.exception(f"FastAPI请求异常: {error_detail}")
    err_title = str(object=exc).replace("\n", "")
    await a_push_error(
        subject="FastAPI请求异常",
        content=f"URL: {request.url}\n错误详情: {err_title}\n{traceback.format_exc()}",
    )
    return JSONResponse(
        status_code=500,
        content=CommonResponseModel(code=-500, msg=str(exc)).model_dump(),
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=23333)
