import asyncio
import sys
import io
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from loguru import logger

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))  # 将CONFIG导入
current_dir = os.path.dirname(__file__)
# 目录名是 Service（大写）：原先写成 service，Linux 下大小写敏感 → 路径不存在，
# grpc 生成的 bilibili.* 包导不进来（ModuleNotFoundError: No module named 'bilibili'）
grpc_dir = os.path.join(current_dir, "Service/GrpcModule/Grpc/GrpcProto")
sys.path.append(grpc_dir)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
if sys.platform.startswith("windows"):
    asyncio.set_event_loop_policy(
        asyncio.WindowsProactorEventLoopPolicy()
    )  # 祖传代码不可删，windows必须替换掉selector，不然跑一半就停了
else:
    import uvloop

    uvloop.install()
from CONFIG import settings

# 与 main.py 对齐：SHOW_LOG=0 时把 stdout sink 的级别交给 LOG_LEVEL，业务文件 sink 保持不变。
# （原先这里依赖已被删除的 Utils.argParse.parse()，导致该入口一启动就 ModuleNotFoundError）
if not settings.SHOW_LOG:
    # 只移除 loguru 默认的 stderr sink（id=0），其余 sink（含业务文件 sink）保持不动
    logger.remove(0)
    logger.add(
        sink=sys.stdout,
        level=settings.LOG_LEVEL,
        colorize=True,
        backtrace=False,
        diagnose=False,
    )
    print(f"日志输出：stdout sink 级别={settings.LOG_LEVEL}（本地文件 sink 保持不变）")
print(
    f"MQ / RPC 消费服务启动：SERVICE_ROLE={settings.SERVICE_ROLE.value}（端口 23334）"
)
if settings.is_api_only:
    logger.warning(
        "当前 SERVICE_ROLE=api：该角色本不应启动 MQ / RPC 消费服务，"
        "请用 SERVICE_ROLE=crawler 启动"
    )
from controller.v1.mq.mq_controller import router
import fastapi
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend


@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield


app = fastapi.FastAPI(lifespan=lifespan)
FastAPICache.init(InMemoryBackend(), prefix="fastapi-cache")
app.include_router(router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=23334, loop="uvloop")
