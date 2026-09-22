import asyncio
import sys
import io
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from loguru import logger

sys.path.append(os.path.join(os.path.dirname(__file__), '../'))  # 将CONFIG导入
current_dir = os.path.dirname(__file__)
grpc_dir = os.path.join(current_dir, 'service/GrpcModule/Grpc/GrpcProto')
sys.path.append(grpc_dir)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
if sys.platform.startswith('windows'):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())  # 祖传代码不可删，windows必须替换掉selector，不然跑一半就停了
else:
    import uvloop

    uvloop.install()
from Utils.argParse import parse
from CONFIG import settings

args = parse()
print(f'运行 args:{args}')
if not args.logger:
    # 与 main.py / dev_env_main.py 一致：级别由环境变量 LOG_LEVEL 决定
    print(f'日志输出：stdout sink 级别={settings.LOG_LEVEL}')
    # 只移除 loguru 默认的 stderr sink（id=0），其余 sink（含业务文件 sink）保持不动
    logger.remove(0)
    logger.add(
        sink=sys.stdout,
        level=settings.LOG_LEVEL,
        colorize=True,
        backtrace=False,
        diagnose=False,
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
