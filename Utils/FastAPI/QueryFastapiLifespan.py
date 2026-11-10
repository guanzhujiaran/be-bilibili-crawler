"""纯查询角色（SERVICE_ROLE=query）的 lifespan。

刻意与 ``Utils/FastAPI/FastapiLifespan.py`` 分开，原因：
那个模块在**导入时**就会拉起 MQ 发布器、alembic 管理器、后台调度控制器，
把它 import 进查询服务等于把爬虫依赖重新绑回来。

本模块只做一件事：确认查询真正用到的几个库能连上。
不做 alembic 迁移（DDL 只允许由爬虫角色或部署流程执行）、
不做 schema 校验、不初始化 Milvus、不重投 MQ 消息、不启动定时任务。

注意（2.70.0）：本角色**会**挂 RPC 服务端 —— `FastapiApp.rpc.*` 全部是只读取数
（见 rpc_read_lottery_data.py），broker 由 FastAPI 随 include_router 自动 start / stop；
写库不走 RPC 而走 bili_data 的普通 MQ 消息（crawler 角色消费），
所以本角色依然不消费入库队列、也不做任何写库动作。
"""

import asyncio
import contextlib

from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import create_async_engine

from CONFIG import CONFIG
from Service.llm_service import config_store
from log.base_log import myfastapi_logger

# 单库探活的总超时（秒），必须用外层 ``asyncio.timeout`` 而不是 SQLAlchemy 的
# ``connect_args={"connect_timeout": ...}``：aiomysql 的 connect_timeout 只包住
# TCP 建连（``_open_connection``），之后的 ``_get_server_information()``
# 等握手包不在其保护范围内。于是「TCP 三次握手成功但服务端一个字节都不回」的路径
# （典型：防火墙 SYN 代理 / 端口转发进程卡死）会永久阻塞在
# ``Waiting for application startup``，既不报错也不退出，排查时毫无线索。
_DB_CONNECT_TIMEOUT_SECONDS = 10


@contextlib.asynccontextmanager
async def query_life_span(app: FastAPI):
    # 与爬虫角色保持同一份 LLM 配置（只读）：本角色不调用 LLM、也不提供 /llm/* 写接口，
    # 加载只为「两容器看到同一份列表」；读不到文件时回退 .env，不影响启动。
    # 只读角色不写共享配置文件，脱敏 token 自愈由 crawler 角色负责
    await config_store.apply_to_settings_async(repair=False)
    await test_query_database_connections()
    myfastapi_logger.info(
        "查询服务启动完成（只读模式：无迁移 / 无调度 / 无入库消费；"
        "FastapiApp.rpc.* 只读取数 RPC 服务端已随 router 挂载；写库走 MQ）"
    )
    yield
    myfastapi_logger.info("查询服务正在退出")


async def test_query_database_connections():
    """只探测查询实际会读的库；连不上直接拒绝启动。

    与爬虫角色的 ``test_database_connections`` 相比少两个库
    （proxy_db / samsclub 只有爬虫侧使用），并且**不做**任何写入或 DDL。
    """
    databases = {
        "bilidb": CONFIG.database.MYSQL.bili_db_URI,
        "bili_reserve": CONFIG.database.MYSQL.bili_reserve_URI,
        "get_other_lot": CONFIG.database.MYSQL.get_other_lot_URI,
        "dyndetail": CONFIG.database.MYSQL.dyn_detail_URI,
    }

    for db_name, db_uri in databases.items():
        engine = create_async_engine(db_uri)
        try:
            myfastapi_logger.info(f"正在测试数据库 '{db_name}' 连接...")
            # 外层硬超时：覆盖「建连 + 等握手包 + 认证 + SELECT 1」全过程
            async with asyncio.timeout(_DB_CONNECT_TIMEOUT_SECONDS):
                async with engine.connect() as conn:
                    await conn.execute(text("SELECT 1"))
            myfastapi_logger.info(f"数据库 '{db_name}' 连接成功")
        except TimeoutError as e:
            myfastapi_logger.critical(
                f"数据库 '{db_name}' 连接超时（>{_DB_CONNECT_TIMEOUT_SECONDS}s）"
                f" | URI={db_uri} | 错误: {e!r}"
            )
            raise
        except OperationalError as e:
            myfastapi_logger.critical(
                f"数据库 '{db_name}' 连接失败 | URI={db_uri} | 错误: {e}"
            )
            raise
        except Exception as e:
            myfastapi_logger.critical(
                f"数据库 '{db_name}' 连接失败 | URI={db_uri} | 错误: {e}"
            )
            raise SystemExit(f"数据库 '{db_name}' 连接失败") from e
        finally:
            # 无论成功、超时还是异常都要释放连接池，避免探活留下的 engine 泄漏
            await engine.dispose()


__all__ = ["query_life_span"]
