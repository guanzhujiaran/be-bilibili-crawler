import asyncio
import contextlib
import socket

from fastapi import FastAPI
import httpx
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import create_async_engine
from CONFIG import settings, CONFIG
from Service.LangChainCompo.lottery_data_vec_sql.sql_helper import milvus_sql_helper
from Service.MQ.base.MQClient.BiliLotDataPublisher import BiliLotDataPublisher
from Utils.FastAPI.alembic_manager import check_schemas, run_alembic_upgrade_head
from Utils.通用.Common import GLOBAL_SCHEDULER, asyncio_gather
from controller.v1.background_service import BackgroundServiceController
from log.base_log import myfastapi_logger


@contextlib.asynccontextmanager
async def life_span(app: FastAPI):
    # 测试数据库连接
    try:
        await test_database_connections()
    except OperationalError as e:
        myfastapi_logger.critical(f"数据库（{CONFIG.database.MYSQL._base_url}）连接测试失败: {e}")
        raise e
    # 测试各个服务的端口和 host 连通性（含 message-service 的 /health 探测）
    await test_service_ports_and_hosts()

    # ---- 启动期自动执行 alembic upgrade head 增量迁移 ----
    if not await run_alembic_upgrade_head():
        raise RuntimeError("alembic upgrade head 执行失败，请检查数据库连接与迁移脚本")

    # ---- Schema 一致性校验：不一致则拒绝启动 ----
    if not await check_schemas():
        raise RuntimeError("数据库 Schema 与模型不一致，请先手动执行 alembic upgrade head")

    # 检查 milvus 数据库集合
    await asyncio.sleep(3)
    myfastapi_logger.critical("检查 milvus 数据库集合")
    await milvus_sql_helper.ensure_collection_exists()
    myfastapi_logger.critical("重试未处理的消息")
    await BiliLotDataPublisher.retry_pending_messages()
    # RPC handler 由 mq_controller.py 末尾导入 lottery_data 模块触发 @rpc_subscriber 注册
    # broker 连接由 FastAPI 通过 app.include_router(MqController.router) 自动管理
    myfastapi_logger.critical("开启其他服务")
    back_ground_tasks = []
    if settings.IS_DEV:
        myfastapi_logger.critical("开发环境不启动定时任务喵~")
    else:
        show_log = False
        GLOBAL_SCHEDULER.start()
        back_ground_tasks = BackgroundServiceController.start_monitor_tasks(
            show_log=show_log)
        myfastapi_logger.critical("其他服务已开启！可以开启服务了喵~")
        try:
            from Service.GetOthersLotDyn.core.get_others_lot_dyn import get_others_lot_dyn
            myfastapi_logger.critical("启动时预填充用户列表...")
            supp_summary = await get_others_lot_dyn._supplement_users()
            myfastapi_logger.critical(
                f"启动补充完成: {supp_summary['before_count']} -> {supp_summary['after_count']}个用户"
            )
        except Exception as e:
            myfastapi_logger.error(f"启动时补充用户列表失败: {e}")
    yield
    myfastapi_logger.critical("正在取消其他服务")
    [x.cancel() for x in back_ground_tasks]
    await asyncio_gather(*back_ground_tasks, log=myfastapi_logger)
    myfastapi_logger.critical("其他服务已取消")


async def test_database_connections():
    """测试所有数据库连接，如果连接失败直接报错退出"""
    databases = {
        "proxy_db": CONFIG.database.MYSQL.proxy_db_URI,
        "bilidb": CONFIG.database.MYSQL.bili_db_URI,
        "bili_reserve": CONFIG.database.MYSQL.bili_reserve_URI,
        "get_other_lot": CONFIG.database.MYSQL.get_other_lot_URI,
        "dyndetail": CONFIG.database.MYSQL.dyn_detail_URI,
        "sams_club": CONFIG.database.MYSQL.sams_club_URI,
    }

    for db_name, db_uri in databases.items():
        try:
            myfastapi_logger.info(f"正在测试数据库 '{db_name}' 连接...")
            engine = create_async_engine(db_uri)
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            await engine.dispose()
            myfastapi_logger.info(f"数据库 '{db_name}' 连接成功")
        except OperationalError as e:
            myfastapi_logger.critical(
                f"数据库 '{db_name}' 连接失败 | URI={db_uri} | 错误: {e}")
            raise
        except Exception as e:
            myfastapi_logger.critical(
                f"数据库 '{db_name}' 连接失败 | URI={db_uri} | 错误: {e}")
            raise SystemExit(f"数据库 '{db_name}' 连接失败") from e


async def test_port_connectivity(host: str, port: int, service_name: str, timeout: float = 5.0) -> bool:
    """测试指定 host 和 port 的连通性"""
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=timeout,
        )
        writer.close()
        await writer.wait_closed()
        myfastapi_logger.info(f"服务 '{service_name}' ({host}:{port}) 连接成功")
        return True
    except asyncio.TimeoutError:
        myfastapi_logger.error(f"服务 '{service_name}' ({host}:{port}) 连接超时")
        return False
    except ConnectionRefusedError:
        myfastapi_logger.error(f"服务 '{service_name}' ({host}:{port}) 连接被拒绝")
        return False
    except OSError as e:
        myfastapi_logger.error(f"服务 '{service_name}' ({host}:{port}) 连接失败：{e}")
        return False
    except Exception as e:
        myfastapi_logger.error(f"服务 '{service_name}' ({host}:{port}) 连接异常：{e}")
        return False


async def test_http_endpoint(url: str, service_name: str, timeout: float = 5.0) -> bool:
    """测试 HTTP 端点的连通性"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=timeout)
            myfastapi_logger.info(
                f"HTTP 服务 '{service_name}' ({url}) 连接成功，状态码：{response.status_code}")
            return True
    except httpx.TimeoutException:
        myfastapi_logger.error(f"HTTP 服务 '{service_name}' ({url}) 连接超时")
        return False
    except httpx.RequestError as e:
        myfastapi_logger.error(f"HTTP 服务 '{service_name}' ({url}) 连接失败：{e}")
        return False
    except Exception as e:
        myfastapi_logger.error(f"HTTP 服务 '{service_name}' ({url}) 连接异常：{e}")
        return False


async def test_service_ports_and_hosts():
    """测试 CONFIG 中设置的所有服务的端口和 host 连通性。

    kind="port" -> TCP 端口探测（test_port_connectivity）；
    kind="http" -> HTTP 端点探测（test_http_endpoint，path 为可选路径）。

    critical=True 的服务若连接失败，将直接终止启动（SystemExit），
    其它非关键服务仅记录警告，不阻断启动。
    """
    myfastapi_logger.critical("开始测试各服务端口和 host 连通性...")

    failed_services = []
    failed_critical_services = []

    # (name, host, port, kind, path, critical)
    services_to_test = [
        ("MySQL", settings.MYSQL_HOST, int(settings.MYSQL_PORT), "port", "", True),
        ("Redis", settings.REDIS_HOST, int(settings.REDIS_PORT), "port", "", True),
        ("RabbitMQ", settings.RABBITMQ_HOST, int(
            settings.RABBITMQ_PORT), "port", "", True),
        ("Milvus", settings.MILVUS_HOST, int(settings.MILVUS_PORT), "port", "", True),
        ("Unidbg", settings.UNIDBG_HOST, int(settings.UNIDBG_PORT), "http", "", False),
        ("V2Ray", settings.V2RAY_HOST, int(settings.V2RAY_PORT), "http", "", False),
        ("llama.cpp", settings.LLAMA_HOST, int(settings.LLAMA_PORT), "http", "", True),
        ("message-service", settings.MESSAGE_SERVICE_HOST,
         int(settings.MESSAGE_SERVICE_PORT), "http", "/health", True),
    ]

    for name, host, port, kind, path, critical in services_to_test:
        if kind == "port":
            ok = await test_port_connectivity(host, port, name)
            address = f"{host}:{port}"
        else:
            url = f"http://{host}:{port}{path}"
            ok = await test_http_endpoint(url, name)
            address = url
        if not ok:
            failed_services.append(f"{name} ({address})")
            if critical:
                failed_critical_services.append(f"{name} ({address})")

    if failed_services:
        myfastapi_logger.critical("=" * 60)
        myfastapi_logger.critical("以下服务连接失败:")
        for failed_service in failed_services:
            myfastapi_logger.critical(f"  ❌ {failed_service}")
        myfastapi_logger.critical("=" * 60)
    else:
        myfastapi_logger.critical("✅ 所有服务端口和 host 连通性测试通过!")

    # 关键服务未启动 -> 直接报错并拒绝启动
    if failed_critical_services:
        myfastapi_logger.critical("=" * 60)
        myfastapi_logger.critical(
            f"以下关键服务未启动，禁止服务启动: {len(failed_critical_services)} 个")
        for failed_service in failed_critical_services:
            myfastapi_logger.critical(f"  🚫 {failed_service}")
        myfastapi_logger.critical("=" * 60)
        raise SystemExit(
            f"关键依赖服务未启动，拒绝启动: {', '.join(failed_critical_services)}")


__all__ = ["life_span"]

if __name__ == '__main__':
    asyncio.run(test_database_connections())
