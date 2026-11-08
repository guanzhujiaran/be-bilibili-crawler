import asyncio
import contextlib
import re
import socket

from fastapi import FastAPI
import httpx
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import create_async_engine
from CONFIG import settings, CONFIG
from Service.LangChainCompo.lottery_data_vec_sql.sql_helper import milvus_sql_helper
from Service.llm_service import config_store
from Service.MQ.base.MQClient.BiliLotDataPublisher import BiliLotDataPublisher
from Utils.FastAPI.alembic_manager import check_schemas, run_alembic_upgrade_head
from Utils.通用.Common import GLOBAL_SCHEDULER, asyncio_gather
from controller.v1.background_service import BackgroundServiceController
from log.base_log import myfastapi_logger

# 单库探活的总超时（秒）。必须用外层 ``asyncio.timeout``：aiomysql 的
# ``connect_args={"connect_timeout": ...}`` 只包住 TCP 建连（``_open_connection``），
# 之后的 ``_get_server_information()`` 等握手包不在其保护范围内。
# 于是「TCP 三次握手成功但服务端一个字节都不回」的路径（典型：防火墙 SYN 代理 /
# 端口转发进程卡死）会永久阻塞在 ``Waiting for application startup``，
# 既不报错也不退出。与 QueryFastapiLifespan 保持同一口径。
_DB_CONNECT_TIMEOUT_SECONDS = 10


@contextlib.asynccontextmanager
async def life_span(app: FastAPI):
    # ---- 云端 LLM 配置来源：runtime/llm_apis.json 优先，缺失 / 损坏回退 .env ----
    # 只覆盖 settings.llm_apis（不建实例池、不联网）；文件不存在时保持 .env 原样。
    # 来源会打一行日志（LLM 配置来源: file/env），便于运维确认当前生效的是哪份。
    config_store.apply_to_settings()

    if settings.is_api_only:
        # 「只提供接口、不爬取」角色（SERVICE_ROLE=api，本地开发用）：
        # 到这里就只探库，其余全部跳过 —— 不迁移 / 不校验 schema / 不初始化 Milvus /
        # 不重投 MQ / 不启动调度器与监控任务 / 不做 critical 依赖探活。
        # 这样只起了基础设施的机器也能把接口跑起来（缺库的接口在请求时才报错），
        # 进程启动后不会自动抓任何数据（不依赖 IS_DEV，也不需要 IS_DEV）。
        await probe_database_connections()
        myfastapi_logger.info(
            "接口服务启动完成（api 模式：只提供接口，无迁移 / 无调度 / 无 MQ 消费 / 不自动爬取）"
        )
        yield
        myfastapi_logger.info("接口服务正在退出")
        return

    # 测试数据库连接
    try:
        await test_database_connections()
    except OperationalError as e:
        myfastapi_logger.critical(
            f"数据库（{CONFIG.database.MYSQL._base_url}）连接测试失败: {e}"
        )
        raise e
    # 测试各个服务的端口和 host 连通性（不包含 be-message-service：它不是本服务的依赖）
    await test_service_ports_and_hosts()

    # ---- 启动期自动执行 alembic upgrade head 增量迁移 ----
    if not await run_alembic_upgrade_head():
        raise RuntimeError("alembic upgrade head 执行失败，请检查数据库连接与迁移脚本")

    # ---- Schema 一致性校验：不一致则拒绝启动 ----
    if not await check_schemas():
        raise RuntimeError(
            "数据库 Schema 与模型不一致，请先手动执行 alembic upgrade head"
        )

    # 检查 milvus 数据库集合
    await asyncio.sleep(3)
    myfastapi_logger.info("检查 milvus 数据库集合")
    await milvus_sql_helper.ensure_collection_exists()
    myfastapi_logger.info("重试未处理的消息")
    await BiliLotDataPublisher.retry_pending_messages()
    # MQ 消费者由 mq_controller.py 的 @router.subscriber 注册；broker 连接由 FastAPI 通过
    # app.include_router(MqController.router) 自动管理。
    # RPC 只做只读取数（2.70.0）：RPC 服务端已交给 query 角色（query_main.py），
    # 本角色只消费 bili_data 的写库消息，因此这里不 import 任何 RPC handler 模块。
    myfastapi_logger.info("开启其他服务")
    back_ground_tasks = []
    if settings.IS_DEV:
        myfastapi_logger.info("开发环境不启动定时任务喵~")
    else:
        show_log = False
        GLOBAL_SCHEDULER.start()
        back_ground_tasks = BackgroundServiceController.start_monitor_tasks(
            show_log=show_log
        )
        myfastapi_logger.info("其他服务已开启！可以开启服务了喵~")
    yield
    myfastapi_logger.info("正在取消其他服务")
    [x.cancel() for x in back_ground_tasks]
    await asyncio_gather(*back_ground_tasks, log=myfastapi_logger)
    myfastapi_logger.info("其他服务已取消")


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
            await engine.dispose()


async def probe_database_connections():
    """api 角色专用：逐个探库并打日志，**连不上也不拒绝启动**。

    与 ``test_database_connections``（失败即 SystemExit）的区别：本地开发经常只同步了
    部分库，api 角色的职责只是「把接口挂出去」，缺库让对应接口在请求时自己报错即可，
    不该因为无关的库导致整个进程起不来。
    """
    databases = {
        "proxy_db": CONFIG.database.MYSQL.proxy_db_URI,
        "bilidb": CONFIG.database.MYSQL.bili_db_URI,
        "bili_reserve": CONFIG.database.MYSQL.bili_reserve_URI,
        "get_other_lot": CONFIG.database.MYSQL.get_other_lot_URI,
        "dyndetail": CONFIG.database.MYSQL.dyn_detail_URI,
        "sams_club": CONFIG.database.MYSQL.sams_club_URI,
    }

    unreachable: list[str] = []
    for db_name, db_uri in databases.items():
        engine = create_async_engine(db_uri)
        try:
            # 探测同样要限时，否则黑洞路径会让 api 角色也卡在启动阶段
            async with asyncio.timeout(_DB_CONNECT_TIMEOUT_SECONDS):
                async with engine.connect() as conn:
                    await conn.execute(text("SELECT 1"))
            myfastapi_logger.info(f"数据库 '{db_name}' 连接成功")
        except Exception as exc:  # noqa: BLE001 —— 探测失败只记录，不阻断接口启动
            unreachable.append(f"{db_name} ({exc.__class__.__name__}: {exc})")
        finally:
            await engine.dispose()

    if unreachable:
        myfastapi_logger.warning(
            "以下数据库连接失败，相关接口请求时会报错（api 模式不迁移 / 不校验 schema，不阻断启动）: "
            + "; ".join(unreachable)
        )
    else:
        myfastapi_logger.info("全部数据库连接正常")


async def test_port_connectivity(
    host: str, port: int, service_name: str, timeout: float = 5.0
) -> bool:
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
                f"HTTP 服务 '{service_name}' ({url}) 连接成功，状态码：{response.status_code}"
            )
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


_IPV6_RE = re.compile(r"^[0-9a-fA-F:]+$")


def _looks_like_ipv6(addr: str) -> bool:
    """粗粒度校验字符串是否像一个 IPv6 地址（含冒号且整体为十六进制/冒号字符）。"""
    addr = addr.strip()
    if ":" not in addr:
        return False
    return bool(_IPV6_RE.match(addr)) and addr.count("::") <= 1


async def test_proxy_connectivity(
    proxy_url: str,
    service_name: str = "IPv6代理(3128)",
    test_url: str = "https://ipv6.icanhazip.com",
    timeout: float = 10.0,
) -> bool:
    """通过 HTTP 代理实际发起请求，验证 3128 IPv6 代理是否真的可用。

    与普通的 TCP/HTTP 端口探测不同，这里会把请求真正经代理转发到外网，
    并校验返回内容是否为 IPv6 地址，从而确认代理出口正常。
    仅做功能性展示，不阻断启动（代理偶发不可用不应阻止服务启动）。
    """
    try:
        async with httpx.AsyncClient(proxy=proxy_url, timeout=timeout) as client:
            response = await client.get(test_url)
        body = response.text.strip()
        if response.status_code == 200 and _looks_like_ipv6(body):
            myfastapi_logger.info(
                f"✅ 代理 '{service_name}' ({proxy_url}) 正常，出口 IPv6: {body}"
            )
            return True
        myfastapi_logger.error(
            f"❌ 代理 '{service_name}' ({proxy_url}) 返回异常: "
            f"status={response.status_code}, body={body[:80]!r}"
        )
        return False
    except httpx.ProxyError as e:
        myfastapi_logger.error(f"❌ 代理 '{service_name}' ({proxy_url}) 连接失败：{e}")
        return False
    except httpx.TimeoutException:
        myfastapi_logger.error(f"❌ 代理 '{service_name}' ({proxy_url}) 请求超时")
        return False
    except Exception as e:
        myfastapi_logger.error(f"❌ 代理 '{service_name}' ({proxy_url}) 检测异常：{e}")
        return False


async def test_service_ports_and_hosts():
    """测试 CONFIG 中设置的所有服务的端口和 host 连通性。

    kind="port" -> TCP 端口探测（test_port_connectivity）；
    kind="http" -> HTTP 端点探测（test_http_endpoint，path 为可选路径）。

    critical=True 的服务若连接失败，将直接终止启动（SystemExit），
    其它非关键服务仅记录警告，不阻断启动。
    """
    myfastapi_logger.info("开始测试各服务端口和 host 连通性...")

    failed_services = []
    failed_critical_services = []

    # (name, host, port, kind, path, critical)
    #
    # 注意：**be-message-service 不在探活列表里**（2.70.1）。crawler 与它只有「消息队列」的弱耦合：
    # 推送 / 告警通过 RabbitMQ（message_exchange / message.push）fire-and-forget 发布，
    # message-service 不在线时消息留在队列里等它上线后消费，crawler 的启动与业务不受影响。
    # 之前把它列为 critical 会导致「本机没跑 message-service 就完全起不来」，dev 下反复重启。
    services_to_test = [
        ("MySQL", settings.MYSQL_HOST, int(settings.MYSQL_PORT), "port", "", True),
        ("Redis", settings.REDIS_HOST, int(settings.REDIS_PORT), "port", "", True),
        (
            "RabbitMQ",
            settings.RABBITMQ_HOST,
            int(settings.RABBITMQ_PORT),
            "port",
            "",
            True,
        ),
        ("Milvus", settings.MILVUS_HOST, int(settings.MILVUS_PORT), "port", "", True),
        ("Unidbg", settings.UNIDBG_HOST, int(settings.UNIDBG_PORT), "http", "", False),
        ("V2Ray", settings.V2RAY_HOST, int(settings.V2RAY_PORT), "http", "", False),
        ("llama.cpp", settings.LLAMA_HOST, int(settings.LLAMA_PORT), "http", "", True),
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

    # 额外：实际通过 3128 IPv6 代理发起请求，验证代理是否真的可用（非关键，仅展示）
    proxy_url = settings.PROXY_SERVER
    if proxy_url:
        proxy_ok = await test_proxy_connectivity(
            proxy_url, service_name="IPv6代理(3128)"
        )
        if not proxy_ok:
            failed_services.append(f"IPv6代理(3128) ({proxy_url})")
    else:
        myfastapi_logger.warning("未配置 PROXY_SERVER，跳过 3128 IPv6 代理检测")

    if failed_services:
        # 这里可能包含「非关键服务」（V2Ray / 3128 代理等）探测失败，不构成致命故障，
        # 只有下面的 failed_critical_services 才是 critical。
        myfastapi_logger.error("=" * 60)
        myfastapi_logger.error("以下服务连接失败:")
        for failed_service in failed_services:
            myfastapi_logger.error(f"  ❌ {failed_service}")
        myfastapi_logger.error("=" * 60)
    else:
        myfastapi_logger.info("✅ 所有服务端口和 host 连通性测试通过!")

    # 关键服务未启动 -> 直接报错并拒绝启动
    if failed_critical_services:
        myfastapi_logger.critical("=" * 60)
        myfastapi_logger.critical(
            f"以下关键服务未启动，禁止服务启动: {len(failed_critical_services)} 个"
        )
        for failed_service in failed_critical_services:
            myfastapi_logger.critical(f"  🚫 {failed_service}")
        myfastapi_logger.critical("=" * 60)
        raise SystemExit(
            f"关键依赖服务未启动，拒绝启动: {', '.join(failed_critical_services)}"
        )


__all__ = ["life_span"]

if __name__ == "__main__":
    asyncio.run(test_database_connections())
