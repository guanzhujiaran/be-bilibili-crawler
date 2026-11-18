import asyncio
import contextlib
import gc
import re
import socket
import time

from fastapi import FastAPI
import httpx
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import create_async_engine
from CONFIG import settings, CONFIG
from Service.GrpcModule.Grpc.Bapi.BiliCookie import warmup_bili_cookie_pool
from Service.LangChainCompo.lottery_data_vec_sql.sql_helper import milvus_sql_helper
from Service.llm_service import config_store
from Service.MQ.base.MQClient.BiliLotDataPublisher import BiliLotDataPublisher
from Utils.FastAPI.alembic_manager import check_schemas, run_alembic_upgrade_head
from Utils.FastAPI.loop_monitor import start_event_loop_stall_monitor
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


class _StageMarker:
    """启动 / 关闭阶段里程碑打点（**WARNING 级**）。

    为什么必须 WARNING：生产默认 ``LOG_LEVEL=WARNING``，lifespan 里原来的
    ``myfastapi_logger.info`` 全都不落盘，于是「卡在哪一步」只能靠时间戳猜。
    每条带两段耗时：``elapsed``（距上一个里程碑）与 ``since_start``（距 lifespan 进入），
    一旦某一步吃了几十秒，日志里立刻能看出来是哪一步。
    """

    def __init__(self) -> None:
        self._enabled = CONFIG.loop_monitor.startup_milestones
        self._t0 = time.monotonic()
        self._last = self._t0

    def mark(self, stage: str, extra: str = "") -> None:
        if not self._enabled:
            return
        now = time.monotonic()
        myfastapi_logger.warning(
            f"event=lifespan_milestone stage={stage} "
            f"elapsed={now - self._last:.2f}s since_start={now - self._t0:.2f}s"
            f"{f' {extra}' if extra else ''}"
        )
        self._last = now


def _apply_gc_tuning() -> None:
    """启动完成后的 GC 调优（``CONFIG.loop_monitor``），并落一行改前/改后对照。

    为什么放在 ``startup_done`` 之后：模块 / 类 / 函数 / 常量表在启动期已创建完毕，
    它们几乎不会变成垃圾，却每次都参与 gen2 全量扫描。``gc.freeze()`` 把这些对象
    移到「永久代」后，后续 gen2 只扫运行期动态对象，单次回收耗时显著下降。
    代价：启动期已经形成的循环垃圾会被永久保留（一次性、可接受）。

    gen2 阈值默认从解释器的 10 调到 50（由 ``LOOP_MONITOR__GC_GEN2_THRESHOLD`` 覆盖），
    把全量回收频率降到 1/5；gen0 / gen1 不传时保持解释器默认。
    """
    cfg = CONFIG.loop_monitor
    before_threshold = gc.get_threshold()
    before_count = gc.get_count()
    before_stats = [dict(stat) for stat in gc.get_stats()]
    before_frozen = gc.get_freeze_count()
    desired = (
        cfg.gc_gen0_threshold if cfg.gc_gen0_threshold > 0 else before_threshold[0],
        cfg.gc_gen1_threshold if cfg.gc_gen1_threshold > 0 else before_threshold[1],
        cfg.gc_gen2_threshold if cfg.gc_gen2_threshold > 0 else before_threshold[2],
    )
    if desired != before_threshold:
        gc.set_threshold(*desired)
    if cfg.gc_freeze_on_startup:
        gc.freeze()
    after_frozen = gc.get_freeze_count()
    gen2_collections = (
        int(before_stats[2].get("collections", 0)) if len(before_stats) > 2 else -1
    )
    myfastapi_logger.warning(
        f"event=gc_startup_tuning freeze={'on' if cfg.gc_freeze_on_startup else 'off'} "
        f"frozen_objects={after_frozen - before_frozen} "
        f"total_frozen={after_frozen} "
        f"threshold={before_threshold}->{gc.get_threshold()} "
        f"count={before_count} gen2_collections_before={gen2_collections} "
        f"（freeze 把启动期模块/类/函数移出 GC 扫描集合；gen2 阈值调大可降低全量回收频率）"
    )


@contextlib.asynccontextmanager
async def life_span(app: FastAPI):
    stage = _StageMarker()
    # ---- 云端 LLM 配置来源：runtime/llm_apis.json 优先，缺失 / 损坏回退 .env ----
    # 只覆盖 settings.llm_apis（不建实例池、不联网）；文件不存在时保持 .env 原样。
    # 来源会打一行日志（LLM 配置来源: file/env），便于运维确认当前生效的是哪份。
    # 文件 IO（含脱敏 token 自愈写回）放线程池，启动阶段不阻塞事件循环
    await config_store.apply_to_settings_async()
    stage.mark("llm_config_applied")

    if settings.is_api_only:
        # 「只提供接口、不爬取」角色（SERVICE_ROLE=api，本地开发用）：
        # 到这里就只探库，其余全部跳过 —— 不迁移 / 不校验 schema / 不初始化 Milvus /
        # 不重投 MQ / 不启动调度器与监控任务 / 不做 critical 依赖探活。
        # 这样只起了基础设施的机器也能把接口跑起来（缺库的接口在请求时才报错），
        # 进程启动后不会自动抓任何数据（不依赖 IS_DEV，也不需要 IS_DEV）。
        await probe_database_connections()
        stage.mark("api_mode_db_probe")
        myfastapi_logger.info(
            "接口服务启动完成（api 模式：只提供接口，无迁移 / 无调度 / 无 MQ 消费 / 不自动爬取）"
        )
        yield
        myfastapi_logger.info("接口服务正在退出")
        stage.mark("api_mode_exit")
        return

    # 测试数据库连接
    try:
        await test_database_connections()
    except OperationalError as e:
        myfastapi_logger.critical(
            f"数据库（{CONFIG.database.MYSQL._base_url}）连接测试失败: {e}"
        )
        raise e
    stage.mark("test_database_connections")
    # 测试各个服务的端口和 host 连通性（不包含 be-message-service：它不是本服务的依赖）
    await test_service_ports_and_hosts()
    stage.mark("test_service_ports_and_hosts")

    # ---- 启动期自动执行 alembic upgrade head 增量迁移 ----
    if not await run_alembic_upgrade_head():
        raise RuntimeError("alembic upgrade head 执行失败，请检查数据库连接与迁移脚本")
    stage.mark("run_alembic_upgrade_head")

    # ---- Schema 一致性校验：不一致则拒绝启动 ----
    if not await check_schemas():
        raise RuntimeError(
            "数据库 Schema 与模型不一致，请先手动执行 alembic upgrade head"
        )
    stage.mark("check_schemas")

    # 检查 milvus 数据库集合
    await asyncio.sleep(3)
    myfastapi_logger.info("检查 milvus 数据库集合")
    await milvus_sql_helper.ensure_collection_exists()
    stage.mark("milvus_ensure_collection_exists")
    myfastapi_logger.info("重试未处理的消息")
    await BiliLotDataPublisher.retry_pending_messages()
    stage.mark("retry_pending_messages")
    # MQ 消费者由 mq_controller.py 的 @router.subscriber 注册；broker 连接由 FastAPI 通过
    # app.include_router(MqController.router) 自动管理。
    # RPC 只做只读取数（2.70.0）：RPC 服务端已交给 query 角色（query_main.py），
    # 本角色只消费 bili_data 的写库消息，因此这里不 import 任何 RPC handler 模块。
    myfastapi_logger.info("开启其他服务")
    back_ground_tasks = []
    # 事件循环卡顿监控：任何 >1s 的阻塞都记 warning（并按 10min 节流推送），
    # 且由独立守护线程抓「占住事件循环的那个同步调用栈」，
    # 让「启动后 HTTP 无响应但端口已监听」这类问题有直接证据，而不是靠时间戳猜。
    if CONFIG.loop_monitor.enabled:
        back_ground_tasks.append(start_event_loop_stall_monitor())
        stage.mark("loop_monitor_started")
    else:
        myfastapi_logger.warning(
            "事件循环卡顿监控已关闭（LOOP_MONITOR__ENABLED=false）"
        )
        stage.mark("loop_monitor_disabled")
    if settings.IS_DEV:
        myfastapi_logger.info("开发环境不启动定时任务喵~")
        stage.mark("dev_skip_scheduler")
    else:
        show_log = False
        GLOBAL_SCHEDULER.start()
        stage.mark("global_scheduler_started")
        # extend 而不是赋值：赋值会把上面已经加进去的「事件循环卡顿监控」丢掉，
        # 导致它在 shutdown 时不会被取消。
        back_ground_tasks.extend(
            BackgroundServiceController.start_monitor_tasks(show_log=show_log)
        )
        stage.mark("monitor_tasks_started")
        back_ground_tasks.append(asyncio.create_task(_warmup_bili_cookie_pool()))
        stage.mark("cookie_warmup_task_started")
        myfastapi_logger.info("其他服务已开启！可以开启服务了喵~")
    # 汇总一行：启动到此结束，yield 之后服务对外可用
    stage.mark("startup_done", extra=f"background_tasks={len(back_ground_tasks)}")
    # 启动完成、模块 / 类 / 函数等对象已经稳定：把它们移出 GC 扫描集合，
    # 并（按需）放缓 gen2 全量回收，降低「分配内存时触发 gen2 GC」造成的事件循环停顿。
    _apply_gc_tuning()
    yield
    myfastapi_logger.info("正在取消其他服务")
    # 这里 elapsed 就是「服务已运行多久」（上一个里程碑是 startup_done），不是卡顿
    stage.mark("shutdown_enter", extra="（elapsed=服务运行时长，非卡顿）")
    [x.cancel() for x in back_ground_tasks]
    await asyncio_gather(*back_ground_tasks, log=myfastapi_logger)
    stage.mark("background_tasks_cancelled")
    # 关闭 Web 会话池：AsyncSession 的 curl 句柄必须在事件循环结束前显式释放，
    # 否则进程退出时连接会以「半开」状态留在系统里（容器重启后上游侧仍见到旧连接）。
    from Utils.代理.SessionPool import web_session_pool

    await web_session_pool.aclose()
    stage.mark("web_session_pool_closed")
    myfastapi_logger.info("其他服务已取消")


#: 启动期 cookie 预热的延迟（秒）。刻意放到「服务已就绪」之后：
#: 生成 cookie 要跑主站首页 + spi + gaia 一整条链路，放在 lifespan 里 await 会把
#: 「Application startup complete」推迟，还可能和调度器的首批任务抢同一个事件循环。
_COOKIE_WARMUP_DELAY_SECONDS = 15


async def _warmup_bili_cookie_pool() -> None:
    """后台预热 cookie 池（可配开关 / 数量，见 ``CONFIG.bili_cookie``）。

    失败只记 warning：预热只是「提前把长链路跑掉」，不预热也必须能正常工作。
    """
    if not CONFIG.bili_cookie.warmup_on_startup:
        myfastapi_logger.info("cookie 池预热已关闭（BILI_COOKIE__WARMUP_ON_STARTUP=false）")
        return
    await asyncio.sleep(_COOKIE_WARMUP_DELAY_SECONDS)
    try:
        await warmup_bili_cookie_pool()
    except Exception as e:  # noqa: BLE001 —— 预热失败不影响服务
        myfastapi_logger.warning(f"cookie 池预热失败（首个业务请求会自行重试）：{e!r}")


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
    host: str, port: int, service_name: str, timeout: float = 5.0, *, critical: bool = True
) -> bool:
    """测试指定 host 和 port 的连通性。

    ``critical=False`` 表示「可选 / 按环境启用」的依赖（如 V2Ray、本地 IPv6 代理）：
    探测失败只记 WARNING，避免每次启动刷一堆 ERROR 掩盖真正的故障。
    """
    log_fail = myfastapi_logger.error if critical else myfastapi_logger.warning
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
        log_fail(f"服务 '{service_name}' ({host}:{port}) 连接超时")
        return False
    except ConnectionRefusedError:
        log_fail(f"服务 '{service_name}' ({host}:{port}) 连接被拒绝")
        return False
    except OSError as e:
        log_fail(f"服务 '{service_name}' ({host}:{port}) 连接失败：{e}")
        return False
    except Exception as e:
        log_fail(f"服务 '{service_name}' ({host}:{port}) 连接异常：{e}")
        return False


async def test_http_endpoint(
    url: str, service_name: str, timeout: float = 5.0, *, critical: bool = True
) -> bool:
    """测试 HTTP 端点的连通性（``critical=False`` 时失败只记 WARNING）"""
    log_fail = myfastapi_logger.error if critical else myfastapi_logger.warning
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=timeout)
            myfastapi_logger.info(
                f"HTTP 服务 '{service_name}' ({url}) 连接成功，状态码：{response.status_code}"
            )
            return True
    except httpx.TimeoutException:
        log_fail(f"HTTP 服务 '{service_name}' ({url}) 连接超时")
        return False
    except httpx.RequestError as e:
        log_fail(f"HTTP 服务 '{service_name}' ({url}) 连接失败：{e}")
        return False
    except Exception as e:
        log_fail(f"HTTP 服务 '{service_name}' ({url}) 连接异常：{e}")
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
    仅做功能性展示，不阻断启动（代理偶发不可用不应阻止服务启动），
    因此失败一律记 WARNING：dev / 无 IPv6 出口的机器本来就没有这个代理。
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
        myfastapi_logger.warning(
            f"⚠️ 可选依赖：代理 '{service_name}' ({proxy_url}) 返回异常: "
            f"status={response.status_code}, body={body[:80]!r}"
        )
        return False
    except httpx.ProxyError as e:
        myfastapi_logger.warning(
            f"⚠️ 可选依赖：代理 '{service_name}' ({proxy_url}) 连接失败：{e}"
        )
        return False
    except httpx.TimeoutException:
        myfastapi_logger.warning(
            f"⚠️ 可选依赖：代理 '{service_name}' ({proxy_url}) 请求超时"
        )
        return False
    except Exception as e:
        myfastapi_logger.warning(
            f"⚠️ 可选依赖：代理 '{service_name}' ({proxy_url}) 检测异常：{e}"
        )
        return False


async def test_service_ports_and_hosts():
    """测试 CONFIG 中设置的所有服务的端口和 host 连通性。

    kind="port" -> TCP 端口探测（test_port_connectivity）；
    kind="http" -> HTTP 端点探测（test_http_endpoint，path 为可选路径）。

    critical=True 的服务若连接失败，将直接终止启动（SystemExit）；
    critical=False（可选 / 按环境启用，例如 V2Ray、本地 IPv6 代理）失败只记 WARNING，
    既不影响启动，也不会在每次启动时刷一屏 ERROR 掩盖真正的故障。
    """
    myfastapi_logger.info("开始测试各服务端口和 host 连通性...")

    failed_optional_services = []
    failed_critical_services = []

    # (name, host, port, kind, path, critical)
    #
    # 注意：**be-message-service 不在探活列表里**（2.70.1）。crawler 与它只有「消息队列」的弱耦合：
    # 推送 / 告警通过 RabbitMQ（message_exchange / message.push）fire-and-forget 发布，
    # message-service 不在线时消息留在队列里等它上线后消费，crawler 的启动与业务不受影响。
    # 之前把它列为 critical 会导致「本机没跑 message-service 就完全起不来」，dev 下反复重启。
    #
    # critical=False 的都是「可选 / 按环境启用」：V2Ray 只在需要代理访问外网时启动，
    # Unidbg 只给登录相关流程用，dev / 部分生产机器上本来就没有。
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
            ok = await test_port_connectivity(host, port, name, critical=critical)
            address = f"{host}:{port}"
        else:
            url = f"http://{host}:{port}{path}"
            ok = await test_http_endpoint(url, name, critical=critical)
            address = url
        if not ok:
            target = failed_critical_services if critical else failed_optional_services
            target.append(f"{name} ({address})")

    # 额外：实际通过 3128 IPv6 代理发起请求，验证代理是否真的可用（非关键，仅展示）
    proxy_url = settings.PROXY_SERVER
    if proxy_url:
        proxy_ok = await test_proxy_connectivity(
            proxy_url, service_name="IPv6代理(3128)"
        )
        if not proxy_ok:
            failed_optional_services.append(f"IPv6代理(3128) ({proxy_url})")
    else:
        myfastapi_logger.warning("未配置 PROXY_SERVER，跳过 3128 IPv6 代理检测")

    if failed_optional_services:
        # 可选依赖：只记 WARNING 并单独成段，不与 critical 故障混在一起
        myfastapi_logger.warning("=" * 60)
        myfastapi_logger.warning(
            "以下可选依赖未就绪（按环境启用，不影响启动与业务）:"
        )
        for failed_service in failed_optional_services:
            myfastapi_logger.warning(f"  ⚠️ {failed_service}")
        myfastapi_logger.warning("=" * 60)

    if not failed_optional_services and not failed_critical_services:
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
