import asyncio
import concurrent.futures
import re
from functools import wraps
from typing import Callable, TypeVar, Awaitable, Any, Optional, TYPE_CHECKING
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy.exc import InternalError, TimeoutError, OperationalError
from log.base_log import myfastapi_logger, sql_log
import random

if TYPE_CHECKING:
    from loguru import Logger

GLOBAL_SCHEDULER: AsyncIOScheduler = AsyncIOScheduler()
_comm_lock = asyncio.Lock()
# 全局线程池，避免重复创建
_global_executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)

TResult = TypeVar("TResult")
FuncT = TypeVar("FuncT", bound=Callable[..., Awaitable[Any]])


def sem_gen(sem_limit=100) -> asyncio.Semaphore:
    return asyncio.Semaphore(sem_limit)


def ensure_asyncio_loop():
    try:
        loop = asyncio.get_running_loop()
        if not loop.is_running():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
    except RuntimeError:
        # 当前没有运行的事件循环，创建新的
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)


def comm_lock_wrapper(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        async with _comm_lock:
            res = await func(*args, **kwargs)
            return res

    return wrapper


def comm_wrapper(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        res = await func(*args, **kwargs)
        return res

    return wrapper


def lock_retry_wrapper(lock: asyncio.Lock):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            while 1:
                try:
                    async with lock:
                        res = await func(*args, **kwargs)
                        return res
                except Exception as e:
                    myfastapi_logger.exception(e)
                    await asyncio.sleep(10)

        return wrapper

    return decorator


def retry_wrapper(func, max_retries: int = -1, sleep_time: int = 10):
    """
    重试装饰器，支持最大重试次数配置

    Args:
        func: 要装饰的函数
        max_retries: 最大重试次数，-1表示无限重试
        sleep_time: 重试间隔时间(秒)
    """

    @wraps(func)
    async def wrapper(*args, **kwargs):
        retry_count = 0
        while True:
            try:
                res = await func(*args, **kwargs)
                return res
            except Exception as e:
                if max_retries >= 0 and retry_count >= max_retries:
                    myfastapi_logger.error(
                        f"函数【{func.__name__}】重试{max_retries}次后仍失败，抛出异常"
                    )
                    raise

                retry_count += 1
                myfastapi_logger.exception(
                    f"函数【{func.__name__}】执行失败，第{retry_count}次重试: {e}"
                )
                await asyncio.sleep(sleep_time)

    return wrapper


async def run_in_executor(func, *args):
    loop = asyncio.get_event_loop()
    future = loop.run_in_executor(_global_executor, func, *args)
    return await future


def _log_db_connection_info(log, err: OperationalError) -> None:
    """打印当前 MySQL 连接配置与目标数据库，便于排查连接/库不存在错误。

    提取 host/port/user、报错中的目标库名，以及 CONFIG 中已配置的全部数据库 URI
    （密码脱敏），帮助快速定位「连错主机 / 库不存在」等问题。
    """
    try:
        from CONFIG import settings, CONFIG as _APP_CONFIG
    except Exception as _imp_err:  # 兜底：导入失败不影响主流程
        log.error(f"[MySQL诊断] 无法读取 CONFIG 用于诊断: {_imp_err}")
        return

    base_url = f"{settings.MYSQL_HOST}:{settings.MYSQL_PORT}"
    user = settings.MYSQL_USER

    # 从报错中解析目标数据库名（如 Unknown database 'biliopusdb'）
    m = re.search(r"Unknown database '([^']+)'", str(err.args[0]))
    target_db = m.group(1) if m else "(无法从报错解析)"

    # 收集已配置的数据库 URI（密码脱敏）
    configured: list[tuple[str, str]] = []
    try:
        mysql_cfg = _APP_CONFIG.database.MYSQL
        for name, attr in [
            ("biliopusdb(get_other_lot)", "get_other_lot_URI"),
            ("bilidb", "bili_db_URI"),
            ("bili_reserve", "bili_reserve_URI"),
            ("dyndetail", "dyn_detail_URI"),
            ("proxy_db", "proxy_db_URI"),
            ("samsclub", "sams_club_URI"),
        ]:
            uri = getattr(mysql_cfg, attr, "")
            if uri:
                configured.append((name, re.sub(r":([^:@/]+)@", ":****@", uri)))
    except Exception:
        configured = []

    lines = [
        "=" * 64,
        "[MySQL 连接诊断] 发生 OperationalError，请核对以下连接信息：",
        f"  HOST:PORT = {base_url}",
        f"  USER      = {user}",
        f"  报错中的目标数据库 = {target_db}",
        "  已配置的数据库(库名 -> URI)：",
    ]
    for name, uri in configured:
        lines.append(f"    - {name}: {uri}")
    lines += [
        "  排查建议：",
        "    1) 确认 HOST:PORT 指向的是否为预期的生产 MySQL；",
        "    2) 确认该 MySQL 实例上是否已创建报错中的目标数据库；",
        "    3) 检查 .env 的 MYSQL_HOST/PORT/USER/PASSWORD，",
        "       或使用 --db-host/--db-port/--db-user/--db-password 覆盖。",
        "=" * 64,
    ]
    log.error("\n".join(lines))


async def handle_sql_operational_error(func, log, err: OperationalError) -> bool:
    """
    返回是否需要继续重试
    """
    # err.args[0] 形如 "(pymysql.err.OperationalError) (2013, '...')" 的字符串，
    # 无法直接用 match 匹配整数错误码，先统一取出字符串备用。
    err_str = str(err.args[0]) if err.args else str(err)

    if "(pymysql.err.OperationalError) (1054," in err_str:  # 数据操作错了
        return False
    # 1040: 连接数过多，并发太高，等待 MySQL 释放连接后重试
    if "(pymysql.err.OperationalError) (1040," in err_str:
        log.error(
            f"{func} \t连接数过多(Too many connections)，并发太高，等待后重试: {err}"
        )
        await asyncio.sleep(random.uniform(5, 120))
        return True

    # 从原始异常中解析出整数错误码：优先用 err.orig（pymysql 原始异常），
    # 失败则从字符串中正则提取，避免 match 因类型不匹配而全部落入未知分支。
    code = None
    orig = getattr(err, "orig", None)
    if orig is not None and getattr(orig, "args", None):
        cand = orig.args[0]
        if isinstance(cand, int):
            code = cand
    if code is None:
        m = re.search(r"\((\d+),", err_str)
        if m:
            code = int(m.group(1))

    match code:
        case 1129:
            log.error(f"{func} \t{err}")
            await asyncio.sleep(120)
        case 1213:  # 死锁错误，随机等待后重试
            sleep_time = random.uniform(1, 5)  # 随机等待1-5秒
            log.error(f"{func} \t死锁错误: {err}, 将在{sleep_time:.2f}秒后重试")
            await asyncio.sleep(sleep_time)
        case 2013:  # mysql连接丢失(2013)，等待一段时间再重试
            log.error(f"{func} \tMySQL 连接丢失(2013)，等待后重试: {err}")
            await asyncio.sleep(random.uniform(5, 120))
        case 2003:  # mysql配置不正确或者mysql暂时挂了，在重启
            log.error(f"{func} \t{err}")
            _log_db_connection_info(log, err)
            await asyncio.sleep(120)
        case _:  # 未知代码（含 1049 Unknown database 等配置/库错误）
            log.error(f"未知mysql错误代码：{func} \t{err}")
            _log_db_connection_info(log, err)
            # 未知代码多为配置/库不存在等不可恢复错误，停止无限重试并抛出
            return False

    return True


def sql_retry_wrapper(_func: FuncT) -> FuncT:
    @wraps(_func)
    async def wrapper(*args: Any, **kwargs: Any):
        while True:
            try:
                res = await _func(*args, **kwargs)
                return res
            except InternalError as internal_error:
                sql_log.error(internal_error)
                await asyncio.sleep(60)
                continue
            except OperationalError as operational_error:
                if await handle_sql_operational_error(_func, log, operational_error):
                    continue
                break
            except TimeoutError as timeout_error:  # 这个是超时了，可能mysql负载太高卡了
                await asyncio.sleep(60)
                continue
            except Exception as e:
                sql_log.critical(
                    f"函数：【{_func.__name__}】\targs：【{args}\t{kwargs}】\t报错：【{e}】"
                )
                await asyncio.sleep(60)
                continue

    return wrapper


def log_sql_retry_wrapper(log: "Logger" = myfastapi_logger):
    def _wrapper(_func: FuncT) -> FuncT:
        @wraps(_func)
        async def wrapper(*args: Any, **kwargs: Any) -> TResult:
            while True:
                try:
                    res = await _func(*args, **kwargs)
                    return res
                except InternalError as internal_error:
                    log.error(internal_error)
                    await asyncio.sleep(60)
                    continue
                except OperationalError as operational_error:
                    if await handle_sql_operational_error(
                        _func, log, operational_error
                    ):
                        continue
                    log.exception(operational_error)
                    raise  # 无法恢复时抛出异常，避免返回 None 导致调用方出现误导性 TypeError
                except Exception as e:
                    # 检查是否是死锁错误(已经在内层处理过重试)
                    error_str = str(e)
                    if "Deadlock" in error_str or "1213" in error_str:
                        log.error(
                            f"{_func.__name__} \t死锁错误(内层重试已耗尽): {error_str[:200]}"
                        )
                        await asyncio.sleep(5)  # 短暂等待后继续重试
                        continue
                    # 其他异常保持原有逻辑
                    log.exception(f"{args}\n{kwargs}\n{e}")
                    await asyncio.sleep(60)
                    continue

        return wrapper

    return _wrapper


async def asyncio_gather(*coros_or_futures, log: Optional["Logger"] = myfastapi_logger):
    async def _handle_coroutine(coro):
        try:
            return await coro
        except Exception as e:
            log and log.exception(f"协程 [{coro.cr_code}] 执行失败.")

    coros_or_futures_wrapped = map(_handle_coroutine, coros_or_futures)
    results = await asyncio.gather(*coros_or_futures_wrapped, return_exceptions=True)
    return results


def log_max_count_retry_wrapper(
    *, log: "Logger" = myfastapi_logger, max_count: int = 3, sleep_time: int = 10
):
    """
    Decorator factory that creates a retry decorator with logging.

    Args:
        log: Logger instance to use (default: myfastapi_logger)
        max_count: Maximum number of retry attempts (default: 3)
                  If max_count <= 0, retry infinitely
    """

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            attempt = 0
            while True:  # Infinite loop for retrying
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if 0 < max_count <= attempt:
                        log.error(
                            f"All {max_count + 1} attempts failed for {func.__name__}. "
                            f"Last error: {str(e)}"
                        )
                        break

                    log.exception(
                        f"Attempt {attempt + 1} failed for {func.__name__}: {str(e)}. "
                        f"{'Retrying...' if max_count <= 0 else f'Retrying... ({max_count - attempt} attempts left)'}"
                    )
                    await asyncio.sleep(sleep_time)  # Fixed backoff interval
                    attempt += 1
            raise last_exception

        return wrapper

    return decorator
