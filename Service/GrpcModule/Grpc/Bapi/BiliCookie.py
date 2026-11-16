import asyncio
import random
import time
from typing import AsyncGenerator
from urllib.parse import quote

from CONFIG import CONFIG
from log.base_log import bapi_log
from Models.AntiRisk.Bili.WebCookie import BiliWebCookie, CookieWrapper
from Models.AntiRisk.Bili.CookieRedisStore import CookieRedisStore
from Service.GrpcModule.Grpc.Bapi.BiliApiBase import (
    get_frontend_finger_spi,
    gen_web_ticket,
    get_bili_main_page_raw_resp,
    gaia_gateway_ExClimbWuzhi,
    gaia_gateway_ExClimbCongling,
)
from Service.GrpcModule.Grpc.Bapi.Constants import (
    BASE_COOKIE_KEYS,
    EXCLIMB_WUZHI_COOKIE_KEYS,
)
from Utils.加密.utils import GenWebCookieParams
from Utils.加密.utils import lsid, uuidInfoc
from Utils.代理.session_profile import profile_for_session_key


class BiliCookieGenerationError(RuntimeError):
    """cookie 生成链路彻底失败（含「生成器被关闭后重建仍失败」）。

    用显式异常替代 ``StopAsyncIteration``：后者只说明「生成器没下一个元素了」，
    不携带任何失败原因，上层只能看到一个无法定位的空迭代错误。
    """


# --- Cookie池状态变量 ---
_cookie_lock = asyncio.Lock()
_fake_cookie_list: list[CookieWrapper] = []
_cookie_queue_num = 0
#: 当前使用的 cookie 生成器。默认惰性创建；被取消 / 关闭后可随时重建（见 ``_next_bili_cookie``）。
_cookie_iter: AsyncGenerator[BiliWebCookie, None] | None = None
#: Redis 持久化（跨进程 / 重启复用）；Redis 异常时自动降级为进程内存池。
_cookie_store: CookieRedisStore | None = None


def get_cookie_store() -> CookieRedisStore | None:
    """返回 Redis 池；关闭持久化时返回 None（回退到原内存池）。"""
    global _cookie_store
    if not CONFIG.bili_cookie.persist_to_redis:
        return None
    if _cookie_store is None:
        _cookie_store = CookieRedisStore()
    return _cookie_store


async def persist_cookie_risk(cookie_data: CookieWrapper | None) -> None:
    """把 -352 计数写回 Redis（跨进程继续生效）。

    只在命中风控时调用，不进入正常请求热路径；Redis 失败只降级，不抛异常。
    """
    store = get_cookie_store()
    if store is not None and cookie_data is not None:
        await store.save(cookie_data)


async def _gen_buvid4(cookie: BiliWebCookie) -> None:
    cookie_str = cookie.to_str(include_keys=BASE_COOKIE_KEYS)
    spi_resp_dict = await get_frontend_finger_spi(
        cookie_str=cookie_str, bili_web_cookie=cookie
    )
    cookie.buvid4 = quote(spi_resp_dict.data.b_4, safe="")


async def _gen_web_ticket(cookie: BiliWebCookie) -> None:
    cookie_str = cookie.to_str(include_keys=BASE_COOKIE_KEYS)
    spi_resp_dict = await gen_web_ticket(cookie_str=cookie_str, bili_web_cookie=cookie)
    cookie.bili_ticket = spi_resp_dict["data"]["ticket"]
    cookie.bili_ticket_expires = (
        spi_resp_dict["data"]["created_at"] + spi_resp_dict["data"]["ttl"]
    )


def _gen_lsid_hit_dyn_v2(cookie: BiliWebCookie) -> None:
    cookie.b_lsid = lsid()
    cookie.hit_dyn_v2 = "1"


async def _get_buvid3(cookie: BiliWebCookie) -> None:
    result = await get_bili_main_page_raw_resp(bili_web_cookie=cookie)
    if ua := result.request.headers.get("user-agent"):
        cookie.gen_web_cookie_params.ua = ua
    for k, v in result.cookies.items():
        setattr(cookie, k, v)


async def _active_exclimb_wuzhi(cookie: BiliWebCookie):
    await gaia_gateway_ExClimbWuzhi(
        cookie_str=cookie.to_str(include_keys=EXCLIMB_WUZHI_COOKIE_KEYS),
        payload=cookie.gen_web_cookie_params.payload_str,
        bili_web_cookie=cookie,
    )


async def _active_exclimb_congling():
    await gaia_gateway_ExClimbCongling()


def _log_step_failure(step: str, exc: BaseException) -> None:
    """把「哪一步失败」写清楚：StopAsyncIteration 之类的空栈错误没有排查价值。"""
    bapi_log.warning(
        f"生成 bilibili cookie 失败：步骤【{step}】异常 "
        f"{type(exc).__name__}: {exc}（后续重试 / 重建生成器仍从该步骤重新开始）"
    )


async def _gen_buvid3_step(cookie: BiliWebCookie) -> None:
    """buvid3 / _uuid 等基础 cookie：失败说明整个 cookie 不可用，必须上抛。"""
    try:
        await _get_buvid3(cookie)
    except Exception as e:
        _log_step_failure("buvid3(主站首页)", e)
        raise BiliCookieGenerationError(
            f"生成 bilibili cookie 失败：buvid3 阶段 {type(e).__name__}: {e}"
        ) from e


async def _gen_extra_steps(cookie: BiliWebCookie) -> None:
    """buvid4 / bili_ticket：两者相互独立，任一失败只降级（不阻断 cookie 落地）。"""
    results = await asyncio.gather(
        _gen_buvid4(cookie), _gen_web_ticket(cookie), return_exceptions=True
    )
    for step, result in zip(("buvid4", "bili_ticket"), results):
        if isinstance(result, BaseException):
            _log_step_failure(step, result)


async def _active_gaia_step(cookie: BiliWebCookie) -> None:
    """gaia（ExClimbWuzhi / ExClimbCongling）激活：失败只降级，cookie 仍然可用。"""
    try:
        await _active_exclimb_wuzhi(cookie)
        await _active_exclimb_congling()
    except Exception as e:
        _log_step_failure("gaia(ExClimbWuzhi/ExClimbCongling)", e)


async def _bili_web_cookie_gen_once() -> BiliWebCookie:
    """生成单个 BiliWebCookie 实例。

    UA 不再来自 ``CONFIG.rand_ua``：先确定本会话的 ``_uuid``，再用稳定哈希选出
    Chrome 指纹 + 同主版本 UA（见 ``session_profile``）。这样后续所有请求的
    TLS 指纹、UA、Client Hints 都与 cookie 会话同源，避免「JA3 说 Chrome、
    UA 说 Firefox」的交叉校验错位。
    """
    session_key = uuidInfoc.gen()
    profile = profile_for_session_key(session_key)
    cookie = BiliWebCookie(
        gen_web_cookie_params=GenWebCookieParams(
            uuid=session_key,
            ua=profile.ua,
            window_h=random.randint(1080, 1440),
            window_w=random.randint(1920, 2560),
            avail_w=random.randint(1920, 2560),
            avail_h=random.randint(1080, 1440),
        )
    )
    await _gen_buvid3_step(cookie)
    _gen_lsid_hit_dyn_v2(cookie)
    await _gen_extra_steps(cookie)
    await _active_gaia_step(cookie)
    return cookie


async def _bili_web_cookie_generator() -> AsyncGenerator[BiliWebCookie, None]:
    """一个异步生成器，持续生成新的 Cookie"""
    while True:
        yield await _bili_web_cookie_gen_once()


def _get_cookie_iter() -> AsyncGenerator[BiliWebCookie, None]:
    """**工厂**：每次调用都返回一个全新的 cookie 生成器。

    以前这里是模块级单例（``async_bili_cookie_iter = _bili_web_cookie_generator()``）：
    生成器一旦被取消（定时任务杀旧开新 / 进程内取消）或内部异常关闭，
    之后每次 ``anext`` 都只会抛 ``StopAsyncIteration``，且原始异常已被吞掉，
    **不重启进程就永远拿不到 cookie**（线上 03:21 起 8 次 get_lot_notice 全失败即此）。
    """
    return _bili_web_cookie_generator()


async def _next_bili_cookie() -> BiliWebCookie:
    """取下一个 cookie；生成器已关闭 / 耗尽时重建一次，仍失败抛明确异常。"""
    global _cookie_iter
    if _cookie_iter is None:
        _cookie_iter = _get_cookie_iter()
    try:
        return await anext(_cookie_iter)
    except StopAsyncIteration:
        bapi_log.warning(
            "cookie 生成器已关闭 / 耗尽（可能被取消或内部异常退出），重建后重新获取"
        )
    except Exception as e:
        # 生成过程中抛出的真实异常：生成器此刻已被标记为关闭，重建后重试一次。
        _log_step_failure("cookie 生成（生成器执行中）", e)
    _cookie_iter = _get_cookie_iter()
    try:
        return await anext(_cookie_iter)
    except StopAsyncIteration as e:
        raise BiliCookieGenerationError(
            "cookie 生成器重建后仍然无法产出 cookie（生成链路已退出，详见上一条 warning）"
        ) from e


def _prune_cookie_pool() -> list[CookieWrapper]:
    """剔除过期 / 被风控封死的 cookie，返回当前可用列表（调用方需持有锁）。"""
    global _cookie_queue_num
    now = time.time()
    alive: list[CookieWrapper] = []
    for cw in _fake_cookie_list:
        if cw.expire_ts > now and cw.able:
            alive.append(cw)
    if len(alive) != len(_fake_cookie_list):
        _fake_cookie_list[:] = alive
        _cookie_queue_num = len(alive)
    return alive


def _remember_cookie(cookie_data: CookieWrapper) -> None:
    """把 cookie 记进进程内存池（去重 + 按容量挤掉最旧）。调用方需持有锁。"""
    uuid = cookie_data.ck.gen_web_cookie_params.uuid
    if not any(
        cw.ck.gen_web_cookie_params.uuid == uuid for cw in _fake_cookie_list
    ):
        _fake_cookie_list.append(cookie_data)
    while len(_fake_cookie_list) > max(1, CONFIG.bili_cookie.pool_size):
        _fake_cookie_list.pop(0)


async def get_bili_cookie() -> CookieWrapper:
    """从池中获取一个可用的 Bilibili Cookie。

    优先 Redis（跨进程复用）→ 进程内存池 → 现场生成并回写。
    Redis 任何异常都只降级为未命中，不阻断业务。
    """
    global _cookie_queue_num
    cfg = CONFIG.bili_cookie

    async with _cookie_lock:
        store = get_cookie_store()
        if store is not None:
            remote = await store.random_usable()
            if remote is not None:
                bapi_log.debug(
                    f"cookie 池命中 Redis：{remote.ck.gen_web_cookie_params.uuid}"
                )
                _remember_cookie(remote)
                return remote

        usable = _prune_cookie_pool()
        if usable:
            return random.choice(usable)

        # 池里没有可用 cookie（首次 / 全被剔除）：生成一个新的补进去
        _cookie_queue_num += 1
        bapi_log.debug(
            f"当前cookie池数量：{len(_fake_cookie_list)}，总共{_cookie_queue_num}个cookie，前往获取新的cookie"
        )
        new_ck = await _next_bili_cookie()
        cookie_data = CookieWrapper(
            ck=new_ck,
            ua=new_ck.gen_web_cookie_params.ua,
            expire_ts=int(time.time() + cfg.ttl_seconds),
            max_352_prefixes=cfg.max_352_prefixes,
        )
        _remember_cookie(cookie_data)
        if store is not None:
            await store.save(cookie_data)
        return cookie_data


def cookie_pool_size() -> int:
    """当前池内可用 cookie 数量（预热 / 巡检用）。"""
    return len(_prune_cookie_pool())


async def warmup_bili_cookie_pool(count: int | None = None) -> int:
    """启动期预热：串行生成 N 个 cookie，避免首个业务请求撞上「冷启动长链路」。

    失败只记 warning 并立即返回（生成链路的依赖不可用时会由首个请求继续重试），
    绝不能因为预热失败影响服务启动。
    """
    cfg = CONFIG.bili_cookie
    target = cfg.warmup_count if count is None else count
    target = min(max(0, target), max(1, cfg.pool_size))
    generated = 0
    for index in range(target):
        try:
            await get_bili_cookie()
            generated += 1
        except Exception as e:  # noqa: BLE001 —— 预热失败不影响启动
            bapi_log.warning(
                f"cookie 池预热中断（已生成 {generated}/{target}，第 {index + 1} 个失败）："
                f"{type(e).__name__}: {e}"
            )
            break
    bapi_log.info(
        f"cookie 池预热完成：生成 {generated}/{target} 个，池内现有 {cookie_pool_size()} 个"
    )
    return generated


if __name__ == "__main__":
    asyncio.run(get_bili_cookie())
