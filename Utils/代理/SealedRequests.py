import asyncio
import random
import re
import typing
from typing import Union
from curl_cffi import Curl, Response, BrowserTypeLiteral
from curl_cffi.fingerprints import FingerprintManager
from curl_cffi.requests.utils import _is_native_impersonate_target
from curl_cffi.requests.impersonate import resolve_latest_browser_type
from httpx._types import (
    RequestContent,
    RequestFiles,
    QueryParamTypes,
    HeaderTypes,
    CookieTypes,
    RequestData,
)

from CONFIG import CONFIG

from Utils.代理.数据库操作.comm import get_scheme_ip_port_form_proxy_dict
from Utils.代理.SessionPool import TRUST_ENV, web_session_pool
from Utils.代理.session_profile import (
    ANONYMOUS_SESSION_KEY,
    WebProfile,
    profile_for_session_key,
    profile_for_target,
    random_profile,
)

# curl_cffi 内部实现变动导致探测失败时的兜底目标（别名，会被解析成最新版本）
_FALLBACK_IMPERSONATE_TARGETS = ("chrome", "edge", "safari", "firefox")

#: 请求库是否读取环境变量（``HTTP_PROXY`` / ``HTTPS_PROXY`` / ``ALL_PROXY`` 等）。
#: 显式关闭：容器 / 宿主机若设置了这些变量，curl_cffi 会把它当成默认代理，
#: 从而绕过我们显式传入的出口 —— cookie / metadata 必须固定走自建 IPv6 专线，
#: 一旦被环境代理截胡，日志里就会出现「目标出口是 host.docker.internal，
#: 实际请求却走了别的 IP」这类问题。常量定义在 ``SessionPool``，这里 re-export。
__all__ = ["TRUST_ENV", "my_async_httpx", "MYASYNCHTTPX", "resolve_web_profile"]


def _probe_impersonate_target(target: str, named_fingerprints: set[str]) -> bool:
    """实测单个 impersonate 目标是否真的可用。

    必须复刻 curl_cffi 在 set_curl_options 里的分支口径，只看任何一边都会误判：

    - 原生分支：库会先 resolve_latest_browser_type() 再调 Curl.impersonate()，
      返回非 0 表示底层 .so 确实不支持。注意不能用裸名去调 Curl.impersonate，
      例如 "chrome" 裸调返回 43，但库先解析成 chrome150 之后是成功的。
    - 扩展分支：名字不在原生名单里时，库改查指纹库（内置 + impersonate.pro 下发），
      查不到就直接抛 ImpersonateError。典型例子正是 safari18_4 / safari18_4_ios：
      底层 .so 其实支持（Curl.impersonate 返回 0），但 Python 侧名单没登记、
      指纹库也没有，库永远进不了原生分支 → 实际仍然不可用。
    """

    if _is_native_impersonate_target(target):
        curl = Curl()
        try:
            if curl.impersonate(resolve_latest_browser_type(target)) == 0:
                return True
        finally:
            try:
                curl.close()
            except Exception:  # noqa: BLE001 句柄关闭失败不影响判定结果
                pass
    return target in named_fingerprints


def _build_supported_impersonate_targets() -> list[str]:
    """对候选目标逐个实测，构建 curl_cffi 当前构建真正支持的 impersonate 白名单。

    ``BrowserTypeLiteral.__args__`` 只是给类型检查器看的字面量清单：它既可能残留
    底层已移除的 deprecated 别名，也可能漏掉底层其实支持的名字，因此不能当作支持
    清单使用。这里对全部候选逐个实测（每个目标走一遍库的真实分支口径），只保留
    真正能用的，避免依赖任何硬编码的黑白名单。
    """
    try:
        named_fingerprints = set(FingerprintManager.load_fingerprints())
        targets = [
            target
            for target in BrowserTypeLiteral.__args__
            if _probe_impersonate_target(target, named_fingerprints)
        ]
    except Exception:  # curl_cffi 内部实现变动时静默退回兜底白名单
        targets = []
    return targets or list(_FALLBACK_IMPERSONATE_TARGETS)


# 进程启动时确定一次即可，请求路径上只做随机取值，不再重复探测
SUPPORTED_IMPERSONATE_TARGETS = _build_supported_impersonate_targets()


def _is_desktop_chrome_target(target: str) -> bool:
    """是否为带明确主版本号的桌面 Chrome 指纹（排除 android / edge / safari / firefox / 裸别名）。

    ``chrome`` 这类裸别名会被 resolve_latest_browser_type 解析到最新 Chrome，本身也是
    桌面 Chrome，但其版本号随 curl_cffi 升级漂移，Our UA 生成器无法稳定对齐；因此只保留
    ``chromeNNN`` 形式。若过滤后为空（curl_cffi 构建变化），退回裸 ``chrome`` 别名。
    """
    return bool(re.fullmatch(r"chrome\d{3}", target))


#: 与显式 UA / Client Hints 成组使用的桌面 Chrome 指纹白名单。
#: 随机挑 Firefox/Safari/Edge 指纹、却带 Chrome UA，会让 JA3/JA4 与 UA 互相矛盾，
#: 是最容易被风控交叉校验识别的错位。此处收窄到 Chrome 桌面系换取一致性。
WEB_IMPERSONATE_TARGETS: list[str] = [
    t for t in SUPPORTED_IMPERSONATE_TARGETS if _is_desktop_chrome_target(t)
] or ["chrome"]

_CHROME_UA_TEMPLATE = (
    "Mozilla/5.0 ({platform}) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/{major}.0.0.0 Safari/537.36"
)
_CHROME_UA_PLATFORMS = (
    "Windows NT 10.0; Win64; x64",
    "Macintosh; Intel Mac OS X 10_15_7",
    "X11; Linux x86_64",
)


def chrome_major_of(target: str) -> int:
    """从 ``chromeNNN`` 指纹名取主版本号；裸别名回落到 130（仅用于 UA 兜底）。"""
    if m := re.fullmatch(r"chrome(\d{3})", target):
        return int(m.group(1))
    return 130


def rand_web_ua(target: str | None = None) -> str:
    """生成与目标指纹同主版本号的桌面 Chrome UA。

    :param target: 已选定的 impersonate target；None 时随机挑一个 WEB 目标。
    这样 UA 版本 (``Chrome/152``) 与 TLS 指纹 (``chrome152``) 同源，
    避免「TLS 说 Firefox、UA 说 Chrome」的错位。
    """
    chosen = target or random.choice(WEB_IMPERSONATE_TARGETS)
    return _CHROME_UA_TEMPLATE.format(
        platform=random.choice(_CHROME_UA_PLATFORMS),
        major=chrome_major_of(chosen),
    )


def pick_web_fingerprint(headers: HeaderTypes | None) -> tuple[str, HeaderTypes | None]:
    """挑一个桌面 Chrome 指纹，并把传入 headers 里的 UA 对齐到它的主版本。

    上游（``RequestHandler`` / ``BiliCookie``）生成的 UA 可能是 Firefox/Safari，或
    与本次随机到的 Chrome 版本差好几代。这里在**真正发请求的最后一刻**统一改写，
    保证 JA3/JA4（TLS 指纹）与 UA / sec-ch-ua（HTTP 头）同家族、同主版本。

    - 只处理 dict / list(tuple) 两种 ``headers`` 形态，其他形态原样返回；
    - 不改变调用方对象本身（拷贝后再改）。
    """
    target = random.choice(WEB_IMPERSONATE_TARGETS)
    ua = rand_web_ua(target)
    if headers is None:
        return target, None
    if isinstance(headers, dict):
        new_headers = dict(headers)
        new_headers["user-agent"] = ua
        return target, new_headers
    if isinstance(headers, (list, tuple)):
        new_items = [(k, v) for k, v in headers if k.lower() != "user-agent"]
        new_items.append(("user-agent", ua))
        return target, new_items
    return target, headers


def resolve_web_profile(
    session_key: str | None = None, *, impersonate: str | None = None
) -> WebProfile:
    """决定本次请求使用的 ``WebProfile``（TLS target + 同源 UA）。

    - ``impersonate`` 显式指定时优先（探测脚本 / 风控矩阵需要单变量对照）；
    - ``WEB_SESSION__PIN_FINGERPRINT=false`` 时回退为随机 target（线上回滚开关）；
    - 默认按会话键稳定映射，保证同一 buvid 会话的 JA3/UA 不漂移。
    """
    if impersonate:
        return profile_for_target(impersonate, session_key=session_key)
    if not CONFIG.web_session.pin_fingerprint:
        return random_profile()
    return profile_for_session_key(session_key or ANONYMOUS_SESSION_KEY)


def format_httpx_proxy(request_proxy: dict | None) -> str | None:
    if not request_proxy:
        return None
    if format_ip_str := get_scheme_ip_port_form_proxy_dict(request_proxy):
        return format_ip_str
    else:
        return None


class MYASYNCHTTPX:
    """对 ``curl_cffi.AsyncSession`` 的薄封装：按会话固定指纹并复用连接。

    - 指纹 / UA 由 ``resolve_web_profile(session_key)`` 决定，同一个 buvid 会话
      永远命中同一个 target，不再每请求 ``random.choice``；
    - 业务请求走 ``web_session_pool``（LRU + TTL）复用 AsyncSession；
    - ``ephemeral=True``（cookie / metadata 激活链路）每次新建短连接，用完即关，
      不进入池 —— 这类请求是一次性握手，复用无收益且有跨出口串味风险。
    """

    @staticmethod
    def _lease(
        *,
        session_key: str | None,
        impersonate: str | None,
        ephemeral: bool,
    ):
        profile = resolve_web_profile(session_key, impersonate=impersonate)
        # 未固定指纹时随机 target 不能落池（否则池里会出现同一 key 多个 target）；
        # 显式指定 target 的探测请求也不落池，避免污染业务会话。
        ephemeral = (
            ephemeral
            or bool(impersonate)
            or not CONFIG.web_session.pin_fingerprint
        )
        return web_session_pool.lease(profile, ephemeral=ephemeral)

    async def get(
        self,
        url,
        headers=None,
        verify=False,
        proxies: Union[dict, None] = None,
        timeout=10,
        params=None,
        impersonate: str | None = None,
        session_key: str | None = None,
        ephemeral: bool = False,
        *args,
        **kwargs,
    ) -> Response:
        """

        :param url:
        :param headers:
        :param verify:
        :param proxies: like {
            'http':'http://1.1.1.1',
            'https':'http://1.1.1.1'
        }
        :param timeout:
        :param params:
        :param impersonate: 显式指定 TLS 指纹（探测脚本用；None = 按会话键固定）
        :param session_key: 会话键（cookie 的 _uuid / buvid3），决定固定指纹
        :param ephemeral: True = 一次性短连接，不进会话池
        :return:
        """
        format_proxy_str = format_httpx_proxy(proxies)
        if type(headers) is tuple:
            headers = list(headers)
        async with self._lease(
            session_key=session_key, impersonate=impersonate, ephemeral=ephemeral
        ) as s:
            resp = await s.get(
                url=url,
                headers=headers,
                timeout=timeout,
                params=params,
                proxy=format_proxy_str,
                verify=verify,
            )
        return resp

    async def post(
        self,
        url,
        data=None,
        headers=None,
        verify=False,
        proxies: dict = None,
        timeout=10,
        json: dict | list | None = None,
        impersonate: str | None = None,
        session_key: str | None = None,
        ephemeral: bool = False,
        *args,
        **kwargs,
    ) -> Response:
        format_proxy_str = format_httpx_proxy(proxies)
        if type(headers) is tuple:
            headers = list(headers)
        async with self._lease(
            session_key=session_key, impersonate=impersonate, ephemeral=ephemeral
        ) as s:
            resp = await s.post(
                url=url,
                data=data,
                headers=headers,
                timeout=timeout,
                proxy=format_proxy_str,
                verify=verify,
                default_headers=False,
                json=json,
            )
        return resp

    async def request(
        self,
        url,
        data: typing.Optional[RequestData] = None,
        method: typing.Literal[
            "GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "TRACE", "PATCH", "QUERY"
        ] = "GET",
        headers: typing.Optional[HeaderTypes] = None,
        verify=False,
        proxies=None,
        timeout=10,
        content: typing.Optional[RequestContent] = None,
        files: typing.Optional[RequestFiles] = None,
        json: typing.Optional[typing.Any] = None,
        params: typing.Optional[QueryParamTypes] = None,
        cookies: typing.Optional[CookieTypes] = None,
        extensions: typing.Optional[dict] = None,
        impersonate: str | None = None,
        session_key: str | None = None,
        ephemeral: bool = False,
        *args,
        **kwargs,
    ) -> Response:
        """
        :param proxies: {"http":"xxx.xxx.xxx.xxx", "https":"xxx.xxx.xxx.xxx"}
        :param impersonate: 显式指定 TLS 指纹（探测脚本用；None = 按会话键固定）
        :param session_key: 会话键（cookie 的 _uuid / buvid3），决定固定指纹
        :param ephemeral: True = 一次性短连接，不进会话池（local_exit_only 必须为 True）
        """
        if (
            "api.bilibili.com/x/gaia-vgate/v1/register" in url
            or "api.bilibili.com/x/gaia-vgate/v1/validate" in url
        ):
            format_proxy_str = None
        else:
            format_proxy_str = format_httpx_proxy(proxies)
        if type(headers) is tuple:
            headers = list(headers)
        async with self._lease(
            session_key=session_key, impersonate=impersonate, ephemeral=ephemeral
        ) as s:
            resp = await s.request(
                url=url,
                data=data,
                method=method,
                headers=headers,
                timeout=timeout,
                content=content,
                files=files,
                json=json,
                params=params,
                cookies=cookies,
                proxy=format_proxy_str,
                verify=verify,
                default_headers=False,
            )
        return resp


my_async_httpx = MYASYNCHTTPX()
if __name__ == "__main__":
    MyAsyncReq = MYASYNCHTTPX()
    loop = asyncio.get_event_loop()
    task = loop.create_task(
        MyAsyncReq.request(
            method="GET",
            url="https://test.ipw.cn",
            headers=(
                ("Referer", "https://www.bilibili.com/"),
                (
                    "User-Agent",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                ),
                ("Cookie", "1"),
            ),
            proxies={
                "sock4": "socks4://1.10.133.155:4145",
                "socks4": "socks4://1.10.133.155:4145",
            },
        )
    )
    loop.run_until_complete(task)
    print(task.result().text)
