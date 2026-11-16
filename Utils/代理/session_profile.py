"""会话级 Web 指纹：把一个会话键稳定映射到「TLS impersonate target + UA」。

为什么必须按会话固定：TLS 指纹（JA3/JA4）与 UA / Client Hints 是服务端最容易做
交叉校验的两组信号。如果每个请求都随机挑 target，同一个 buvid 会话的 JA3 会来回
漂移（这次 Chrome、下次 Firefox），而 UA 却一直是 Chrome —— 比「固定一个指纹」
更像机器人。因此这里用稳定哈希把会话键（cookie 的 ``_uuid`` / buvid3）映射到
唯一 target，UA 再由该 target 的主版本号派生，三者天然同源。

本模块只依赖 ``SealedRequests`` 里的 target 白名单，且**延迟导入**，避免与
``SealedRequests`` 形成模块级循环依赖。
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

#: 匿名会话键：没有 cookie 的请求（up 主动态探测、直播分区等）共用一份稳定指纹。
#: 它们本来就不带 buvid，共用不会引入跨会话关联，但能避免每次随机漂移。
ANONYMOUS_SESSION_KEY = "anon:web"

_CHROME_UA_TEMPLATE = (
    "Mozilla/5.0 ({platform}) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/{major}.0.0.0 Safari/537.36"
)
#: 桌面 Chrome 的三个平台串；索引由会话哈希决定，同一会话永远同一平台。
_CHROME_UA_PLATFORMS = (
    "Windows NT 10.0; Win64; x64",
    "Macintosh; Intel Mac OS X 10_15_7",
    "X11; Linux x86_64",
)


@dataclass(frozen=True, slots=True)
class WebProfile:
    """一个会话固定使用的 Web 指纹组合。"""

    session_key: str
    target: str
    ua: str
    platform: str

    @property
    def major(self) -> int:
        return _major_of(self.target)


def _major_of(target: str) -> int:
    if m := re.fullmatch(r"chrome(\d{3})", target):
        return int(m.group(1))
    # 裸 chrome 别名会解析到最新版本；UA 只做兜底，不参与关键判定
    return 130


def web_targets() -> list[str]:
    """桌面 Chrome 指纹白名单（从 ``SealedRequests`` 实测白名单过滤而来）。"""
    from Utils.代理.SealedRequests import WEB_IMPERSONATE_TARGETS

    return list(WEB_IMPERSONATE_TARGETS)


def _digest(key: str) -> int:
    return int.from_bytes(hashlib.md5(key.encode("utf-8")).digest(), "big")


def profile_for_session_key(session_key: str | None) -> WebProfile:
    """把会话键稳定映射为 ``WebProfile``（纯函数，同一键永远同一结果）。"""
    key = (session_key or ANONYMOUS_SESSION_KEY).strip() or ANONYMOUS_SESSION_KEY
    targets = web_targets()
    digest = _digest(key)
    target = targets[digest % len(targets)]
    return profile_for_target(target, session_key=key, digest=digest)


def profile_for_target(
    target: str, *, session_key: str | None = None, digest: int | None = None
) -> WebProfile:
    """按指定 target 构造 ``WebProfile``（UA 主版本号与 target 同源）。"""
    key = (session_key or ANONYMOUS_SESSION_KEY).strip() or ANONYMOUS_SESSION_KEY
    seed = _digest(key) if digest is None else digest
    platform = _CHROME_UA_PLATFORMS[(seed >> 32) % len(_CHROME_UA_PLATFORMS)]
    ua = _CHROME_UA_TEMPLATE.format(platform=platform, major=_major_of(target))
    return WebProfile(session_key=key, target=target, ua=ua, platform=platform)


def random_profile() -> WebProfile:
    """回滚用：随机挑一个桌面 Chrome target（不按会话固定）。"""
    import random

    target = random.choice(web_targets())
    return profile_for_target(target, session_key=f"random:{random.getrandbits(32)}")


def session_key_of_cookie(cookie_wrapper) -> str | None:
    """从 ``CookieWrapper`` 取稳定会话键：优先生成的 ``_uuid``，其次 buvid3。"""
    if cookie_wrapper is None:
        return None
    ck = getattr(cookie_wrapper, "ck", None)
    if ck is None:
        return None
    params = getattr(ck, "gen_web_cookie_params", None)
    params_uuid = getattr(params, "uuid", None) if params else None
    return params_uuid or getattr(ck, "uuid", None) or getattr(ck, "buvid3", None)


def session_key_from_headers(headers) -> str | None:
    """从请求头里的 Cookie 串解析 ``_uuid`` / ``buvid3``（激活链路没有 CookieWrapper）。"""
    if not headers:
        return None
    if isinstance(headers, dict):
        cookie = headers.get("cookie") or headers.get("Cookie") or ""
    else:
        cookie = ""
        for key, value in headers:
            if str(key).lower() == "cookie":
                cookie = value
                break
    if not cookie:
        return None
    for part in str(cookie).split(";"):
        name, _, value = part.strip().partition("=")
        if name in ("_uuid", "buvid3") and value:
            return value.strip()
    return None
